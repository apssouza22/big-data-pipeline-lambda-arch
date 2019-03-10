package com.iot.app.spark.processor;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import com.iot.app.spark.util.IoTDataDeserializer;
import com.iot.app.spark.util.PropertyFileReader;
import com.iot.app.spark.dto.IoTData;
import com.iot.app.spark.dto.POIData;

import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;
import scala.Tuple3;

/**
 * This class consumes Kafka IoT messages and creates stream for processing the IoT data.
 *
 * @author apssouza22
 */
public class StreamingProcessor implements Serializable {

    private static final Logger logger = Logger.getLogger(StreamingProcessor.class);
    private String file;

    public StreamingProcessor(String file) {
        this.file = file;
    }

    public static void main(String[] args) throws Exception {
        String file = "iot-spark.properties";
        StreamingProcessor streamingProcessor = new StreamingProcessor(file);
        streamingProcessor.start();
    }

    private void start() throws Exception {
        Properties prop = PropertyFileReader.readPropertyFile(file);
        Map<String, Object> kafkaProperties = getKafkaParams(prop);
        String[] jars = {
        };
        SparkConf conf = getSparkConf(prop, jars);

        //batch interval of 5 seconds for incoming stream
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        final SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        jssc.checkpoint(prop.getProperty("com.iot.app.spark.checkpoint.dir"));
        Map<TopicPartition, Long> lastOffSet = getLatestOffSet(sparkSession, prop);
//        Map<TopicPartition, Long> lastOffSet = Collections.EMPTY_MAP;
        JavaInputDStream<ConsumerRecord<String, IoTData>> directKafkaStream = getStream(prop, jssc, kafkaProperties, lastOffSet);

        logger.info("Starting Stream Processing");

        JavaDStream<IoTData> transformedStream = directKafkaStream.transform(item -> {
            return getEnhancedObjWithKafkaInfo(item);
        });

        processStream(prop, jssc, sparkSession, transformedStream);
        commitOffset(directKafkaStream);

        jssc.start();
        jssc.awaitTermination();
    }

    /**
     * Commit the ack to kafka some time later, after process have completed
     *
     * @param directKafkaStream
     */
    private void commitOffset(JavaInputDStream<ConsumerRecord<String, IoTData>> directKafkaStream) {
        directKafkaStream.foreachRDD((JavaRDD<ConsumerRecord<String, IoTData>> trafficRdd) -> {
            if (!trafficRdd.isEmpty()) {
                OffsetRange[] offsetRanges = ((HasOffsetRanges) trafficRdd.rdd()).offsetRanges();

                CanCommitOffsets canCommitOffsets = (CanCommitOffsets) directKafkaStream.inputDStream();
                canCommitOffsets.commitAsync(offsetRanges, new TrafficOffsetCommitCallback());
            }
        });
    }


    private Map<TopicPartition, Long> getLatestOffSet(SparkSession sparkSession, Properties prop) {
        Map<TopicPartition, Long> collect = Collections.emptyMap();
        try {
            Dataset<Row> parquet = sparkSession.read()
                    .parquet(prop.getProperty("com.iot.app.hdfs") + "iot-data-parque");

            parquet.createTempView("traffic");
            Dataset<Row> sql = parquet.sqlContext()
                    .sql("select max(untilOffset) as untilOffset, topic, kafkaPartition from traffic group by topic, kafkaPartition");

            collect = sql.javaRDD()
                    .collect()
                    .stream()
                    .map(row -> {
                        TopicPartition topicPartition = new TopicPartition(row.getString(row.fieldIndex("topic")), row.getInt(row.fieldIndex("kafkaPartition")));
                        Tuple2<TopicPartition, Long> key = new Tuple2<>(
                                topicPartition,
                                Long.valueOf(row.getString(row.fieldIndex("untilOffset")))
                        );
                        return key;
                    })
                    .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
        } catch (Exception e) {
            return collect;
        }
        return collect;
    }


    private JavaRDD<IoTData> getEnhancedObjWithKafkaInfo(JavaRDD<ConsumerRecord<String, IoTData>> item) {
        OffsetRange[] offsetRanges = ((HasOffsetRanges) item.rdd()).offsetRanges();

        return item.mapPartitionsWithIndex((index, items) -> {
            Map<String, String> meta = new HashMap<String, String>() {{
                int partition = offsetRanges[index].partition();
                long from = offsetRanges[index].fromOffset();
                long until = offsetRanges[index].untilOffset();

                put("topic", offsetRanges[index].topic());
                put("fromOffset", "" + from);
                put("kafkaPartition", "" + partition);
                put("untilOffset", "" + until);
            }};
            List<IoTData> list = new ArrayList<>();
            while (items.hasNext()) {
                ConsumerRecord<String, IoTData> next = items.next();
                IoTData dataItem = next.value();
                meta.put("dayOfWeek", "" + dataItem.getTimestamp().toLocalDate().getDayOfWeek().getValue());
                dataItem.setMetaData(meta);
                list.add(dataItem);
            }
            return list.iterator();
        }, true);
    }

    private Map<String, Object> getKafkaParams(Properties prop) {
        Map<String, Object> kafkaProperties = new HashMap<>();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, prop.getProperty("com.iot.app.kafka.brokerlist"));
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IoTDataDeserializer.class);
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, prop.getProperty("com.iot.app.kafka.topic"));
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, prop.getProperty("com.iot.app.kafka.resetType"));
        kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return kafkaProperties;
    }

    private void processStream(Properties prop, JavaStreamingContext jssc, SparkSession sql, JavaDStream<IoTData> nonFilteredIotDataStream) throws IOException {
        appendDataToHDFS(prop, sql, nonFilteredIotDataStream);
        JavaDStream<IoTData> filteredIotDataStream = getVehicleNotProcessed(nonFilteredIotDataStream);

        //cache stream as it is used in many computation
        filteredIotDataStream.cache();

        //process data
        RealtimeTrafficDataProcessor iotTrafficProcessor = new RealtimeTrafficDataProcessor();
        iotTrafficProcessor.processTotalTrafficData(filteredIotDataStream);
        iotTrafficProcessor.processWindowTrafficData(filteredIotDataStream);
        processPOI(jssc, nonFilteredIotDataStream, iotTrafficProcessor);

        RealTimeHeatMapProcessor realTimeHeatMapProcessor = new RealTimeHeatMapProcessor();
        realTimeHeatMapProcessor.processHeatMap(filteredIotDataStream);
    }

    private JavaDStream<IoTData> getVehicleNotProcessed(JavaDStream<IoTData> nonFilteredIotDataStream) {
        //We need filtered stream for total and traffic data calculation
        JavaPairDStream<String, IoTData> iotDataPairStream = nonFilteredIotDataStream
                .mapToPair(iot -> new Tuple2<>(iot.getVehicleId(), iot))
                .reduceByKey((a, b) -> a);

        // Check vehicle Id is already processed
        JavaMapWithStateDStream<String, IoTData, Boolean, Tuple2<IoTData, Boolean>> iotDStreamWithStatePairs =
                iotDataPairStream
                        .mapWithState(
                                StateSpec.function(processedVehicleFunc).timeout(Durations.seconds(3600))
                        );//maintain state for one hour

        // Filter processed vehicle ids and keep un-processed
        JavaDStream<Tuple2<IoTData, Boolean>> filteredIotDStreams = iotDStreamWithStatePairs
                .filter(tuple -> tuple._2.equals(Boolean.FALSE));

        // Get stream of IoTdata
        return filteredIotDStreams.map(tuple -> tuple._1);
    }

    private void appendDataToHDFS(Properties prop, SparkSession sql, JavaDStream<IoTData> nonFilteredIotDataStream) {
        nonFilteredIotDataStream.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                Dataset<Row> dataFrame = sql.createDataFrame(rdd, IoTData.class);
                Dataset<Row> dfStore = dataFrame.selectExpr(
                        "fuelLevel", "latitude", "longitude",
                        "routeId", "speed", "timestamp", "vehicleId", "vehicleType",
                        "metaData.fromOffset as fromOffset",
                        "metaData.untilOffset as untilOffset",
                        "metaData.kafkaPartition as kafkaPartition",
                        "metaData.topic as topic",
                        "metaData.dayOfWeek as dayOfWeek"
                );

                dfStore.write()
                        .partitionBy("topic", "kafkaPartition", "dayOfWeek")
                        .mode(SaveMode.Append)
                        .parquet(prop.getProperty("com.iot.app.hdfs") + "iot-data-parque");
            }
        });
    }

    private JavaInputDStream<ConsumerRecord<String, IoTData>> getStream(
            Properties prop,
            JavaStreamingContext jssc,
            Map<String, Object> kafkaProperties,
            Map<TopicPartition, Long> fromOffsets
    ) {
        List<String> topicSet = Arrays.asList(new String[]{prop.getProperty("com.iot.app.kafka.topic")});
        ConsumerStrategy<String, IoTData> subscribe;
        if (fromOffsets.isEmpty()) {
            subscribe = ConsumerStrategies.<String, IoTData>Subscribe(topicSet, kafkaProperties);
        } else {
            subscribe = ConsumerStrategies.<String, IoTData>Subscribe(topicSet, kafkaProperties, fromOffsets);
        }

        return KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                subscribe
        );
    }

    private SparkConf getSparkConf(Properties prop, String[] jars) {
        return new SparkConf()
                .setAppName(prop.getProperty("com.iot.app.spark.app.name"))
                .setMaster(prop.getProperty("com.iot.app.spark.master"))
                .set("spark.cassandra.connection.host", prop.getProperty("com.iot.app.cassandra.host"))
                .set("spark.cassandra.connection.port", prop.getProperty("com.iot.app.cassandra.port"))
                .set("spark.cassandra.auth.username", prop.getProperty("com.iot.app.cassandra.username"))
                .set("spark.cassandra.auth.password", prop.getProperty("com.iot.app.cassandra.password"))
                .set("spark.cassandra.connection.keep_alive_ms", prop.getProperty("com.iot.app.cassandra.keep_alive"))
                ;
//                .setJars(jars);
    }

    private void processPOI(
            JavaStreamingContext jssc,
            JavaDStream<IoTData> nonFilteredIotDataStream,
            RealtimeTrafficDataProcessor iotTrafficProcessor
    ) {
        //poi data
        POIData poiData = new POIData();
        poiData.setLatitude(33.877495);
        poiData.setLongitude(-95.50238);
        poiData.setRadius(30);//30 km

        //broadcast variables. We will monitor vehicles on Route 37 which are of type Truck
        //Basically we are sending the data to each worker nodes on a Spark cluster.
        Broadcast<Tuple3<POIData, String, String>> broadcastPOIValues = jssc.sparkContext().broadcast(new Tuple3<>(poiData, "Route-37", "Truck"));
        //call method  to process stream
        iotTrafficProcessor.processPOIData(nonFilteredIotDataStream, broadcastPOIValues);
    }

    //Function to check processed vehicles.
    private final Function3<String, org.apache.spark.api.java.Optional<IoTData>, State<Boolean>, Tuple2<IoTData, Boolean>> processedVehicleFunc = (String, iot, state) -> {
        Tuple2<IoTData, Boolean> vehicle = new Tuple2<>(iot.get(), false);
        if (state.exists()) {
            vehicle = new Tuple2<>(iot.get(), true);
        } else {
            state.update(Boolean.TRUE);
        }
        return vehicle;
    };

}

final class TrafficOffsetCommitCallback implements OffsetCommitCallback, Serializable {

    private static final Logger log = Logger.getLogger(TrafficOffsetCommitCallback.class);

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        log.info("---------------------------------------------------");
        log.info(String.format("{0} | {1}", new Object[]{offsets, exception}));
        log.info("---------------------------------------------------");
    }
}
