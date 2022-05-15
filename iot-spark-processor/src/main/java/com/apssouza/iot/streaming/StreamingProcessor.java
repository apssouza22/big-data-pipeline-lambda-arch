package com.apssouza.iot.streaming;

import com.apssouza.iot.batch.LatestOffSetReader;
import com.apssouza.iot.common.ProcessorUtils;
import com.apssouza.iot.common.dto.IoTData;
import com.apssouza.iot.common.dto.POIData;
import com.apssouza.iot.common.IoTDataDeserializer;
import com.apssouza.iot.common.PropertyFileReader;
import com.datastax.spark.connector.util.JavaApiHelper;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import scala.reflect.ClassTag;

/**
 * This class consumes Kafka IoT messages and creates stream for processing the IoT data.
 *
 * @author apssouza22
 */
public class StreamingProcessor implements Serializable {

    private static final Logger logger = Logger.getLogger(StreamingProcessor.class);
    private final Properties prop;

    public StreamingProcessor(Properties properties) {
        this.prop = properties;
    }

    public static void main(String[] args) throws Exception {
//        String file = "iot-spark-local.properties";
        String file = "iot-spark.properties";
        Properties prop = PropertyFileReader.readPropertyFile(file);
        StreamingProcessor streamingProcessor = new StreamingProcessor(prop);
        streamingProcessor.start();
    }

    private void start() throws Exception {
        String parqueFile = prop.getProperty("com.iot.app.hdfs") + "iot-data-parque";
        Map<String, Object> kafkaProperties = getKafkaParams(prop);
        SparkConf conf = ProcessorUtils.getSparkConf(prop, "streaming-processor");

        //batch interval of 5 seconds for incoming stream
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

        //Please note that while data checkpointing is useful for stateful processing, it comes with a latency cost.
        // Hence, it's necessary to use this wisely.
        // This is necessary because we keep state in some operations.
        // We are not using this for fault-tolerance. For that, we use Kafka offset @see commitOffset

        streamingContext.checkpoint(prop.getProperty("com.iot.app.spark.checkpoint.dir"));
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        Map<TopicPartition, Long> offsets = getOffsets(parqueFile, sparkSession);
        JavaInputDStream<ConsumerRecord<String, IoTData>> kafkaStream = getKafkaStream(
                prop,
                streamingContext,
                kafkaProperties,
                offsets
        );

        logger.info("Starting Stream Processing");

        //broadcast variables. We will monitor vehicles on Route 37 which are of type Truck
        //Basically we are sending the data to each worker nodes on a Spark cluster.
        ClassTag<POIData> classTag = JavaApiHelper.getClassTag(POIData.class);
        Broadcast<POIData> broadcastPOIValues = sparkSession
                .sparkContext()
                .broadcast(getPointOfInterest(), classTag);

        StreamProcessor streamProcessor = new StreamProcessor(kafkaStream);
        streamProcessor.transform()
                .appendToHDFS(sparkSession, parqueFile)
                .processPOIData(broadcastPOIValues)
                .filterVehicle()
                .cache()
                .processTotalTrafficData()
                .processWindowTrafficData()
                .processHeatMap();

        commitOffset(kafkaStream);

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    private Map<TopicPartition, Long> getOffsets(final String parqueFile, final SparkSession sparkSession) {
        try {
            LatestOffSetReader latestOffSetReader = new LatestOffSetReader(sparkSession, parqueFile);
            return latestOffSetReader.read().offsets();
        } catch (Exception e) {
            return new HashMap<>();
        }
    }

    private POIData getPointOfInterest() {
        POIData poiData = new POIData();
        poiData.setLatitude(53.877495);
        poiData.setLongitude(-6.50238);
        poiData.setRadius(100);//100 km
        return poiData;
    }

    /**
     * Commit the ack to kafka after process have completed
     * This is our fault-tolerance implementation
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


    private JavaInputDStream<ConsumerRecord<String, IoTData>> getKafkaStream(
            Properties prop,
            JavaStreamingContext streamingContext,
            Map<String, Object> kafkaProperties,
            Map<TopicPartition, Long> fromOffsets
    ) {
        List<String> topicSet = Arrays.asList(new String[]{prop.getProperty("com.iot.app.kafka.topic")});
        if (fromOffsets.isEmpty()) {
            return KafkaUtils.createDirectStream(
                    streamingContext,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(topicSet, kafkaProperties)
            );
        }

        return KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicSet, kafkaProperties, fromOffsets)
        );
    }



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
