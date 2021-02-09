package com.apssouza.iot.processor;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

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
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;

import com.apssouza.iot.dto.POIData;
import com.apssouza.iot.util.IoTDataDeserializer;
import com.apssouza.iot.util.PropertyFileReader;
import com.apssouza.iot.dto.IoTData;

import org.apache.spark.streaming.kafka010.*;

import scala.Tuple3;

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
        String file = "iot-spark.properties";
        Properties prop = PropertyFileReader.readPropertyFile(file);
        StreamingProcessor streamingProcessor = new StreamingProcessor(prop);
        streamingProcessor.start();
    }

    private void start() throws Exception {
        String[] jars = {};
        String parqueFile = prop.getProperty("com.iot.app.hdfs") + "iot-data-parque";
        Map<String, Object> kafkaProperties = getKafkaParams(prop);
        SparkConf conf = getSparkConf(prop, jars);

        //batch interval of 5 seconds for incoming stream
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));
        streamingContext.checkpoint(prop.getProperty("com.iot.app.spark.checkpoint.dir"));
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        LatestOffSetReader latestOffSetReader = new LatestOffSetReader(sparkSession, parqueFile);
        JavaInputDStream<ConsumerRecord<String, IoTData>> kafkaStream = getKafkaStream(
                prop,
                streamingContext,
                kafkaProperties,
                latestOffSetReader.read().offsets()
        );

        logger.info("Starting Stream Processing");

        //broadcast variables. We will monitor vehicles on Route 37 which are of type Truck
        //Basically we are sending the data to each worker nodes on a Spark cluster.
        var broadcastPOIValues = streamingContext
                .sparkContext()
                .broadcast(new Tuple3<>(getPointOfInterest(), "Route-37", "Truck"));

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

    private POIData getPointOfInterest() {
        //poi data
        POIData poiData = new POIData();
        poiData.setLatitude(33.877495);
        poiData.setLongitude(-95.50238);
        poiData.setRadius(30);//30 km
        return poiData;
    }

    /**
     * Commit the ack to kafka after process have completed
     *
     * @param directKafkaStream
     */
    private void commitOffset(JavaInputDStream<ConsumerRecord<String, IoTData>> directKafkaStream) {
        directKafkaStream.foreachRDD((JavaRDD<ConsumerRecord<String, IoTData>> trafficRdd) -> {
            if (!trafficRdd.isEmpty()) {
                OffsetRange[] offsetRanges = ((HasOffsetRanges) trafficRdd.rdd()).offsetRanges();

                CanCommitOffsets canCommitOffsets = (CanCommitOffsets) directKafkaStream.inputDStream();
                canCommitOffsets.commitAsync(offsetRanges, (offsets, exception) -> {
                    var log = Logger.getLogger(StreamingProcessor.class);
                    log.info("---------------------------------------------------");
                    log.info(String.format("{0} | {1}", new Object[]{offsets, exception}));
                    log.info("---------------------------------------------------");
                });
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

}
