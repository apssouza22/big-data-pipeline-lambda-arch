package com.apssouza.iot.processor;

//import com.apssouza.iot.dto.IoTData;
//
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.streaming.Duration;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//
//import java.io.Serializable;
//import java.sql.Date;
//import java.time.LocalDate;
//import java.util.Arrays;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Queue;
//
//public class StreamProcessorTest extends JavaSparkContext implements Serializable {
//
//    public static void main(String[] args) throws Exception {
//        List<ConsumerRecord<String, IoTData>> list = new LinkedList<>();
//        Date from = java.sql.Date.valueOf(LocalDate.now());
//        IoTData ioTData = new IoTData("1", "truck", "routeid1",
//                "lat", "lon", from, 60, 0);
//        list.add(new ConsumerRecord<>(
//                "topic",
//                1,
//                0,
//                "1",
//                ioTData
//        ));
//        SparkConf conf =  new SparkConf()
//                .setAppName(StreamProcessorTest.class.getName())
//                .setMaster("local[2]")
//                .set("spark.driver.bindAddress", "127.0.0.1")
//                ;
//        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(4000));
//
//        Queue<JavaRDD<ConsumerRecord<String, IoTData>>> rddQueue = new LinkedList<>();
//        JavaRDD<ConsumerRecord<String, IoTData>> rdd = jssc.sparkContext().parallelize(list);
//        rdd.foreach(i -> System.out.println(i));
//        rddQueue.add(rdd);
//
//        JavaDStream<ConsumerRecord<String, IoTData>> dStream = jssc.queueStream(rddQueue);
//        dStream.foreachRDD(i ->{
//            System.out.println(i);
//        });
//        new StreamProcessor(dStream);
//    }
//}