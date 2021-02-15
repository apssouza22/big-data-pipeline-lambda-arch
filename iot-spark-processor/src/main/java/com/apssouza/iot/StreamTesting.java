package com.apssouza.iot;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;

import scala.Tuple2;

public class StreamTesting {

    public static final void main(String[] s) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName(StreamTesting.class.getName())
                .setMaster("local[*]")
                .set("spark.driver.bindAddress", "127.0.0.1");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(30));

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
        lines.print();

        JavaDStream<Integer> length = lines.map(x -> x.length());
        length.print();

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        words.print();

        JavaDStream<String> reduce = words.reduce((a, b) -> a.length() >= b.length() ? a : b);
        reduce.print();

        JavaDStream<String> filter = words.filter(x -> !x.equals("hello"));
        filter.print();

        JavaPairDStream<String, Integer> pairs = words.mapToPair(x -> new Tuple2<>(x, 1));
        pairs.print();

        JavaPairDStream<String, Integer> sum = pairs.reduceByKey((a,b) -> a + b);
        sum.print();

        JavaPairDStream<String, Long> countByValue = words.countByValue();
        countByValue.print();

        JavaPairDStream<Long, String> swap = countByValue.mapToPair(x -> x.swap());
        swap.print();

        JavaPairDStream<Long, String> sort = swap.transformToPair(x -> x.sortByKey(false));
        sort.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
    }
}
