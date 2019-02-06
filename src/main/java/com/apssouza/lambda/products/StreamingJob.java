package com.apssouza.lambda.products;


import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;

public class StreamingJob {

    private static final String MEETUP_VIEW = "meetup_view";

    private static final String RUN_LOCAL_WITH_AVAILABLE_CORES = "local[*]";
    //    private static final String RUN_LOCAL_WITH_AVAILABLE_CORES =  "spark://spark-master:7077";
    private static final String APPLICATION_NAME = "Kafka <- Spark(Dataset) -> MongoDb";
    private static final String CASE_SENSITIVE = "false";

    private static final int BATCH_DURATION_INTERVAL_MS = 5000;

    private static final Map<String, Object> KAFKA_CONSUMER_PROPERTIES;

    private static final String KAFKA_BROKERS = "localhost:9092";
    private static final String KAFKA_OFFSET_RESET_TYPE = "earliest";
    private static final String KAFKA_GROUP = "meetupGroup";
    private static final String KAFKA_TOPIC = "heatmap2";
    private static final Collection<String> TOPICS = Collections.unmodifiableList(Arrays.asList(KAFKA_TOPIC));

    static {
        Map<String, Object> kafkaProperties = new HashMap<>();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_GROUP);
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KAFKA_OFFSET_RESET_TYPE);
        kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KAFKA_CONSUMER_PROPERTIES = Collections.unmodifiableMap(kafkaProperties);
    }

    public static void main(String[] args) {
        final SparkConf conf = getSparkConf();
        final JavaStreamingContext streamingContext = getJavaStreamingContext(conf);
        final SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        final JavaInputDStream<ConsumerRecord<String, String>> locationStream = getDirectStream(streamingContext);


//        Map<TopicAndPartition, Long> fromOffsets =  new HashedMap();
//
//        String  hdfsPath;
//        Dataset<Row> parquet = sparkSession.read().parquet(hdfsPath);
//
//        parquet.rdd().foreach(hdfsData ->{
//                fromOffsets = hdfsData.groupBy("topic", "kafkaPartition").agg(max("untilOffset").as("untilOffset"))
//                .collect().map(row -> {
//                            TopicAndPartition(
//                                    row.getAs[String] ("topic"),
//                                    row.getAs[Int] ("kafkaPartition"),
//                            row.getAs[String]("untilOffset").toLong + 1
//                            ),
//                        });
//        }).
//            .toMap
//        )

//        val kafkaDirectStream = fromOffsets.isEmpty match {
//            case true =>
//                KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
//                    ssc, kafkaDirectParams, Set(topic)
//                )
//            case false =>
//                KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
//                    ssc, kafkaDirectParams, fromOffsets, {mmd:MessageAndMetadata[String, String]=>(mmd.key(), mmd.message())}
//                )
//        }
//
//        val activityStream = kafkaDirectStream.transform(input = > {
//                functions.rddToRDDActivity(input)
//        }).cache()
//
//        // save data to HDFS
//        activityStream.foreachRDD {
//            rdd =>
//            val activityDF = rdd
//                    .toDF()
//                    .selectExpr("timestamp_hour", "referrer", "action", "prevPage", "page", "visitor", "product", "inputProps.topic as topic", "inputProps.kafkaPartition as kafkaPartition", "inputProps.fromOffset as fromOffset", "inputProps.untilOffset as untilOffset")
//
//            activityDF
//                    .write
//                    .partitionBy("topic", "kafkaPartition", "timestamp_hour")
//                    .mode(SaveMode.Append)
//                    .parquet(hdfsPath)
//        }
//
//        // activity by product
//        val activityStateSpec =
//                StateSpec
//                        .function(mapActivityStateFunc)
//                        .timeout(Minutes(120))
//
//        val statefulActivityByProduct = activityStream.transform(rdd = > {
//                val df = rdd.toDF()
//                df.registerTempTable("activity")
//                val activityByProduct = sqlContext.sql(
//                """SELECT
//                product,
//                timestamp_hour,
//                sum( case when action = 'purchase' then 1 else 0 end)as purchase_count,
//        sum( case when action = 'add_to_cart' then 1 else 0 end)as add_to_cart_count,
//        sum( case when action = 'page_view' then 1 else 0 end)as page_view_count
//        from activity
//        group by product, timestamp_hour "" ")
//
//        activityByProduct
//                .map {
//            r =>((r.getString(0), r.getLong(1)),
//            ActivityByProduct(r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4))
//            )
//        }
//        }).mapWithState(activityStateSpec)
//
//        val activityStateSnapshot = statefulActivityByProduct.stateSnapshots()
//        activityStateSnapshot
//                .reduceByKeyAndWindow(
//                        (a, b) =>b,
//                (x, y)=>x,
//                Seconds(30 / 4 * 4)
//        ) // only save or expose the snapshot every x seconds
//        .map(sr = > ActivityByProduct(sr._1._1, sr._1._2, sr._2._1, sr._2._2, sr._2._3))
//        .saveToCassandra("lambda", "stream_activity_by_product")
//
//
//        // unique visitors by product
//        val visitorStateSpec =
//                StateSpec
//                        .function(mapVisitorsStateFunc)
//                        .timeout(Minutes(120))
//
//        val statefulVisitorsByProduct = activityStream.map(a = > {
//                val hll = new HyperLogLogMonoid(12)
//                ((a.product, a.timestamp_hour),hll(a.visitor.getBytes))
//        }).mapWithState(visitorStateSpec)
//
//        val visitorStateSnapshot = statefulVisitorsByProduct.stateSnapshots()
//        visitorStateSnapshot
//                .reduceByKeyAndWindow(
//                        (a, b) =>b,
//                (x, y)=>x,
//                Seconds(30 / 4 * 4)
//        ) // only save or expose the snapshot every x seconds
//        .map(sr = > VisitorsByProduct(sr._1._1, sr._1._2, sr._2.approximateSize.estimate))
//        .saveToCassandra("lambda", "stream_visitors_by_product")
//
//        /*.foreachRDD(rdd =>
//        rdd
//          .map(sr => VisitorsByProduct(sr._1._1, sr._1._2, sr._2.approximateSize.estimate))
//          //.toDF().registerTempTable("VisitorsByProduct")
//      )*/
//
//        ssc
//    }
//
//    val ssc = getStreamingContext(streamingApp, sc, batchDuration)
//    //ssc.remember(Minutes(5))
//    ssc.start()
//            ssc.awaitTermination()

    }


    private static JavaStreamingContext getJavaStreamingContext(SparkConf conf) {
        return new JavaStreamingContext(
                conf,
                new Duration(BATCH_DURATION_INTERVAL_MS)
        );
    }

    private static JavaInputDStream<ConsumerRecord<String, String>> getDirectStream(JavaStreamingContext streamingContext) {
        return KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(TOPICS, KAFKA_CONSUMER_PROPERTIES)
        );
    }

    private static SparkConf getSparkConf() {
        return new SparkConf()
                .setMaster(RUN_LOCAL_WITH_AVAILABLE_CORES)
                .setAppName(APPLICATION_NAME)
                .set("spark.sql.caseSensitive", CASE_SENSITIVE);
    }

}
