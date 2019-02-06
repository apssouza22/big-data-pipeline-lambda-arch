package com.apssouza.lambda;

import com.apssouza.lambda.dto.RddEnhanced;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Function1;
import org.apache.spark.api.java.function.Function

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;


public class AppStreaming {


    // 'AU': ('Australia', (113.338953078, -43.63459726
    //                      153.569469029, -10.6681857235))
    private static final double LAT_1 = -43.6345972634d;
    private static final double LAT_2 = -10.6681857235;
    private static final double LON_1 = 113.338953078d;
    private static final double LON_2 = 153.569469029d;

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

    public static void main(String[] args) throws InterruptedException {
        final SparkConf conf = getSparkConf();
        final JavaStreamingContext streamingContext = getJavaStreamingContext(conf);
        final SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        final JavaInputDStream<ConsumerRecord<String, String>> locationStream = getDirectStream(streamingContext);

        final HeatMapMesurement app = new HeatMapMesurement();
        final JavaDStream<String> locationStreamValues = locationStream.map(ConsumerRecord::value);
        final String csvFile = "/Users/apssouza/Projetos/java/lambda-arch/data/spark/output/realtime";

        locationStreamValues.foreachRDD((JavaRDD<String> locationRDD) -> {
            if (!locationRDD.isEmpty()) {
                List<String> collect = locationRDD.rdd().toJavaRDD().collect();
                Dataset<String> dataset = sparkSession.createDataset(locationRDD.rdd(),  Encoders.STRING());
                transformRdd(dataset, sparkSession);
                Dataset<RddEnhanced> rddEnhancedDataset = null;// transformRdd(dataset, sparkSession);
                Dataset<Row> row =rddEnhancedDataset.toDF()
                        .selectExpr(
                                "source", "timestamp", "latitude", "longitude",
                                "inputInfo.topic as topic", "inputInfo.kafkaPartition as kafkaPartition", "inputInfo.fromOffset as fromOffset", "inputInfo.untilOffset as untilOffset"
                        );
                row.write().csv(csvFile);
                row.createOrReplaceTempView(MEETUP_VIEW);
                Dataset<Row> locations = sparkSession.sql("select * from " + MEETUP_VIEW);
                List<Map<String, Object>> heatmap = app.getHeatmap(10, locations);
            }
        });
        commitAck(locationStream);
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    private static Dataset<RddEnhanced> transformRdd(Dataset<String> dataset, SparkSession sparkSession) {
        Dataset<RddEnhanced> transform = dataset.toDF().transform(getDatasetDatasetFunction1(sparkSession));
//////
//        Dataset<String> transform = dataset.toDF().transform(item -> {
//            OffsetRange[] offsetRanges = ((HasOffsetRanges) item.rdd()).offsetRanges();
//
//            JavaRDD<String> rddEnhancedJavaRDD = item.javaRDD().mapPartitionsWithIndex((index, items) -> {
//                Map<String, String> meta = new HashMap<String, String>() {{
//                    put("topic", offsetRanges[index].topic());
//                    put("kafkaPartition", String.class.cast(offsetRanges[index].partition()));
//                    put("fromOffset", String.class.cast(offsetRanges[index].fromOffset()));
//                    put("fromOffset", String.class.cast(offsetRanges[index].untilOffset()));
//                }};
//                List<String> list = new ArrayList<>();
//                while (items.hasNext()) {
//                    String[] split = items.next().split(",");
////                    list.add(new RddEnhanced(split[0], split[1], split[2], split[3], meta));
//                    list.add("dfda");
//                }
//                return list.iterator();
//            }, true);
//            Encoder<RddEnhanced> bean = Encoders.bean(RddEnhanced.class);
//            return sparkSession.createDataset(rddEnhancedJavaRDD.rdd(), Encoders.STRING());
//        });
        return transform;
    }

    private static Function1<Dataset<Row>, Dataset<RddEnhanced>> getDatasetDatasetFunction1(SparkSession sparkSession) {
        return item -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) item.rdd()).offsetRanges();

            JavaRDD<RddEnhanced> rddEnhancedJavaRDD = item.javaRDD().mapPartitionsWithIndex((index, items) -> {
                Map<String, String> meta = new HashMap<String, String>() {{
                    put("topic", offsetRanges[index].topic());
                    put("kafkaPartition", String.class.cast(offsetRanges[index].partition()));
                    put("fromOffset", String.class.cast(offsetRanges[index].fromOffset()));
                    put("fromOffset", String.class.cast(offsetRanges[index].untilOffset()));
                }};
                List<RddEnhanced> list = new ArrayList<>();
                while (items.hasNext()) {
                    String[] split = items.next().getString(0).split(",");
                    list.add(new RddEnhanced(split[0], split[1], split[2], split[3], meta));
                }
                return list.iterator();
            }, true);
            Encoder<RddEnhanced> bean = Encoders.bean(RddEnhanced.class);
            return sparkSession.createDataset(rddEnhancedJavaRDD.rdd(), bean);
        };
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

    private static void commitAck(JavaInputDStream<ConsumerRecord<String, String>> meetupStream) {
        // some time later, after outputs have completed
        meetupStream.foreachRDD((JavaRDD<ConsumerRecord<String, String>> meetupRDD) -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) meetupRDD.rdd()).offsetRanges();

            ((CanCommitOffsets) meetupStream.inputDStream())
                    .commitAsync(offsetRanges, new MeetupOffsetCommitCallback());
        });
    }
}

final class MeetupOffsetCommitCallback implements OffsetCommitCallback {

    private static final Logger log = Logger.getLogger(MeetupOffsetCommitCallback.class.getName());

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        log.info("---------------------------------------------------");
        log.log(Level.INFO, "{0} | {1}", new Object[]{offsets, exception});
        log.info("---------------------------------------------------");
    }
}
