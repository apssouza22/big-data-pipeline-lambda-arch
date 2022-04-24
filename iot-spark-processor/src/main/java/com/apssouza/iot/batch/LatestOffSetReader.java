package com.apssouza.iot.batch;

import org.apache.kafka.common.TopicPartition;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

/**
 * Read from the HDFS the latest processed kafka offset
 */
public class LatestOffSetReader {

    private Dataset<Row> parquetData;

    final SparkSession sparkSession;
    final String file;

    public LatestOffSetReader(final SparkSession sparkSession, final String file) {
        this.sparkSession = sparkSession;
        this.file = file;
    }

    public LatestOffSetReader read() {
        parquetData = sparkSession.read().parquet(file);
        return this;
    }

    private JavaRDD<Row> query() throws AnalysisException {
        parquetData.createTempView("traffic");
        return parquetData.sqlContext()
                .sql("select max(untilOffset) as untilOffset, topic, kafkaPartition from traffic group by topic, kafkaPartition")
                .javaRDD();
    }

    public Map<TopicPartition, Long> offsets() throws AnalysisException {
        return this.query()
                .collect()
                .stream()
                .map(LatestOffSetReader::mapToPartition)
                .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
    }

    private static Tuple2<TopicPartition, Long> mapToPartition(Row row) {
        TopicPartition topicPartition = new TopicPartition(
                row.getString(row.fieldIndex("topic")),
                row.getInt(row.fieldIndex("kafkaPartition"))
        );
        Long offSet = Long.valueOf(row.getString(row.fieldIndex("untilOffset")));
        return new Tuple2<>(
                topicPartition,
                offSet
        );
    }
}
