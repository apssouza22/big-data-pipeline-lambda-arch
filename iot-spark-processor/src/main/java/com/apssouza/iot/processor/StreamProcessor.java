package com.apssouza.iot.processor;

import com.apssouza.iot.dto.IoTData;
import com.apssouza.iot.dto.POIData;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import scala.Tuple2;
import scala.Tuple3;

public class StreamProcessor implements Serializable {

    final JavaDStream<ConsumerRecord<String, IoTData>> directKafkaStream;
    private JavaDStream<IoTData> transformedStream;
    private JavaDStream<IoTData> filteredStream;


    public StreamProcessor(JavaDStream<ConsumerRecord<String, IoTData>> directKafkaStream) {
        this.directKafkaStream = directKafkaStream;
    }

    private static JavaRDD<IoTData> transformRecord(JavaRDD<ConsumerRecord<String, IoTData>> item) {
        OffsetRange[] offsetRanges;
        offsetRanges = ((HasOffsetRanges) item.rdd()).offsetRanges();
        return item.mapPartitionsWithIndex(addMetaData(offsetRanges), true);
    }

    private static Function2<Integer, Iterator<ConsumerRecord<String, IoTData>>, Iterator<IoTData>> addMetaData(
            final OffsetRange[] offsetRanges
    ) {
        return (index, items) -> {
            List<IoTData> list = new ArrayList<>();
            while (items.hasNext()) {
                ConsumerRecord<String, IoTData> next = items.next();
                IoTData dataItem = next.value();

                Map<String, String> meta = new HashMap<>();
                meta.put("topic", offsetRanges[index].topic());
                meta.put("fromOffset", "" + offsetRanges[index].fromOffset());
                meta.put("kafkaPartition", "" + offsetRanges[index].partition());
                meta.put("untilOffset", "" + offsetRanges[index].untilOffset());
                meta.put("dayOfWeek", "" + dataItem.getTimestamp().toLocalDate().getDayOfWeek().getValue());

                dataItem.setMetaData(meta);
                list.add(dataItem);
            }
            return list.iterator();
        };
    }

    public StreamProcessor transform() {
        this.transformedStream = directKafkaStream.transform(StreamProcessor::transformRecord);
        return this;
    }

    public StreamProcessor appendToHDFS(final SparkSession sql, final String file) {
        transformedStream.foreachRDD(rdd -> {
                    if (rdd.isEmpty()) {
                        return;
                    }
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
                            .parquet(file);
                }
        );
        return this;
    }

    public StreamProcessor filterVehicle() {
        // Check vehicle Id is already processed
        StateSpec<String, IoTData, Boolean, Tuple2<IoTData, Boolean>> stateFunc = StateSpec
                .function(StreamProcessor::updateState)
                .timeout(Durations.seconds(3600));//maintain state for one hour

        //We need filtered stream for total and traffic data calculation
        this.filteredStream = transformedStream
                .mapToPair(iot -> new Tuple2<>(iot.getVehicleId(), iot))
                .reduceByKey((a, b) -> a)
                .mapWithState(stateFunc)
                .filter(tuple -> tuple._2.equals(Boolean.FALSE))
                .map(tuple -> tuple._1);
        return this;
    }

    public StreamProcessor cache() {
        this.filteredStream.cache();
        return this;
    }

    /**
     * Create tuple (IotData, boolean) the boolean will  be defined to true if iot object exists in the state
     *
     * @param str
     * @param iot
     * @param state
     * @return
     */
    private static Tuple2<IoTData, Boolean> updateState(String str, Optional<IoTData> iot, State<Boolean> state) {
        Tuple2<IoTData, Boolean> vehicle = new Tuple2<>(iot.get(), false);
        if (state.exists()) {
            vehicle = new Tuple2<>(iot.get(), true);
        } else {
            state.update(Boolean.TRUE);
        }
        return vehicle;
    }

    public StreamProcessor processPOIData(final Broadcast<POIData> broadcastPOIValues) {
        PointOfInterestProcessor.processPOIData(transformedStream, broadcastPOIValues);
        return this;
    }

    public StreamProcessor processTotalTrafficData() {
        RealtimeTrafficDataProcessor.processTotalTrafficData(filteredStream);
        return this;
    }

    public StreamProcessor processWindowTrafficData() {
        RealtimeTrafficDataProcessor.processWindowTrafficData(filteredStream);
        return this;
    }

    public StreamProcessor processHeatMap() throws IOException {
        RealTimeHeatMapProcessor.processHeatMap(filteredStream);
        return this;
    }
}
