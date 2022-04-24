package com.apssouza.iot.streaming;

import com.apssouza.iot.common.dto.IoTData;
import com.apssouza.iot.common.dto.POIData;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
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
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
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

public class StreamProcessor implements Serializable {

    private static final Logger logger = Logger.getLogger(StreamProcessor.class);

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
                    dfStore.printSchema();
                    dfStore.write()
                            .partitionBy("topic", "kafkaPartition", "dayOfWeek")
                            .mode(SaveMode.Append)
                            .parquet(file);
                }
        );
        return this;
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


    public StreamProcessor filterVehicle() {
        //We need filtered stream for total and traffic data calculation
        var map = mapToPair(transformedStream);
        var key = reduceByKey(map);
        var state = mapWithState(key);
        this.filteredStream = filterByState(state).map(tuple -> tuple._1);
        return this;
    }

    private JavaDStream<Tuple2<IoTData, Boolean>> filterByState(final JavaMapWithStateDStream<String, IoTData, Boolean, Tuple2<IoTData, Boolean>> state) {
        var dsStream = state .filter(tuple -> tuple._2.equals(Boolean.FALSE));
        logger.info("Starting Stream Processing");
        dsStream.print();
        return dsStream;
    }

    private JavaMapWithStateDStream<String, IoTData, Boolean, Tuple2<IoTData, Boolean>> mapWithState(final JavaPairDStream<String, IoTData> key) {
        // Check vehicle Id is already processed
        StateSpec<String, IoTData, Boolean, Tuple2<IoTData, Boolean>> stateFunc = StateSpec
                .function(StreamProcessor::updateState)
                .timeout(Durations.seconds(3600));//maintain state for one hour

        var dStream = key.mapWithState(stateFunc);
        dStream.print();
        return dStream;
    }

    private JavaPairDStream<String, IoTData> reduceByKey(final JavaPairDStream<String, IoTData> map) {
        JavaPairDStream<String, IoTData> dStream = map.reduceByKey((a, b) -> a);
        dStream.print();
        return dStream;
    }

    private JavaPairDStream<String, IoTData> mapToPair(final JavaDStream<IoTData> stream){
        var dStream = stream.mapToPair(iot -> new Tuple2<>(iot.getVehicleId(), iot));
        dStream.print();
        return dStream;
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

}
