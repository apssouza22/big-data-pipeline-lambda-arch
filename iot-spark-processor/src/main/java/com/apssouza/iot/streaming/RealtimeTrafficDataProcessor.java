package com.apssouza.iot.streaming;

import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.apssouza.iot.common.dto.AggregateKey;
import com.apssouza.iot.common.entity.WindowTrafficData;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.apssouza.iot.common.entity.TotalTrafficData;
import com.apssouza.iot.common.dto.IoTData;

import scala.Tuple2;

/**
 * Class to process IoT data stream and to produce traffic data details.
 *
 * @author abaghel
 */
public class RealtimeTrafficDataProcessor {

    private static final Logger logger = Logger.getLogger(RealtimeTrafficDataProcessor.class);

    /**
     * Method to get window traffic counts of different type of vehicles for each route. Window duration = 30 seconds
     * and Slide interval = 10 seconds
     *
     * @param filteredIotDataStream IoT data stream
     */
    public static void processWindowTrafficData(JavaDStream<IoTData> filteredIotDataStream) {
        // reduce by key and window (30 sec window and 10 sec slide).
        JavaDStream<WindowTrafficData> trafficDStream = filteredIotDataStream
                .mapToPair(iot -> new Tuple2<>(new AggregateKey(iot.getRouteId(), iot.getVehicleType()), 1L))
                .reduceByKeyAndWindow((a, b) -> a + b, Durations.seconds(30), Durations.seconds(10))
                .map(RealtimeTrafficDataProcessor::mapToWindowTrafficData);

        saveWindTrafficData(trafficDStream);
    }

    /**
     * Method to get total traffic counts of different type of vehicles for each route.
     *
     * @param filteredIotDataStream IoT data stream
     */
    public static void processTotalTrafficData(JavaDStream<IoTData> filteredIotDataStream) {
        // Need to keep state for total count
        StateSpec<AggregateKey, Long, Long, Tuple2<AggregateKey, Long>> stateSpec = StateSpec
                .function(RealtimeTrafficDataProcessor::updateState)
                .timeout(Durations.seconds(3600));

        // We need to get count of vehicle group by routeId and vehicleType
        JavaDStream<TotalTrafficData> trafficDStream = filteredIotDataStream
                .mapToPair(iot -> new Tuple2<>(new AggregateKey(iot.getRouteId(), iot.getVehicleType()), 1L))
                .reduceByKey((a, b) -> a + b)
                .mapWithState(stateSpec)
                .map(tuple2 -> tuple2)
                .map(RealtimeTrafficDataProcessor::mapToTrafficData);

        saveTotalTrafficData(trafficDStream);
    }

    private static void saveTotalTrafficData(final JavaDStream<TotalTrafficData> trafficDStream) {
        // Map Cassandra table column
        HashMap<String, String> columnNameMappings = new HashMap<>();
        columnNameMappings.put("routeId", "routeid");
        columnNameMappings.put("vehicleType", "vehicletype");
        columnNameMappings.put("totalCount", "totalcount");
        columnNameMappings.put("timeStamp", "timestamp");
        columnNameMappings.put("recordDate", "recorddate");

        // call CassandraStreamingJavaUtil function to save in DB
        javaFunctions(trafficDStream).writerBuilder(
                "traffickeyspace",
                "total_traffic",
                CassandraJavaUtil.mapToRow(TotalTrafficData.class, columnNameMappings)
        ).saveToCassandra();
    }


    private static void saveWindTrafficData(final JavaDStream<WindowTrafficData> trafficDStream) {
        // Map Cassandra table column
        HashMap<String, String> columnNameMappings = new HashMap<>();
        columnNameMappings.put("routeId", "routeid");
        columnNameMappings.put("vehicleType", "vehicletype");
        columnNameMappings.put("totalCount", "totalcount");
        columnNameMappings.put("timeStamp", "timestamp");
        columnNameMappings.put("recordDate", "recorddate");

        // call CassandraStreamingJavaUtil function to save in DB
        javaFunctions(trafficDStream).writerBuilder(
                "traffickeyspace",
                "window_traffic",
                CassandraJavaUtil.mapToRow(WindowTrafficData.class, columnNameMappings)
        ).saveToCassandra();
    }

    /**
     * Function to create WindowTrafficData object from IoT data
     *
     * @param tuple
     * @return
     */
    private static WindowTrafficData mapToWindowTrafficData(Tuple2<AggregateKey, Long> tuple) {
        logger.debug("Window Count : " +
                "key " + tuple._1().getRouteId() + "-" + tuple._1().getVehicleType() +
                " value " + tuple._2());

        WindowTrafficData trafficData = new WindowTrafficData();
        trafficData.setRouteId(tuple._1().getRouteId());
        trafficData.setVehicleType(tuple._1().getVehicleType());
        trafficData.setTotalCount(tuple._2());
        trafficData.setTimeStamp(new Date());
        trafficData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
        return trafficData;
    }

    private static TotalTrafficData mapToTrafficData(Tuple2<AggregateKey, Long> tuple) {
        logger.debug(
                "Total Count : " + "key " + tuple._1().getRouteId() + "-" + tuple._1().getVehicleType() + " value " +
                        tuple._2());
        TotalTrafficData trafficData = new TotalTrafficData();
        trafficData.setRouteId(tuple._1().getRouteId());
        trafficData.setVehicleType(tuple._1().getVehicleType());
        trafficData.setTotalCount(tuple._2());
        trafficData.setTimeStamp(new Date());
        trafficData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
        return trafficData;
    }


    /**
     * Function to get running sum by maintaining the state
     *
     * @param key
     * @param currentSum
     * @param state
     * @return
     */
    private static Tuple2<AggregateKey, Long> updateState(
            AggregateKey key,
            org.apache.spark.api.java.Optional<Long> currentSum,
            State<Long> state
    ) {
        Long objectOption = currentSum.get();
        objectOption = objectOption == null ? 0l : objectOption;
        long totalSum = objectOption + (state.exists() ? state.get() : 0);
        Tuple2<AggregateKey, Long> total = new Tuple2<>(key, totalSum);
        state.update(totalSum);
        return total;
    }

}
