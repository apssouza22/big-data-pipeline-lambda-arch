package com.apssouza.iot.processor;

import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.joda.time.Duration;

import com.apssouza.iot.dto.AggregateKey;
import com.apssouza.iot.dto.POIData;
import com.apssouza.iot.entity.WindowTrafficData;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.apssouza.iot.entity.POITrafficData;
import com.apssouza.iot.entity.TotalTrafficData;
import com.apssouza.iot.util.GeoDistanceCalculator;
import com.apssouza.iot.dto.IoTData;

import scala.Tuple2;
import scala.Tuple3;

/**
 * Class to process IoT data stream and to produce traffic data details.
 *
 * @author abaghel
 */
public class RealtimeTrafficDataProcessor {
    private static final Logger logger = Logger.getLogger(RealtimeTrafficDataProcessor.class);

    /**
     * Method to get total traffic counts of different type of vehicles for each route.
     *
     * @param filteredIotDataStream IoT data stream
     */
    public void processTotalTrafficData(JavaDStream<IoTData> filteredIotDataStream) {

        // We need to get count of vehicle group by routeId and vehicleType
        JavaPairDStream<AggregateKey, Long> countDStreamPair = filteredIotDataStream
                .mapToPair(iot -> new Tuple2<>(new AggregateKey(iot.getRouteId(), iot.getVehicleType()), 1L))
                .reduceByKey((a, b) -> a + b);

        // Need to keep state for total count
        StateSpec<AggregateKey, Long, Long, Tuple2<AggregateKey, Long>> stateSpec =
                StateSpec.function(totalSumFunc).timeout(Durations.seconds(3600));

        JavaMapWithStateDStream<AggregateKey, Long, Long, Tuple2<AggregateKey, Long>> countDStreamWithStatePair =
                countDStreamPair.mapWithState(stateSpec);//maintain state for one hour

        // Transform to dstream of TrafficData
        JavaDStream<Tuple2<AggregateKey, Long>> countDStream = countDStreamWithStatePair.map(tuple2 -> tuple2);
        JavaDStream<TotalTrafficData> trafficDStream = countDStream.map(totalTrafficDataFunc);

        // Map Cassandra table column
        Map<String, String> columnNameMappings = new HashMap<String, String>();
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

    /**
     * Method to get window traffic counts of different type of vehicles for each route. Window duration = 30 seconds
     * and Slide interval = 10 seconds
     *
     * @param filteredIotDataStream IoT data stream
     */
    public void processWindowTrafficData(JavaDStream<IoTData> filteredIotDataStream) {

        // reduce by key and window (30 sec window and 10 sec slide).
        var  trafficDStream = filteredIotDataStream
                .mapToPair(iot -> new Tuple2<>(
                        new AggregateKey(iot.getRouteId(), iot.getVehicleType()),
                        1L
                ))
                .reduceByKeyAndWindow((a, b) -> a + b,
                        Durations.seconds(30),
                        Durations.seconds(10)
                )
                .map(windowTrafficDataFunc);

        // Map Cassandra table column
        var columnNameMappings = new HashMap<String, String>();
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



    //Function to get running sum by maintaining the state
    private static final Function3<AggregateKey, org.apache.spark.api.java.Optional<Long>, State<Long>, Tuple2<AggregateKey, Long>>
            totalSumFunc = (key, currentSum, state) -> {
        Long objectOption = currentSum.get();
        objectOption = objectOption == null ? 0l : objectOption;
        long totalSum = objectOption + (state.exists() ? state.get() : 0);
        Tuple2<AggregateKey, Long> total = new Tuple2<>(key, totalSum);
        state.update(totalSum);
        return total;
    };

    //Function to create TotalTrafficData object from IoT data
    private static final Function<Tuple2<AggregateKey, Long>, TotalTrafficData> totalTrafficDataFunc = (tuple -> {
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
    });

    //Function to create WindowTrafficData object from IoT data
    private static final Function<Tuple2<AggregateKey, Long>, WindowTrafficData> windowTrafficDataFunc = (tuple -> {
        logger.debug(
                "Window Count : " + "key " + tuple._1().getRouteId() + "-" + tuple._1().getVehicleType() + " value " +
                        tuple._2());
        WindowTrafficData trafficData = new WindowTrafficData();
        trafficData.setRouteId(tuple._1().getRouteId());
        trafficData.setVehicleType(tuple._1().getVehicleType());
        trafficData.setTotalCount(tuple._2());
        trafficData.setTimeStamp(new Date());
        trafficData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
        return trafficData;
    });

}
