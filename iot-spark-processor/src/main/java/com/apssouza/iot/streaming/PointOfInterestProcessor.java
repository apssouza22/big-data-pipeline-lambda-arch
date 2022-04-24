package com.apssouza.iot.streaming;

import com.apssouza.iot.common.dto.IoTData;
import com.apssouza.iot.common.dto.POIData;
import com.apssouza.iot.common.entity.POITrafficData;
import com.apssouza.iot.common.GeoDistanceCalculator;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;

import org.apache.log4j.Logger;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.joda.time.Duration;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import scala.Tuple2;

public class PointOfInterestProcessor {

    private static final Logger logger = Logger.getLogger(RealtimeTrafficDataProcessor.class);

    /**
     * Method to get and save vehicles that are in POI radius and their distance from POI.
     *
     * @param dataStream original IoT data stream
     * @param broadcastPOIValues variable containing POI coordinates, route and vehicle types to monitor.
     */
    public static void processPOIData(
            JavaDStream<IoTData> dataStream,
            Broadcast<POIData> broadcastPOIValues
    ) {

        JavaDStream<POITrafficData> trafficDStream = dataStream
                .filter(iot -> filterVehicleInPOI(iot, broadcastPOIValues))
                .mapToPair(iot -> new Tuple2<>(iot, broadcastPOIValues.value()))
                .map(PointOfInterestProcessor::transformToPOITrafficData);

        saveToCassandra(trafficDStream);
    }

    private static void saveToCassandra(final JavaDStream<POITrafficData> trafficDStream) {
        // Map Cassandra table column
        Map<String, String> columnNameMappings = new HashMap<>();
        columnNameMappings.put("vehicleId", "vehicleid");
        columnNameMappings.put("distance", "distance");
        columnNameMappings.put("vehicleType", "vehicletype");
        columnNameMappings.put("timeStamp", "timestamp");

        // call CassandraStreamingJavaUtil function to save in DB
        javaFunctions(trafficDStream)
                .writerBuilder(
                        "traffickeyspace",
                        "poi_traffic",
                        CassandraJavaUtil.mapToRow(POITrafficData.class, columnNameMappings)
                )
                .withConstantTTL(Duration.standardSeconds(120))//keeping data for 30 seconds
                .saveToCassandra();
    }

    /**
     * Filter vehicles in the point of interest range
     * @param iot
     * @param broadcastPOIValues
     * @return
     */
    private static boolean filterVehicleInPOI(IoTData iot, Broadcast<POIData> broadcastPOIValues){
        return GeoDistanceCalculator.isInPOIRadius(
                Double.valueOf(iot.getLatitude()),
                Double.valueOf(iot.getLongitude()),
                broadcastPOIValues.value().getLatitude(),
                broadcastPOIValues.value().getLongitude(),
                broadcastPOIValues.value().getRadius()
        );
    }

    private static POITrafficData transformToPOITrafficData(Tuple2<IoTData, POIData> tuple) {
        POITrafficData poiTraffic = new POITrafficData();
        poiTraffic.setVehicleId(tuple._1.getVehicleId());
        poiTraffic.setVehicleType(tuple._1.getVehicleType());
        poiTraffic.setTimeStamp(new Date());
        double distance = GeoDistanceCalculator.getDistance(
                Double.valueOf(tuple._1.getLatitude()).doubleValue(),
                Double.valueOf(tuple._1.getLongitude()).doubleValue(),
                tuple._2.getLatitude(), tuple._2.getLongitude()
        );
        logger.debug("Distance for " + tuple._1.getLatitude() + "," + tuple._1.getLongitude() + "," +
                tuple._2.getLatitude() + "," + tuple._2.getLongitude() + " = " + distance);

        poiTraffic.setDistance(distance);
        return poiTraffic;
    }
}
