package com.iot.app.spark.util;

/**
 * Class to calculate the distance between to locations on earth using coordinates (latitude and longitude).
 * This class uses "haversine" formula to calculate the great-circle distance between two points on earth.
 * http://www.movable-type.co.uk/scripts/latlong.html  
 * 
 * @author abaghel
 *
 */
public class GeoDistanceCalculator {
	/**
	 * Method to get shortest distance over the earth’s surface in Kilometer between two locations
	 * 
	 * @param lat1 latitude of location A
	 * @param lon1 longitude of location A
	 * @param lat2 latitude of location B
	 * @param lon2 longitude of location B
	 * @return distance between A and B in Kilometer
	 * 
	 */
	public static double getDistance(double lat1, double lon1, double lat2, double lon2) {
		//Earth radius in KM
		final int r = 6371;

		Double latDistance = Math.toRadians(lat2 - lat1);
		Double lonDistance = Math.toRadians(lon2 - lon1);
		Double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(lat1))
				* Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
		Double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		double distance = r * c;
		
		return distance;
	}
	
	/**
	 * Method to check if current location is in radius of point of interest (POI) location
	 * 
	 * @param currentLat latitude of current location
	 * @param currentLon longitude of current location
	 * @param poiLat latitude of POI location
	 * @param poiLon longitude of POI location
	 * @param radius radius in Kilometer from POI
	 * @return true if in POI radius otherwise false
	 * 
	 */
	public static boolean isInPOIRadius(double currentLat, double currentLon, double poiLat, double poiLon,double radius){
		double distance = getDistance(currentLat,currentLon,poiLat,poiLon);
		 if(distance <= radius){
			 return true;
		 }
		return false;
	}

}