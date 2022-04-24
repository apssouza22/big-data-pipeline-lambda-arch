package com.apssouza.iot.common.dto;

import java.io.Serializable;

/**
 * Class to represent attributes of POI
 *
 * @author abaghel
 */
public class POIData implements Serializable {
    private double latitude;
    private double longitude;
    private double radius;
    private String vehicle;
    private String route;

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getRadius() {
        return radius;
    }

    public void setRadius(double radius) {
        this.radius = radius;
    }

    public void setVehicle(final String truck) {
        this.vehicle = truck;
    }

    public void setRoute(final String route) {
        this.route = route;
    }

    public String getVehicle() {
        return vehicle;
    }

    public String getRoute() {
        return route;
    }
}
