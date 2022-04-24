package com.apssouza.iot.common.entity;

import java.io.Serializable;
import java.util.Date;

/**
 * Heatmap data entity
 *
 */
public class HeatMapData implements Serializable {

    private double latitude;
    private double longitude;
    private int totalCount;
    private Date timeStamp;

    public HeatMapData(double latitude, double longitude, int count, Date timeStamp) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.totalCount = count;
        this.timeStamp = timeStamp;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public Date getTimeStamp() {
        return timeStamp;
    }
}
