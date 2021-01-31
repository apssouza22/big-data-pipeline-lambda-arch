package com.apssouza.iot.dao.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

import java.io.Serializable;
import java.util.Date;

/**
 * Heatmap data entity
 * @author apssouza22
 */
@Table("heat_map")
public class HeatMapData implements Serializable {
    @PrimaryKeyColumn(name = "latitude",ordinal = 0,type = PrimaryKeyType.PARTITIONED)
    private double latitude;
    @PrimaryKeyColumn(name = "longitude",ordinal = 1,type = PrimaryKeyType.CLUSTERED)
    private double longitude;
    @Column(value = "totalcount")
    private int totalCount;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone="MST")
    @Column(value = "timestamp")
    private Date timeStamp;

    public HeatMapData(double latitude, double longitude, int totalCount, Date timeStamp) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.totalCount = totalCount;
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

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }

    public void setTimeStamp(Date timeStamp) {
        this.timeStamp = timeStamp;
    }
}
