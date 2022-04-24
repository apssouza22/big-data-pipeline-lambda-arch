package com.apssouza.iot.common.dto;

import java.io.Serializable;
import java.util.Date;

/**
 * Measurement entity
 *
 * @author apssouza22
 */
public class Measurement implements Serializable {

    private Coordinate coordinate;

    private Coordinate roundedCoordinate;

    private Date timestamp;

    public Measurement(Coordinate coordinate, Date timestamp) {
        this.coordinate = coordinate;
        this.timestamp = timestamp;
    }

    public Coordinate getCoordinate() {
        return coordinate;
    }

    public void setCoordinate(Coordinate coordinate) {
        this.coordinate = coordinate;
    }

    public Coordinate getRoundedCoordinate() {
        return roundedCoordinate;
    }

    public void setRoundedCoordinate(Coordinate roundedCoordinate) {
        this.roundedCoordinate = roundedCoordinate;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Measurement that = (Measurement) o;

        if (coordinate != null ? !coordinate.equals(that.coordinate) : that.coordinate != null) return false;
        if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = coordinate != null ? coordinate.hashCode() : 0;
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }

}
