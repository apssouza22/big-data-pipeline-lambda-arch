package com.apssouza.lambda.sensors.model;

import java.io.Serializable;
import java.time.LocalDateTime;

public class Measurement implements Serializable {

  private Coordinate coordinate;

  private Coordinate roundedCoordinate;

  private LocalDateTime timestamp;

  public Measurement(Coordinate coordinate, LocalDateTime timestamp) {
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

  public LocalDateTime getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(LocalDateTime timestamp) {
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
