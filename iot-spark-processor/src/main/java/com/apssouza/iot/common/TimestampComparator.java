package com.apssouza.iot.common;


import com.apssouza.iot.common.dto.Measurement;

import java.io.Serializable;
import java.util.Comparator;

public class TimestampComparator implements Comparator<Measurement>, Serializable {

  @Override
  public int compare(Measurement o1, Measurement o2) {
    if(o1 == null && o2 == null) {
      return 0;
    } else if(o1 == null || o1.getTimestamp() == null) {
      return 1;
    } else if(o2 == null || o2.getTimestamp() == null) {
      return -1;
    } else {
      return o1.getTimestamp().compareTo(o2.getTimestamp());
    }
  }

}
