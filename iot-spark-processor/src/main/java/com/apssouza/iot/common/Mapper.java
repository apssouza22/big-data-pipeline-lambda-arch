package com.apssouza.iot.common;

import com.apssouza.iot.common.dto.IoTData;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Mapper {

    public static IoTData parseToIotData(Object[] columns) {
        Date timestamp1 = null;
        try {
            timestamp1 = new SimpleDateFormat("yyyy-MM-dd").parse(columns[0].toString());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        IoTData ioTData = new IoTData(
                columns[1].toString(),
                columns[2].toString(),
                columns[3].toString(),
                columns[4].toString(),
                columns[5].toString(),
                new java.sql.Date(timestamp1.getTime()),
                Double.valueOf(columns[6].toString()),
                Double.valueOf(columns[7].toString())
        );
        return ioTData;
    }
}
