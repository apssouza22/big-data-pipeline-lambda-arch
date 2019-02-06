package com.apssouza.lambda.dto;

import java.util.Map;


public class RddEnhanced {
    private final String source;
    private final String timestamp;
    private final String latitude;
    private final String longitude;
    private final Map<String, String> inputInfo;

    public RddEnhanced(String source, String timestamp, String latitude, String longitude, Map<String, String> inputInfo) {
        this.source = source;
        this.timestamp = timestamp;
        this.latitude = latitude;
        this.longitude = longitude;
        this.inputInfo = inputInfo;
    }

    public String getSource() {
        return source;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getLatitude() {
        return latitude;
    }

    public String getLongitude() {
        return longitude;
    }

    public Map<String, String> getInputInfo() {
        return inputInfo;
    }
}
