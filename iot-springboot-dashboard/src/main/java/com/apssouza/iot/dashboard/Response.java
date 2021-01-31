package com.apssouza.iot.dashboard;

import java.io.Serializable;
import java.util.List;

import com.apssouza.iot.dao.entity.HeatMapData;
import com.apssouza.iot.dao.entity.POITrafficData;
import com.apssouza.iot.dao.entity.TotalTrafficData;
import com.apssouza.iot.dao.entity.WindowTrafficData;

/**
 * Response object containing traffic details that will be sent to dashboard.
 *
 * @author abaghel
 */
public class Response implements Serializable {
    private List<TotalTrafficData> totalTraffic;
    private List<WindowTrafficData> windowTraffic;
    private List<POITrafficData> poiTraffic;
    private List<HeatMapData> heatMap;

    public List<TotalTrafficData> getTotalTraffic() {
        return totalTraffic;
    }

    public void setTotalTraffic(List<TotalTrafficData> totalTraffic) {
        this.totalTraffic = totalTraffic;
    }

    public List<WindowTrafficData> getWindowTraffic() {
        return windowTraffic;
    }

    public void setWindowTraffic(List<WindowTrafficData> windowTraffic) {
        this.windowTraffic = windowTraffic;
    }

    public List<POITrafficData> getPoiTraffic() {
        return poiTraffic;
    }

    public void setPoiTraffic(List<POITrafficData> poiTraffic) {
        this.poiTraffic = poiTraffic;
    }

    public void setHeatMap(List<HeatMapData> heatMap) {
        this.heatMap = heatMap;
    }

    public List<HeatMapData> getHeatMap() {
        return heatMap;
    }
}
