package com.apssouza.iot.dashboard;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import com.apssouza.iot.dao.TotalTrafficDataRepository;
import com.apssouza.iot.dao.entity.TotalTrafficData;
import com.apssouza.iot.dao.HeatMapDataRepository;
import com.apssouza.iot.dao.entity.HeatMapData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.apssouza.iot.dao.POITrafficDataRepository;
import com.apssouza.iot.dao.WindowTrafficDataRepository;
import com.apssouza.iot.dao.entity.POITrafficData;
import com.apssouza.iot.dao.entity.WindowTrafficData;

/**
 * Service class to send traffic data messages to dashboard ui at fixed interval using web-socket.
 */
@Service
public class TrafficDataService {
    private static final Logger logger = Logger.getLogger(TrafficDataService.class.getName());

    @Autowired
    private SimpMessagingTemplate template;

    @Autowired
    private TotalTrafficDataRepository totalRepository;

    @Autowired
    private WindowTrafficDataRepository windowRepository;

    @Autowired
    private POITrafficDataRepository poiRepository;

    @Autowired
    private HeatMapDataRepository heatMapDataRepository;

    private static DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    //Method sends traffic data message in every 15 seconds.
    @Scheduled(fixedRate = 15000)
    public void trigger() {
        List<TotalTrafficData> totalTrafficList = new ArrayList<>();
        List<WindowTrafficData> windowTrafficList = new ArrayList<>();
        List<POITrafficData> poiTrafficList = new ArrayList<>();
        List<HeatMapData> heatmapData = new ArrayList<>();
        //Call dao methods
        totalRepository.findTrafficDataByDate(sdf.format(new Date())).forEach(e -> totalTrafficList.add(e));
        windowRepository.findTrafficDataByDate(sdf.format(new Date())).forEach(e -> windowTrafficList.add(e));
        poiRepository.findAll().forEach(e -> poiTrafficList.add(e));
        heatMapDataRepository.findAll().forEach(e -> heatmapData.add(e));
        //prepare response
        Response response = new Response();
        response.setTotalTraffic(totalTrafficList);
        response.setWindowTraffic(windowTrafficList);
        response.setPoiTraffic(poiTrafficList);
        response.setHeatMap(heatmapData);
        logger.info("Sending to UI " + response);
        //send to ui
        this.template.convertAndSend("/topic/trafficData", response);
    }

}
