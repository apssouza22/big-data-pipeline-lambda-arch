package com.iot.app.springboot.dashboard;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import com.iot.app.springboot.dao.HeatMapDataRepository;
import com.iot.app.springboot.dao.entity.HeatMapData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.iot.app.springboot.dao.POITrafficDataRepository;
import com.iot.app.springboot.dao.TotalTrafficDataRepository;
import com.iot.app.springboot.dao.WindowTrafficDataRepository;
import com.iot.app.springboot.dao.entity.POITrafficData;
import com.iot.app.springboot.dao.entity.TotalTrafficData;
import com.iot.app.springboot.dao.entity.WindowTrafficData;
import com.iot.app.springboot.vo.Response;

/**
 * Service class to send traffic data messages to dashboard ui at fixed interval using web-socket.
 *
 * @author abaghel
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

    //Method sends traffic data message in every 5 seconds.
    @Scheduled(fixedRate = 5000)
    public void trigger() {
        List<TotalTrafficData> totalTrafficList = new ArrayList<TotalTrafficData>();
        List<WindowTrafficData> windowTrafficList = new ArrayList<WindowTrafficData>();
        List<POITrafficData> poiTrafficList = new ArrayList<POITrafficData>();
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
