/**
* @author apssouza22
**/
var map;
function initMap() {
    map = new google.maps.Map(document.getElementById('map'), {
      zoom: 6,
      center: {lat: 53.36, lng: -6.263},
      mapTypeId: 'satellite'
    });
}


 var totalTrafficChartData={
            labels : ["Vehicle"],
            datasets : [{
                label : "Route",
                data : [1]
            }
           ]
        };

var route37TrafficChartData={
            labels : ["Vehicle"],
            datasets : [{
                data : [1]
            }
           ]
        };

var poiTrafficChartData={
            labels : ["Vehicle"],
            datasets : [{
                data : [1]
            }
           ]
        };

jQuery(document).ready(function() {
    //Charts
    var ctx1 = document.getElementById("totalTrafficChart").getContext("2d");
    window.tChart = new Chart(ctx1, {
                type: 'bar',
                data: totalTrafficChartData
            });

    var ctx2 = document.getElementById("route37TrafficChart").getContext("2d");
    window.wChart = new Chart(ctx2, {
                type: 'doughnut',
                data: route37TrafficChartData
            });

    var ctx3 = document.getElementById("poiTrafficChart").getContext("2d");
    window.pChart = new Chart(ctx3, {
                type: 'radar',
                data: poiTrafficChartData
            });


    //tables
    var totalTrafficList = jQuery("#total_traffic");
    var windowTrafficList = jQuery("#window_traffic");
    var poiTrafficList = jQuery("#poi_traffic");

    //use sockjs
    var socket = new SockJS('/stomp');
    var stompClient = Stomp.over(socket);

    stompClient.connect({ }, function(frame) {
        //subscribe "/topic/trafficData" message
        stompClient.subscribe("/topic/trafficData", function(data) {
            var dataList = data.body;
            var resp=jQuery.parseJSON(dataList);
            points = resp.heatMap.map(e => new google.maps.LatLng(e.latitude, e.longitude))
            new google.maps.visualization.HeatmapLayer({
              data: points,
              map: map
            });

            //Grid Box total
            var gridBoxTotal='';
            jQuery.each(resp.heatMap, function(i,vh) {
                 gridBoxTotal +="<tr><td>"+ vh.latitude+" / "+vh.longitude+"</td><td>"+vh.totalCount+"</td></tr>";
            });
            jQuery("#gridBoxContent tbody").html(gridBoxTotal);

            //Total traffic
            var totalOutput='';
            jQuery.each(resp.totalTraffic, function(i,vh) {
                 totalOutput +="<tbody><tr><td>"+ vh.routeId+"</td><td>"+vh.vehicleType+"</td><td>"+vh.totalCount+"</td></tr></tbody>";
            });
            var t_tabl_start = "<table class='table table-bordered table-condensed table-hover innerTable'><thead><tr><th>Route</th><th>Vehicle</th><th>Count</th></tr></thead>";
            var t_tabl_end = "</table>";
            totalTrafficList.html(t_tabl_start+totalOutput+t_tabl_end);

            //Window traffic
            var windowOutput='';
            jQuery.each(resp.windowTraffic, function(i,vh) {
                 windowOutput +="<tbody><tr><td>"+ vh.routeId+"</td><td>"+vh.vehicleType+"</td><td>"+vh.totalCount+"</td></tr></tbody>";
            });
            var w_tabl_start = "<table class='table table-bordered table-condensed table-hover innerTable'><thead><tr><th>Route</th><th>Vehicle</th><th>Count</th></tr></thead>";
            var w_tabl_end = "</table>";
            windowTrafficList.html(w_tabl_start+windowOutput+w_tabl_end);

            //POI data
            var poiOutput='';
            jQuery.each(resp.poiTraffic, function(i,vh) {
                 poiOutput +="<tbody><tr><td>"+vh.vehicleType+"</td><td>"+vh.distance+"</td></tr></tbody>";
            });
            var p_tabl_start = "<table class='table table-bordered table-condensed table-hover innerTable'><thead><tr><th>Vehicle</th><th>Distance</th></tr></thead>";
            var p_tabl_end = "</table>";
            poiTrafficList.html(p_tabl_start+poiOutput+p_tabl_end);

            //draw total traffic chart
            drawBarChart(resp.totalTraffic,totalTrafficChartData);
            window.tChart.update();

            //draw route-37 traffic chart
            drawDoughnutChart(resp.totalTraffic,route37TrafficChartData);
            window.wChart.update();

            //draw poi  chart
            drawRadarChart(resp.poiTraffic,poiTrafficChartData);
            window.pChart.update();

        });
    });
});

function drawBarChart(trafficDetail,trafficChartData){
    //Prepare data for total traffic chart
    var chartLabel = [ "Bus","Large Truck",  "Private Car","Small Truck", "Taxi"];
    var routeName = ["Route-37", "Route-82", "Route-43"];
    var chartData0 =[0,0,0,0,0], chartData1 =[0,0,0,0,0], chartData2 =[0,0,0,0,0];

    jQuery.each(trafficDetail, function(i,vh) {

        if(vh.routeId == routeName[0]){
            chartData0.splice(chartLabel.indexOf(vh.vehicleType),1,vh.totalCount);
        }
        if(vh.routeId == routeName[1]){
            chartData1.splice(chartLabel.indexOf(vh.vehicleType),1,vh.totalCount);
        }
        if(vh.routeId == routeName[2]){
            chartData2.splice(chartLabel.indexOf(vh.vehicleType),1,vh.totalCount);
        }
    });

    var trafficData = {
        labels : chartLabel,
        datasets : [{
            label				  : routeName[0],
            borderColor           : "#878BB6",
            backgroundColor       : "#878BB6",
            data                  : chartData0
        },
        {
            label				  : routeName[1],
            borderColor           : "#4ACAB4",
            backgroundColor       : "#4ACAB4",
            data                  : chartData1
        },
        {
            label				  : routeName[2],
            borderColor           : "#FFEA88",
            backgroundColor       : "#FFEA88",
            data                  : chartData2
        }

        ]
    };
      //update chart
      trafficChartData.datasets=trafficData.datasets;
      trafficChartData.labels=trafficData.labels;
 }

function drawDoughnutChart(trafficDetail,trafficChartData){
    //Prepare data for Doughnut chart
    var chartData =[];
    var chartLabel = [];
    jQuery.each(trafficDetail, function(i,vh) {
        if(vh.routeId == "Route-37"){
            chartLabel.push(vh.vehicleType);
            chartData.push(vh.totalCount);
        }
      });
        var pieChartData = {
        labels : chartLabel,
        datasets : [{
            backgroundColor  : ["#E81574","#DDE815","#B315E8","#e9967a","#90ee90"],
            data             : chartData
        }]
    };

      //update chart
      trafficChartData.datasets=pieChartData.datasets;
      trafficChartData.labels=pieChartData.labels;
}


function drawRadarChart(trafficDetail,trafficChartData){
    var vTypeLabel =["Large Truck", "Small Truck"];
    var chartLabel = [];
    var chartData =[];

    jQuery.each(trafficDetail, function(i,vh) {
        chartData.push(vh.distance);
        //chartLabel.push(vh.vehicleId);
        chartLabel.push('V-'+(i+1));
      });

     var radarChartData = {
        labels : chartLabel,
        datasets : []
      };

      for(i=0; i<chartData.length;i++){
         var zeroFilledArray = new Array(chartData.length);
         for(j=0;j<chartData.length;j++){
                zeroFilledArray[j]=0;
            }
         var clr = getRandomColor();
         zeroFilledArray.splice(i,1,chartData[i]);
          radarChartData.datasets.push(
              {
                label				  : chartLabel[i],
                borderColor           : clr,
                backgroundColor       : clr,
                borderWidth			  : 5,
                data                  : zeroFilledArray
               }
            );
          }

      //update chart
      trafficChartData.datasets=radarChartData.datasets;
      trafficChartData.labels=radarChartData.labels;
    }

 function getRandomColor() {
    return  'rgba(' + Math.round(Math.random() * 255) + ',' + Math.round(Math.random() * 255) + ',' + Math.round(Math.random() * 255) + ',' + ('1') + ')';
};

