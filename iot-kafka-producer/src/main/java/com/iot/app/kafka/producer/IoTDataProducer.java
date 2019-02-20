package com.iot.app.kafka.producer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.iot.app.kafka.util.PropertyFileReader;
import com.iot.app.kafka.vo.IoTData;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * IoT data event producer class which uses Kafka producer for events. 
 * 
 * @author abaghel
 *
 */
public class IoTDataProducer {
	
	private static final Logger logger = Logger.getLogger(IoTDataProducer.class);

	public static void main(String[] args) throws Exception {
		//read config file
		Properties prop = PropertyFileReader.readPropertyFile();		
		String zookeeper = prop.getProperty("com.iot.app.kafka.zookeeper");
		String brokerList = prop.getProperty("com.iot.app.kafka.brokerlist");
		String topic = prop.getProperty("com.iot.app.kafka.topic");
		logger.info("Using Zookeeper=" + zookeeper + " ,Broker-list=" + brokerList + " and topic " + topic);

		// set producer properties
		Properties properties = new Properties();
		properties.put("zookeeper.connect", zookeeper);
		properties.put("metadata.broker.list", brokerList);
		properties.put("request.required.acks", "1");
		properties.put("serializer.class", "com.iot.app.kafka.util.IoTDataEncoder");
		//generate event
		Producer<String, IoTData> producer = new Producer<String, IoTData>(new ProducerConfig(properties));
		IoTDataProducer iotProducer = new IoTDataProducer();
		iotProducer.generateIoTEvent(producer,topic);		
	}


	/**
	 * Method runs in while loop and generates random IoT data in JSON with below format. 
	 * 
	 * {"vehicleId":"52f08f03-cd14-411a-8aef-ba87c9a99997","vehicleType":"Public Transport","routeId":"route-43","latitude":",-85.583435","longitude":"38.892395","timestamp":1465471124373,"speed":80.0,"fuelLevel":28.0}
	 * 
	 * @throws InterruptedException 
	 * 
	 * 
	 */
	private void generateIoTEvent(Producer<String, IoTData> producer, String topic) throws InterruptedException {
		List<String> routeList = Arrays.asList(new String[]{"Route-37", "Route-43", "Route-82"});
		List<String> vehicleTypeList = Arrays.asList(new String[]{"Large Truck", "Small Truck", "Private Car", "Bus", "Taxi"});
		Random rand = new Random();
		logger.info("Sending events");
		// generate event in loop
		while (true) {
			List<IoTData> eventList = new ArrayList<IoTData>();
			for (int i = 0; i < 100; i++) {// create 100 vehicles
				String vehicleId = UUID.randomUUID().toString();
				String vehicleType = vehicleTypeList.get(rand.nextInt(5));
				String routeId = routeList.get(rand.nextInt(3));
				Date timestamp = new Date();
				double speed = rand.nextInt(100 - 20) + 20;// random speed between 20 to 100
				double fuelLevel = rand.nextInt(40 - 10) + 10;
				for (int j = 0; j < 5; j++) {// Add 5 events for each vehicle
					String coords = getCoordinates(routeId);
					String latitude = coords.substring(0, coords.indexOf(","));
					String longitude = coords.substring(coords.indexOf(",") + 1, coords.length());
					IoTData event = new IoTData(vehicleId, vehicleType, routeId, latitude, longitude, timestamp, speed,fuelLevel);
					eventList.add(event);
				}
			}
			Collections.shuffle(eventList);// shuffle for random events
			for (IoTData event : eventList) {
				KeyedMessage<String, IoTData> data = new KeyedMessage<String, IoTData>(topic, event);
				producer.send(data);
				Thread.sleep(rand.nextInt(3000 - 1000) + 1000);//random delay of 1 to 3 seconds
			}
		}
	}
	
	//Method to generate random latitude and longitude for routes
	private String  getCoordinates(String routeId) {
		Random rand = new Random();
		int latPrefix = 0;
		int longPrefix = -0;
		if (routeId.equals("Route-37")) {
			latPrefix = 33;
			longPrefix = -96;
		} else if (routeId.equals("Route-82")) {
			latPrefix = 34;
			longPrefix = -97;
		} else if (routeId.equals("Route-43")) {
			latPrefix = 35;
			longPrefix = -98;
		} 
		Float lati = latPrefix + rand.nextFloat();
		Float longi = longPrefix + rand.nextFloat();
		return lati + "," + longi;
	}
}
