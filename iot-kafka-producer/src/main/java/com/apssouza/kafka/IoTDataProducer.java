package com.apssouza.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.log4j.Logger;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * IoT data event producer class which uses Kafka producer to send iot data events to Kafka
 */
public class IoTDataProducer {

    private static final Logger logger = Logger.getLogger(IoTDataProducer.class);
    private final Producer<String, IoTData> producer;

    public IoTDataProducer(final Producer<String, IoTData> producer) {
        this.producer = producer;
    }

    public static void main(String[] args) throws Exception {
        Properties properties = PropertyFileReader.readPropertyFile();
        Producer<String, IoTData> producer = new Producer<>(new ProducerConfig(properties));
        IoTDataProducer iotProducer = new IoTDataProducer(producer);
        iotProducer.generateIoTEvent(properties.getProperty("kafka.topic"));
    }

    /**
     * Method runs in while loop and generates random IoT data in JSON with below format.
     * <p>
     * {"vehicleId":"52f08f03-cd14-411a-8aef-ba87c9a99997","vehicleType":"Public Transport","routeId":"route-43","latitude":",-85.583435","longitude":"38.892395","timestamp":1465471124373,"speed":80.0,"fuelLevel":28.0}
     *
     * @throws InterruptedException
     */
    private void generateIoTEvent(String topic) throws InterruptedException {
        List<String> routeList = Arrays.asList(
                new String[]{"Route-37", "Route-43", "Route-82"}
        );
        List<String> vehicleTypeList = Arrays.asList(
                new String[]{"Large Truck", "Small Truck", "Private Car", "Bus", "Taxi"}
        );
        Random rand = new Random();
        logger.info("Sending events");

        while (true) {
            List<IoTData> events = generateVehicleWithPositions(routeList, vehicleTypeList, rand);
            for (IoTData event : events) {
                producer.send(new KeyedMessage<>(topic, event));
            }
            Thread.sleep(rand.nextInt(5000 - 2000) + 2000);//random delay of 2 to 5 seconds
        }
    }

    private List<IoTData> generateVehicleWithPositions(
            final List<String> routeList,
            final List<String> vehicleTypeList,
            final Random rand
    ) {
        List<IoTData> eventList = new ArrayList<>();
        String vehicleId = UUID.randomUUID().toString();
        String vehicleType = vehicleTypeList.get(rand.nextInt(5));
        String routeId = routeList.get(rand.nextInt(3));
        Date timestamp = new Date();
        double speed = rand.nextInt(80) + 20;// random speed between 20 to 100
        double fuelLevel = rand.nextInt(30) + 10;
        float []coords = getCoordinates();
        for (int i = 0; i < 5; i++) {// Add 5 events for each vehicle (Moving)
            coords[0] = coords[0] + (float)0.0001;
            coords[1] = coords[1] + (float)0.0001;
            IoTData event = new IoTData(
                    vehicleId,
                    vehicleType,
                    routeId,
                    String.format("%s", coords[0]),
                    String.format("%s", coords[1]),
                    timestamp,
                    speed,
                    fuelLevel
            );
            eventList.add(event);
        }
        return eventList;
    }


    /**
     * Method to generate random latitude and longitude for routes
     * @return
     */
    private float[] getCoordinates() {
        Random rand = new Random();
        int latPrefix = rand.nextInt(3) + 52;
        int longPrefix = rand.nextInt(3) + 7;
        float latitude = latPrefix + rand.nextFloat();
        float longitude = longPrefix + rand.nextFloat();
        longitude = longitude * -1;
        return new float[]{latitude, longitude};
    }
}
