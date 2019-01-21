package com.apssouza.lambda;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class AppProducer {

    public static void main(String[] args) throws Exception {

        String topicName = "cordinations";
        Producer<Long, String> producer = createProducer();

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>(topicName, "msg test" + Integer.toString(i)));
            producer.flush();
        }
        producer.close();
        System.out.println("Message sent successfully");
    }

    public static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "client_id");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 600);
        return new KafkaProducer<>(props);
    }
}
