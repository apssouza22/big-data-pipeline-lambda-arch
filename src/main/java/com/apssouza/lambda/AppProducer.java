package com.apssouza.lambda;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class AppProducer {

    public static void main(String[] args) throws Exception {
        String topicName = "heatmap2";
        Producer<Long, String> producer = createProducer();

        String csvFile = "/Users/apssouza/Projetos/java/lambda-arch/data/spark/input/localhost.csv";
        try (Stream<String> stream = Files.lines(Paths.get(csvFile))) {
            stream.skip(1).forEach(line -> {
                producer.send(new ProducerRecord<>(topicName, line));
            });
        }
        producer.flush();
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
