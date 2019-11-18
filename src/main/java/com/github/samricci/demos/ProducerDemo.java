package com.github.samricci.demos;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class ProducerDemo {

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String TOPIC = "first_topic";

    public static void main(String[] args) {

        KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerProperties());
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "hello world");

        // send data - asynchronous
        producer.send(record);

        //flush data
        //producer.flush();

        //flush and close producer
        producer.close();
    }

    private static Properties getProducerProperties() {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
