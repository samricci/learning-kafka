package com.github.samricci.demos;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class ConsumerAssignSeekDemo {
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String OFFSET_RESET = "earliest";
    private static final String TOPIC = "first_topic";

    public static void main(String[] args) {
        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProperties());

        // assign and seek are mostly used to replay data or fetch a specific message

        // assign
        TopicPartition partitionReadToReadFrom = new TopicPartition(TOPIC, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(Collections.singleton(partitionReadToReadFrom));

        // seek
        consumer.seek(partitionReadToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesSoFar = 0;

        // poll for new data
        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesSoFar++;
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());

                if(numberOfMessagesSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }

            logger.info("Exiting the application");
        }
    }

    private static Properties getConsumerProperties() {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        /*
        earliest - read from the very beginning of your topic
        latest - only the new messages
         */
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET);
        return properties;
    }
}
