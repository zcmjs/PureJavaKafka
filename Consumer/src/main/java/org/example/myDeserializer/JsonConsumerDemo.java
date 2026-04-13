package org.example.myDeserializer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.myDeserializer.config.KafkaJsonDeserializer;
import org.example.myDeserializer.dto.PersonDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static java.lang.Thread.sleep;

public class JsonConsumerDemo {
    private static final Logger logger = LoggerFactory.getLogger(JsonConsumerDemo.class);
    private static final String GROUP_ID = "java_app";
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String TOPIC = "json_topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
//        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, PersonDTO> consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new KafkaJsonDeserializer());
        consumer.subscribe(Collections.singletonList(TOPIC));
        try {
            while (true) {
                ConsumerRecords<String, PersonDTO> records = consumer.poll(Duration.ofMillis(7_000));
                System.out.println("records.count() : " + records.count());
                records.forEach(record -> {
                    System.out.println(String.format("SUPER INFO: topic = %s, partition = %s, offset = %s, key = %s, value = %s", record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                    // proces data
                });
                sleep(5_000);
                //Dla tego przykladu, gdzie "zadanie" wykonuje sie 10ec, powoduje ze kolejne pobrnaie danych z kafki nastepuje co 17sec
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            consumer.close();
        }

    }
}