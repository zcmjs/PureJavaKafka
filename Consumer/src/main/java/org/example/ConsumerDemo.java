package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Thread.sleep;

public class ConsumerDemo {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
    private static final String GROUP_ID = "java_app";
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String TOPIC = "pure_topic";

    public static void main(String[] args) {
        //create consumer properties. Robic w ten sposob, bo na sztywno wpisanie spowoduje, ze bedziemy odporni na zmiane pakietu
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //Mozemy sie zasubskryowac sie do kilku topicow jednoczesnie. MEtoda subscribe przyjmuje liste topciow do ktorych chcemy sie zasubskrybowac
        //Mozemy podac sztywna nazwe lub uzyc wyrazen regularnych np. singletonList("topic.*) - oznacza ze zasubskrybujemy sie do wszystkich topicow zaczynajacych sie od topic
        consumer.subscribe(Collections.singletonList(TOPIC));
        try {
            while (true) { //bad practise - powinniscmy to obslugiowac w osobnych watkach!!!
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(7_000));
                System.out.println("records.count() : " + records.count());
                records.forEach(record -> {
                    System.out.println(String.format("SUPER INFO: topic = %s, partition = %s, offset = %s, key = %s, value = %s", record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                    // proces data
                });
                sleep(10_000);
                //Dla tego przykladu, gdzie "zadanie" wykonuje sie 10ec, powoduje ze kolejne pobrnaie danych z kafki nastepuje co 17sec
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            consumer.close();
        }

    }
}