package org.example.mySerializer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.mySerializer.config.KafkaJsonSerializer;
import org.example.mySerializer.dto.PersonDTO;
import org.example.mySerializer.util.PersonDTOFakerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

public class JsonProducerDemo {
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String TOPIC = "json_topic";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "10");

        KafkaProducer<String, PersonDTO> producer = new KafkaProducer<>(properties);

        IntStream.rangeClosed(1, 5).forEach(i -> {
            //Stworzenie danych
            ProducerRecord<String, PersonDTO> record = new ProducerRecord<>(TOPIC, PersonDTOFakerFactory.create());
            //send message
            try {
                Thread.sleep(3_000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            Future<RecordMetadata> send = producer.send(record);
            try {
                System.out.println(send.get().toString());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        //close producer
        producer.flush(); //Wszystkie buforowane wiadomosci zostana wysłane przed zamknięciem producenta
        producer.close();
    }


}


//https://oneuptime.com/blog/post/2026-01-24-kafka-producer-batching/view