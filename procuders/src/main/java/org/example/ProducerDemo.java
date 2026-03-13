package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.stream.IntStream;



//Dla poniższego zadania, zawsze eventy trafiały na jedna partycje

/****
 *Dlaczego 3 sekundy nie wymuszają zmiany?
 *
 * W Twoim kodzie wysyłasz tylko 10 bardzo małych wiadomości. Domyślny rozmiar batcha (batch.size) to zazwyczaj 16 KB.
 *     Twoje wiadomości "Hello World" mają zaledwie kilkanaście bajtów.
 *     Nawet po 30 sekundach (10 x 3s) całkowity rozmiar danych jest zbyt mały, by wymusić przejście na nową partycję z powodu przepełnienia bufora.
 *     Producent po prostu uznaje, że najefektywniej jest wysłać to na tę samą partycję, do której ma już otwarte połączenie i zainicjalizowany batch.
 *
 */

// --------------> przy zmianie BATCH_SIZE_CONFIG z default na 1, kafka zaczela rozrzucac wiadomosci

public class ProducerDemo {
//    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemo.class);
private static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static void main(String[] args) {
//        LOGGER.info("Hello and welcome!");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "1");


        //Ten obiekt będzie odpowiedzialny za wysyłanie wiadomości do serwera kafka
        //Tutaj określeśmy jaki typ danych będzie klueczem i wartością wiadomości
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //Przy kazdym uruchomieniu aplikacji, dane byly wyslane na inna partycje. - Zadziałał tutaj 2 mechanizmy optymalizacji. StickyPartitioner oraz Batching
        IntStream.rangeClosed(1, 10).forEach(value -> {
            //Stworzenie danych
            ProducerRecord<String, String> record = new ProducerRecord<>("testovirone", "Hellow WorldX-" + value);
            //send message
            try {
                Thread.sleep(3_000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            producer.send(record);
        });


        //close producer
        producer.flush(); //Wszystkie buforowane wiadomosci zostana wysłane przed zamknięciem producenta
        producer.close();
    }
}

//https://oneuptime.com/blog/post/2026-01-24-kafka-producer-batching/view