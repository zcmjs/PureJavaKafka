package org.example.mySerializer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaJsonSerializer implements Serializer {

    @Override
    public byte[] serialize(String s, Object o) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsBytes(o);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return retVal;
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
