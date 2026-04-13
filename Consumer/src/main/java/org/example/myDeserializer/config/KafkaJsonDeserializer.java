package org.example.myDeserializer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.example.myDeserializer.dto.PersonDTO;

public class KafkaJsonDeserializer implements Deserializer<PersonDTO> {

    @Override
    public PersonDTO deserialize(String topic, byte[] data) {
        PersonDTO personDTO = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            personDTO = objectMapper.readValue(data, PersonDTO.class);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return personDTO;
    }

}
