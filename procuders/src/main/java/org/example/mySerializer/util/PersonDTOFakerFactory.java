package org.example.mySerializer.util;

import com.github.javafaker.Faker;
import org.example.mySerializer.dto.PersonDTO;

public class PersonDTOFakerFactory {

    public static  PersonDTO create() {
        Faker faker = new Faker();
        return new PersonDTO(faker.name().firstName(), faker.name().lastName(), faker.name().title());
    }

}
