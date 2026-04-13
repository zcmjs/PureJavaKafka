package org.example.myDeserializer.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PersonDTO {
    private String firstName;
    private String lastName;
    private String title;

    public PersonDTO(@JsonProperty("firstName")  String firstName, @JsonProperty("lastName") String lastName, @JsonProperty("title") String title) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.title = title;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @Override
    public String toString() {
        return "PersonDTO{" +
                "firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", title='" + title + '\'' +
                '}';
    }
}
