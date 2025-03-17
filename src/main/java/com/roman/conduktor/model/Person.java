package com.roman.conduktor.model;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
public class Person {

    @JsonProperty("_id")
    private String id;

    private String name;
    private String dob;
    private Address address;
    private String telephone;
    private List<String> pets;
    private Double score;
    private String email;
    private String url;
    private String description;
    private Boolean verified;
    private Integer salary;

    //TODO: Could have used @RequiredArgsConstructor
    public Person(String id, String name) {
        this.id = id;
        this.name = name;
    }
    @Data
    public static class Address {
        private String street;
        private String town;
        //TODO: This field is named "postode" in the JSON file
        @JsonProperty("postode")
        private String postcode;
    }
}