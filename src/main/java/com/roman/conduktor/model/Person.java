package com.roman.conduktor.model;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
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

    @Data
    public static class Address {
        private String street;
        private String town;
        //TODO: This field is named "postode" in the JSON file
        @JsonProperty("postode")
        private String postcode;
    }
}