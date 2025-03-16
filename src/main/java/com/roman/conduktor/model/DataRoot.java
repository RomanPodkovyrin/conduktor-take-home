package com.roman.conduktor.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class DataRoot {
    @JsonProperty("ctRoot")
    private List<Person> people;
}
