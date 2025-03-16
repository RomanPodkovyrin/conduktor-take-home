package com.roman.conduktor.model.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.roman.conduktor.model.Person;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class PersonDeserializer implements Deserializer<Person> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public Person deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, Person.class);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing Person", e);
        }
    }

    @Override
    public void close() {}
}