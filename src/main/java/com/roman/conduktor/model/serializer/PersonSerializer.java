package com.roman.conduktor.model.serializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.roman.conduktor.model.Person;
import org.apache.kafka.common.serialization.Serializer;


public class PersonSerializer implements Serializer<Person> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, Person person) {
        try {
            return objectMapper.writeValueAsBytes(person);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing Person", e);
        }
    }
}
