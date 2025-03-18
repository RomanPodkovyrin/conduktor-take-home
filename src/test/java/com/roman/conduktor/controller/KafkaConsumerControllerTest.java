package com.roman.conduktor.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.roman.conduktor.model.Person;
import com.roman.conduktor.service.KafkaService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(KafkaConsumerController.class)
class KafkaConsumerControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private KafkaService kafkaService;

    @Test
    void testGetEndpoint() throws Exception {
        String topicName = "people-topic";
        int offset = 10;
        int count = 1;
        Person person = new Person("1", "John Doe");

        List<Person> persons = Collections.singletonList(person);
        // Set up your mock service
        when(kafkaService.consumeMessages(topicName, offset, count)).thenReturn(Optional.of(persons));

        ObjectMapper objectMapper = new ObjectMapper();
        String expectedJson = objectMapper.writeValueAsString(Collections.singletonList(person));

        // Test the controller
        this.mockMvc.perform(get("/topic/{topicName}/{offset}", topicName, offset)
                .param("count", String.valueOf(count)))
                .andExpect(status().isOk())
                .andExpect(content().string(expectedJson));
    }

// TODO test edge cases

    // - offset or count out of bound?
    // non-existing topic
    // no connection to kafka

}