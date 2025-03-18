package com.roman.conduktor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.roman.conduktor.model.Person;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 3, topics = {"people-topic"}, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" })
class IntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setup() throws JsonProcessingException {
        // Pre-populate the topic with test data
        String[] names = {"John Doe", "Alice", "Bob", "Jack", "Emma", "Liam", "Olivia", "Noah", "Ava", "William", "Sophia", "James", "Isabella", "Benjamin", "Mia", "Lucas", "Charlotte", "Henry", "Amelia", "Alexander"};

        for (int i = 1; i <= 20; i++) {
            kafkaTemplate.send("people-topic", String.valueOf(i), objectMapper.writeValueAsString(new Person(String.valueOf(i), names[i - 1])));
        }

        // Ensure messages are sent before test execution
        kafkaTemplate.flush();
    }

    @Test
    void testGetMessageFromKafkaTopic() throws JsonProcessingException {
        ResponseEntity<String> response = restTemplate.getForEntity("/topic/people-topic/1?count=1", String.class);
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody()).contains("Bob");
        List<Person> people = objectMapper.readValue(response.getBody() , new TypeReference<>() {
        });
        assertThat(people).hasSize(1);
    }

    @Test
    void testGetMessageFromKafkaTopicWithOffset10() throws JsonProcessingException {
        ResponseEntity<String> response = restTemplate.getForEntity("/topic/people-topic/10?count=1", String.class);
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        List<Person> people = objectMapper.readValue(response.getBody() , new TypeReference<>() {
        });
        assertThat(people).hasSize(1);
    }

    @Test
    void testGetMessagesFromKafkaTopicWithCount5() throws JsonProcessingException {
        ResponseEntity<String> response = restTemplate.getForEntity("/topic/people-topic/0?count=5", String.class);
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        List<Person> people = objectMapper.readValue(response.getBody() , new TypeReference<>() {
        });
        assertThat(people).hasSize(5);
    }

    @Test
    void testGetMessagesFromKafkaTopicWithCount10() throws JsonProcessingException {
        ResponseEntity<String> response = restTemplate.getForEntity("/topic/people-topic/5?count=10", String.class);
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        List<Person> people = objectMapper.readValue(response.getBody() , new TypeReference<>() {
        });
        assertThat(people).hasSize(6);
    }

    @Test
    void testGetMessagesFromNonExistingKafkaTopic() {
        ResponseEntity<String> response = restTemplate.getForEntity("/topic/non-existing-topic/0?count=1", String.class);
        assertThat(response.getStatusCode().is4xxClientError()).isTrue();
        assertThat(response.getBody()).isNullOrEmpty();
    }

    @Test
    void testGetMessagesWithOffsetOutOfBound() {
        ResponseEntity<String> response = restTemplate.getForEntity("/topic/people-topic/1000?count=1", String.class);
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody()).isNullOrEmpty();
    }


//    TODO: test isn't clearning kafka, and keeps messages in there
//    @Test
//    void testGetMessagesWithCountOutOfBound() throws JsonProcessingException {
//        ResponseEntity<String> response = restTemplate.getForEntity("/topic/people-topic/0?count=1000", String.class);
//        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
//        List<Person> people = objectMapper.readValue(response.getBody() , new TypeReference<>() {
//        });
//        assertThat(people).hasSize(20);
//    }

    @Test
    void testGetMessagesWithoutCount() throws JsonProcessingException {
        ResponseEntity<String> response = restTemplate.getForEntity("/topic/people-topic/0", String.class);
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        List<Person> people = objectMapper.readValue(response.getBody() , new TypeReference<>() {
        });
        assertThat(people).hasSize(10);
    }

}
