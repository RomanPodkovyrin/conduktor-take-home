package com.roman.conduktor.loader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.roman.conduktor.model.DataRoot;
import com.roman.conduktor.model.Person;
import com.roman.conduktor.service.KafkaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@Component
public class DataLoader {

    private static final Logger logger = LoggerFactory.getLogger(DataLoader.class);

    private final ObjectMapper objectMapper;
    private final KafkaService kafkaService;

    @Value("${kafka.topic.name}")
    private String topicName;

    @Autowired
    public DataLoader(ObjectMapper objectMapper, KafkaService kafkaService) {
        this.kafkaService = kafkaService;
        this.objectMapper = objectMapper;
    }

    public void loadDataToKafka() {
        // prepare kafka topic
        this.kafkaService.setupTopic();

        try {
            List<Person> people = readPeopleFromJson();

            logger.info("Loaded {} people from JSON file", people.size());

            for (Person person : people) {
                kafkaService.sendMessage(topicName, person.getId(), person);
                logger.info("Sent person with ID: {} to Kafka", person.getId());
            }

            logger.info("All data loaded successfully into Kafka topic: {}", topicName);
        } catch (Exception e) {
            logger.error("Error loading data to Kafka", e);
        }
    }

    private List<Person> readPeopleFromJson() throws IOException {
        ClassPathResource resource = new ClassPathResource("random-people-data.json");
        try (InputStream inputStream = resource.getInputStream()) {
            return objectMapper.readValue(inputStream, DataRoot.class).getPeople();
        }
    }
}