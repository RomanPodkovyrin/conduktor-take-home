package com.roman.conduktor.controller;

import com.roman.conduktor.model.Person;
import com.roman.conduktor.service.KafkaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/topic")
public class KafkaConsumerController {
    // TODO: how to handle default values in the controller?
    @Value("${api.default.offset}")
    private String defaultOffset;

    @Value(("${api.default.count}"))
    private String defaultCount;

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerController.class);

    private final KafkaService kafkaService;


    @Autowired
    public KafkaConsumerController(KafkaService kafkaService) {
        this.kafkaService = kafkaService;
    }

    @GetMapping("/{topicName}/{offset}")
    public ResponseEntity<List<Person>> getMessages(
            @PathVariable String topicName,
            @PathVariable int offset,
            @RequestParam(defaultValue = "10") int count) {
        //todo: handle incorrect topic or non existent one
        logger.info("getMessages: {}", topicName);

        // TODO: Set offset to defaultOffset if offset is not provided
        List<Person> messages = this.kafkaService.consumeMessages(topicName, offset, count);

        logger.info("Received request to get {} messages from topic {} at offset {}",
                count, topicName, offset);

        return messages.isEmpty() ? ResponseEntity.noContent().build() : ResponseEntity.ok(messages);
    }

    // TODO: add endpoint for Get /topic/{topicName}?count=n as it says to have default for offset

}
