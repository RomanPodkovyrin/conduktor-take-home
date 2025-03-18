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
import java.util.Optional;

@RestController
@RequestMapping("/topic")
public class KafkaConsumerController {
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
            @RequestParam(required = false) Integer count ){
        logger.info("GET /topic/{}/{}?count={}", topicName, offset, count);


        return getResponseFromKafka(topicName, offset, (count != null) ? count: Integer.parseInt(defaultCount));
    }

    @GetMapping("/{topicName}")
    public ResponseEntity<List<Person>> getMessages(
            @PathVariable String topicName,
            @RequestParam(required = false) Integer count) {
        logger.info("GET /topic/{}?count={}", topicName, count);
        return getResponseFromKafka(topicName, Integer.parseInt(defaultOffset), (count != null) ? count: Integer.parseInt(defaultCount));
    }

    private ResponseEntity<List<Person>> getResponseFromKafka(String topicName, int offset, int count) {
        Optional<List<Person>> messages = this.kafkaService.consumeMessages(topicName, offset, count);

        if (messages.isEmpty()) {
            return ResponseEntity.status(404).build();
        }

        return ResponseEntity.ok(messages.get());
    }

}
