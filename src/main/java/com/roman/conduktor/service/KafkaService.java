package com.roman.conduktor.service;


import com.roman.conduktor.config.KafkaConfig;
import com.roman.conduktor.model.Person;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;


@Service
public class KafkaService {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KafkaService.class);
    private final KafkaConfig kafkaConfig;
    private final AdminClient adminClient;
    private final KafkaProducer<String, Person> kafkaProducer;
    private final KafkaConsumer<String, Person> kafkaConsumer;

    @Autowired
    public KafkaService(KafkaConfig kafkaConfig, AdminClient adminClient) {
        this.adminClient = adminClient;
        this.kafkaConfig = kafkaConfig;
        this.kafkaProducer = new KafkaProducer<>(this.kafkaConfig.producerConfigs());
        this.kafkaConsumer = new KafkaConsumer<>(this.kafkaConfig.consumerConfigs());
        logger.info("KafkaService created");
    }

    @PreDestroy
    public void close() {
        logger.info("Closing KafkaService");
        adminClient.close();
        kafkaProducer.close();
        kafkaConsumer.close();
    }

    public void setupTopic() {
        logger.info("property topicName: {}, partitions: {}, replicationFactor: {}", this.kafkaConfig.getTopicName(), this.kafkaConfig.getPartitions(), this.kafkaConfig.getReplicationFactor());
        if (topicExists(this.kafkaConfig.getTopicName())) {
            logger.info("Topic {} already exists", this.kafkaConfig.getTopicName());
            logger.info("Deleting topic {}", this.kafkaConfig.getTopicName());
            adminClient.deleteTopics(Collections.singleton(this.kafkaConfig.getTopicName()));
            // Other approach to delete topic
            // would be to consume all messages from the topic
            try {
                // Wait for the topic to be deleted
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        logger.info("Creating topic {}", this.kafkaConfig.getTopicName());
        try {
            createTopic(this.kafkaConfig.getTopicName(), this.kafkaConfig.getPartitions(), this.kafkaConfig.getReplicationFactor());
        } catch (Exception e) {
            logger.error("Error creating topic", e);
        }
    }

    private boolean topicExists(String topicName) {
        try {
            Set<String> topics = adminClient.listTopics().names().get();
            return topics.contains(topicName);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error checking if topic exists", e);
        }
    }

    private void createTopic( String topicName, int partitions, short replicationFactor) {
        NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
        try {
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error creating topic: " + topicName, e);
        }
    }

    public void sendMessage(String topicName, String key, Person value) {
        logger.info("sendMessage");


        ProducerRecord<String, Person> record = new ProducerRecord<String, Person>(topicName,key, value);

        kafkaProducer.send(record, (recordMetadata, e) -> {
            // executes every time a record is successfully sent or an exception is thrown
            if (e == null) {
                // the record was successfully sent
                logger.info("Received new metadata. \n" +
                        "Topic:" + recordMetadata.topic() + "\n" +
                        "Partition: " + recordMetadata.partition() + "\n" +
                        "Offset: " + recordMetadata.offset() + "\n" +
                        "Timestamp: " + recordMetadata.timestamp());
            } else {
                logger.error("Error while producing", e);
            }
        });

        try {
            // Sleep to avoid kafka sticky partition assignment
            Thread.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.flush();
    }

    public Optional<List<Person>> consumeMessages(String topicName, int offset, int numMessages) {
        logger.info("Trying to consume messages from topic: {}", topicName);
        if (!topicExists( topicName)) {
            logger.error("Topic {} does not exist", topicName);
            return Optional.empty();
        }

        // Consume messages from the Kafka topic
        try {

            // Get all partitions
            List<TopicPartition> partitions = new ArrayList<>();
            for (int i = 0; i < this.kafkaConfig.getPartitions(); i++) {
                partitions.add(new TopicPartition(topicName, i));
            }

            kafkaConsumer.assign(partitions);
//            partitions.forEach(partition -> kafkaConsumer.seek(partition, offset));

            Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(partitions);
            for (TopicPartition partition : partitions) {
                logger.info("Seeking partition {} offset {}", partition, endOffsets.get(partition));
                if (offset >= endOffsets.get(partition)) {
                    logger.error("Offset {} is out of bounds for partition {}, max partition is {}", offset, partition.partition(), endOffsets.get(partition));
                    kafkaConsumer.seek(partition, endOffsets.get(partition));
                } else {
                    kafkaConsumer.seek(partition, offset);
                }
            }
            logger.info("Consuming messages from topic: %s" ,topicName);

            int collectedRecords = 0;
            List<Person> messages = new ArrayList<>();

            // Use this instead ?https://learn.conduktor.io/kafka/java-consumer-seek-and-assign/
            while (collectedRecords < numMessages) {
                ConsumerRecords<String, Person> records = kafkaConsumer.poll(Duration.ofMillis(100));
                if (records.isEmpty()) {
                    logger.error("Number of records requested {} is out of bounds", numMessages);
                    break;
                }
                for (ConsumerRecord<String, Person> record : records) {
                    logger.info("Received message: Key={}, Value={}, Partition={}, Offset={}",
                            record.key(), record.value(), record.partition(), record.offset());
                    messages.add(record.value());
                    collectedRecords++;
                    if (collectedRecords >= numMessages) {
                        break;
                    }
                }
            }

            return Optional.of(messages);
        } catch (Exception e) {
            e.printStackTrace();

        }


        return Optional.empty();
    }

}
