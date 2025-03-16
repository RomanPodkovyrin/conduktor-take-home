package com.roman.conduktor.service;


import com.roman.conduktor.config.KafkaConfig;
import com.roman.conduktor.model.Person;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


@Service
public class KafkaService {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KafkaService.class);
    private final KafkaConfig kafkaConfig;

    @Autowired
    public KafkaService(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        this.kafkaConfig.setupTopic();
    }

    public void sendMessage(String topicName, String key, Object value) {
        logger.info("sendMessage");

        KafkaProducer kafkaProducer = new KafkaProducer(this.kafkaConfig.producerConfigs());

        ProducerRecord<String, Object> record = new ProducerRecord<>(topicName,key, value);

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
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.flush();
        kafkaProducer.close();
        // Send the message to the Kafka topic
    }

    public List<Person> consumeMessages(String topicName, int offset, int numMessages) {
        // TODO: test topic, exists
        // Consume messages from the Kafka topic
        try (KafkaConsumer kafkaConsumer = new KafkaConsumer(this.kafkaConfig.consumerConfigs())) {
            // TODO: make it more programmatic
            List<TopicPartition> partitions = Arrays.asList(
                    new TopicPartition(topicName, 0),
                    new TopicPartition(topicName, 1),
                    new TopicPartition(topicName, 2)
            );

            // TODO: keep offset for each partition???
            kafkaConsumer.assign(partitions);
            partitions.forEach(partition -> kafkaConsumer.seek(partition, offset));
            logger.info("Consuming messages from topic: %s" ,topicName);

            Integer collectedRecords = 0;
            List<Person> messages = new ArrayList<>();

            // Use this instead ?https://learn.conduktor.io/kafka/java-consumer-seek-and-assign/
            while (collectedRecords < numMessages) {
                ConsumerRecords<String, Person> records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, Person> record : records) {
                    System.out.printf("Received message: Key=%s, Value=%s, Partition=%d, Offset=%d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                    messages.add(record.value());
                    collectedRecords++;
                    if (collectedRecords >= numMessages) {
                        break;
                    }
                }
            }

            return messages;
        } catch (Exception e) {
            e.printStackTrace();

        }


        return null;
    }

}
