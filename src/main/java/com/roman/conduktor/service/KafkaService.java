package com.roman.conduktor.service;


import com.roman.conduktor.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


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
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.flush();
        kafkaProducer.close();
        // Send the message to the Kafka topic
    }

}
