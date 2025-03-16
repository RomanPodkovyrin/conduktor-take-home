package com.roman.conduktor.config;

import com.roman.conduktor.model.deserializer.PersonDeserializer;
import com.roman.conduktor.model.serializer.PersonSerializer;
import lombok.Getter;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Getter
@Configuration
public class KafkaConfig {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KafkaConfig.class);

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${kafka.topic.name}")
    private String topicName;

    @Value("${kafka.topic.partitions}")
    private int partitions;

    @Value("${kafka.topic.replication.factor}")
    private short replicationFactor;

// TODO: move config creation into constructor ? otherwise they all get called on startup

    @Bean
    public AdminClient adminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(props);
    }

    @Bean
    public AdminClient setupTopic() {

        if (topicExists(this.adminClient(), topicName)){
            logger.info("Topic {} already exists", topicName);
            logger.info("Deleting topic {}", topicName);
            this.adminClient().deleteTopics(Collections.singleton(topicName));
            // Other approach to delete topic
            // would be to consume all messages from the topic
            try {
                // Wait for the topic to be deleted
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        logger.info("Creating topic {}", topicName);
        createTopic(this.adminClient(), topicName, partitions, replicationFactor);
        return this.adminClient();
    }


    @Bean
    public Properties producerConfigs() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PersonSerializer.class.getName());
        return props;
    }

    @Bean
    public Properties consumerConfigs() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, PersonDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "people-consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Disable auto-commit
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }


    private static boolean topicExists(AdminClient adminClient, String topicName) {
        try {
            Set<String> topics = adminClient.listTopics().names().get();
            return topics.contains(topicName);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error checking if topic exists", e);
        }
    }

    private static void createTopic(AdminClient adminClient, String topicName, int partitions, short replicationFactor) {
        NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
        try {
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error creating topic: " + topicName, e);
        }
    }
}