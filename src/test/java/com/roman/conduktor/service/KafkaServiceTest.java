package com.roman.conduktor.service;

import com.roman.conduktor.config.KafkaConfig;
import com.roman.conduktor.model.Person;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaServiceTest {

    @Mock
    private KafkaConfig kafkaConfig;

    @Mock
    private AdminClient adminClient;

    @Mock
    private KafkaProducer<String, Person> kafkaProducer;

    @Mock
    private KafkaConsumer<String, Person> kafkaConsumer;

    @Mock
    private ListTopicsResult listTopicsResult;

    @Mock
    private CreateTopicsResult createTopicsResult;

    @Mock
    private KafkaFuture<Set<String>> topicsFuture;

    @Mock
    private KafkaFuture<Void> createTopicsFuture;

    private KafkaService kafkaService;

    private static final String TOPIC_NAME = "test-topic";
    private static final int PARTITIONS = 3;
    private static final short REPLICATION_FACTOR = 1;

    @BeforeEach
    void setUp() {


        // Use ReflectionTestUtils to set private fields
        kafkaService = new KafkaService(kafkaConfig, adminClient, kafkaProducer, kafkaConsumer);
    }

    @Test
    void testSetupTopicWhenTopicDoesNotExist() throws Exception {

        // Setup configuration
        when(kafkaConfig.getTopicName()).thenReturn(TOPIC_NAME);
        when(kafkaConfig.getPartitions()).thenReturn(PARTITIONS);
        when(kafkaConfig.getReplicationFactor()).thenReturn(REPLICATION_FACTOR);

        // Set up mock for topicExists
        when(adminClient.listTopics()).thenReturn(listTopicsResult);
        when(listTopicsResult.names()).thenReturn(topicsFuture);
        when(topicsFuture.get()).thenReturn(Collections.emptySet());

        when(adminClient.createTopics(any())).thenReturn(createTopicsResult);

        when(createTopicsResult.all()).thenReturn(createTopicsFuture);

        // Execute
        kafkaService.setupTopic();

        // Verify
        verify(adminClient).listTopics();
        verify(adminClient).createTopics(argThat(topics ->
                topics.iterator().next().name().equals(TOPIC_NAME) &&
                        topics.iterator().next().numPartitions() == PARTITIONS &&
                        topics.iterator().next().replicationFactor() == REPLICATION_FACTOR));
    }

    @Test
    void testSetupTopicWhenTopicExists() throws Exception {

        // Setup configuration
        when(kafkaConfig.getTopicName()).thenReturn(TOPIC_NAME);
        when(kafkaConfig.getPartitions()).thenReturn(PARTITIONS);
        when(kafkaConfig.getReplicationFactor()).thenReturn(REPLICATION_FACTOR);

        // Set up mock for topicExists
        when(adminClient.listTopics()).thenReturn(listTopicsResult);
        when(listTopicsResult.names()).thenReturn(topicsFuture);
        when(topicsFuture.get()).thenReturn(Collections.singleton(TOPIC_NAME));


        // Set up mock for createTopic
        when(adminClient.createTopics(any())).thenReturn(createTopicsResult);
        when(createTopicsResult.all()).thenReturn(createTopicsFuture);

        // Execute
        kafkaService.setupTopic();

        // Verify
        verify(adminClient).listTopics();
        verify(adminClient).deleteTopics(Collections.singleton(TOPIC_NAME));
        verify(adminClient).createTopics(argThat(topics ->
                topics.iterator().next().name().equals(TOPIC_NAME) &&
                        topics.iterator().next().numPartitions() == PARTITIONS &&
                        topics.iterator().next().replicationFactor() == REPLICATION_FACTOR
        ));


    }

    @Test
    void testSendMessage() {
        // Prepare test data
        String key = "test-key";
        Person person = new Person("John Doe", "30");

        // Set up mock for KafkaProducer
        ArgumentCaptor<ProducerRecord<String, Person>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);

        // Execute
        kafkaService.sendMessage(TOPIC_NAME, key, person);

        // Verify
        verify(kafkaProducer).send(recordCaptor.capture(), any());
        verify(kafkaProducer).flush();

        ProducerRecord<String, Person> capturedRecord = recordCaptor.getValue();
        assertEquals(TOPIC_NAME, capturedRecord.topic());
        assertEquals(key, capturedRecord.key());
        assertEquals(person, capturedRecord.value());
    }

    @Test
    void testConsumeMessagesWhenTopicDoesNotExist() throws Exception {
        // Set up mock for topicExists
        when(adminClient.listTopics()).thenReturn(listTopicsResult);
        when(listTopicsResult.names()).thenReturn(topicsFuture);
        when(topicsFuture.get()).thenReturn(Collections.emptySet());

        // Execute
        Optional<List<Person>> result = kafkaService.consumeMessages(TOPIC_NAME, 0, 10);

        // Verify
        verify(adminClient).listTopics();
        verify(kafkaConsumer, never()).assign(any());
        assertTrue(result.isEmpty());
    }


    @Test
    void testClose() {
        // Execute
        kafkaService.close();

        // Verify
        verify(adminClient).close();
        verify(kafkaProducer).close();
        verify(kafkaConsumer).close();
    }
}