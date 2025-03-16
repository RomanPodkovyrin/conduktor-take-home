# Conduktor Technical Assignment

## Setup

### Start Kafka

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:3.7.0  
```

### Kafka Commands
### List Topics
```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Create a Topic
```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic people-topic --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092 --config cleanup.policy=delete
```

### Delete a Topic
```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --delete --topic people-topic --bootstrap-server localhost:9092
```

### Describe Topic (Get Details/Statistics)
```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --describe --topic people-topic --bootstrap-server localhost:9092
```

### Check Number of Messages in a Topic
```bash
docker exec -it kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic people-topic --partitions 0,1,2
```

### Publish Messages to a Topic
```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --topic people-topic --bootstrap-server localhost:9092
```

### Consume Messages from a Topic
```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic people-topic --from-beginning --bootstrap-server localhost:9092
```

