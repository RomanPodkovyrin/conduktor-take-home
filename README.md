# Conduktor Technical Assignment

## Running the Application

### Start the Application

#### Build the Application
```bash
mvn clean package
```

#### Start the Application

from jar

```bash
java -jar target/conduktor-0.0.1-SNAPSHOT.jar
# or with loading data from file
java -jar target/conduktor-0.0.1-SNAPSHOT.jar load-data
```
 
Start directly from mvn
```bash
mvn spring-boot:run
```

### Test the Application

```bash
mvn test
```

## Assumptions

1. Rather than adding a separate app for data loading, I have added an argument to the application to load the data from the file. As it makes it easier to test it all together. And one less thing to run.
2. As offset is part of the api request url, I can't give it a default value. So I have added a different endpoint where offset can be left empty.
3. Postcode is misspelled in the data file. I have corrected it in the code.

## Future Improvements

Please look for the TODOs in the code for future improvements.

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

