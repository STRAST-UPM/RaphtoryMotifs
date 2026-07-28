#!/bin/bash

### Create Kafka topic
docker exec kafka-local /opt/kafka/bin/kafka-topics.sh   --bootstrap-server localhost:9092   --command-config /etc/kafka/secrets/client-sasl.properties   --create --topic VICOM_CORRELAX   --partitions 1 --replication-factor 1


## List kafka topic in the broker

docker exec kafka-local /opt/kafka/bin/kafka-topics.sh   --bootstrap-server localhost:9092   --command-config /etc/kafka/secrets/client-sasl.properties   --list


## Listen to the event in the Kafka broker in a specific broker 

docker exec -it kafka-local /opt/kafka/bin/kafka-console-consumer.sh   --bootstrap-server localhost:9092   --consumer.config /etc/kafka/secrets/client-sasl.properties   --topic VICOM_CORRELAX --from-beginning
