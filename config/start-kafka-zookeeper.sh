#!/bin/bash

echo -e "\n"
# Start the ZooKeeper service
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &

echo -e "\n"
# Start the Kafka broker service
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &

echo -e "\n"