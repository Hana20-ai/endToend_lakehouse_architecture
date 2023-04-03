#!/bin/bash

# Add the JAR files to HDFS
hdfs dfs -mkdir -p /user/spark/share/lib
hdfs dfs -put  /usr/local/spark/jars/*.jar  /user/spark/share/lib

echo "jars have been added to hdfs..."
# Set the HDFS permissions
hdfs dfs -chmod u+r /user/spark/share/lib
#hdfs dfs -chmod -R 755 /user/spark/share/lib
#hdfs dfs -chown -R hdfs:hadoop /user/spark/share/lib