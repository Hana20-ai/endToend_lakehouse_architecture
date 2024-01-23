# End-to-End Lakehouse with Delta Lake

## Overview

This repository provides a Dockerized Hadoop cluster with Spark and Delta Lake, enabling seamless setup and testing for Big Data projects.

## Quick Start

Follow these steps to get the cluster up and running:

### 1. Build the Docker Compose file

```bash
cd endToend_lakehouse_deltalake
sudo docker compose up 
### 2. Start the Hadoop cluster
sudo ./start-container.sh 
### 3. Test the cluster with Spark and Delta Lake
#### a. Using Scala:
Write into HDFS:
```scala
spark.range(10).repartition(1).write.mode("overwrite").csv("/test-spark")
Read from HDFS into a DataFrame
val df = spark.read.format("csv").load("hdfs:///tmp/test-spark")
Write DataFrame in delta format and save into HDFS
df.write.format("delta").mode("overwrite").save("/delta")



