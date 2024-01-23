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

###3. Test the cluster with Spark and Delta Lake
// Write into HDFS
spark.range(10).repartition(1).write.mode("overwrite").csv("/test-spark")

// Read from HDFS into a DataFrame
val df = spark.read.format("csv").load("hdfs:///tmp/test-spark")

// Write DataFrame in delta format and save into HDFS
df.write.format("delta").mode("overwrite").save("/delta")

pyspark
# Spark session is already started: pyspark: sparkSession: spark, master=yarn, app id=application_1679739312421_0001

# Define DataFrame
data = spark.range(0, 5)
data.show()

# Write DataFrame in delta format and save into HDFS
data.write.format("delta").save("/delta/pysp2")

# Stop the Spark session
spark.stop()

## **Customizing the Cluster**

To adjust the number of nodes in the cluster, follow these steps:

### **1. Resize the cluster:**

```bash
cd endToend_lakehouse_deltalake
# Specify parameter > 1: 2, 3...
# This script rebuilds the Hadoop image with a different workers file, which specifies the name of all worker nodes
sudo ./resize-cluster.sh 5



  

