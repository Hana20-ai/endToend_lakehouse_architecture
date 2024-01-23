# endToend_lakehouse_deltalake
A- N Nodes Hadoop Cluster with spark and delta lake (Default value of N is 3 : hadoop-master, hadoop-slave1, hadoop-slave2)
### 1- Build the docker compose.yml file 
Go to the directory where you put your cloned git repo
  cd endToend_lakehouse_deltalake
  sudo docker compose up 
### 2- start the hadoop cluster by executing:
   sudo ./start-container.sh 
   Output:
   start hadoop-master container...
   start hadoop-slave1 container...
   start hadoop-slave2 container...
   root@hadoop-master:~# 
   # Start DFS and Yarn using the script:
   root@hadoop-master:~# ./start-hadoop.sh
   OUTPUT:
  hadoop-master: Warning: Permanently added 'hadoop-master' (ED25519) to the list of known hosts.
  hadoop-master: WARNING: HADOOP_NAMENODE_OPTS has been replaced by HDFS_NAMENODE_OPTS. Using value of HADOOP_NAMENODE_OPTS.
  Starting datanodes
  WARNING: HADOOP_SECURE_DN_LOG_DIR has been replaced by HADOOP_SECURE_LOG_DIR. Using value of HADOOP_SECURE_DN_LOG_DIR.
  hadoop-slave2: Warning: Permanently added 'hadoop-slave2' (ED25519) to the list of known hosts.
  hadoop-slave1: Warning: Permanently added 'hadoop-slave1' (ED25519) to the list of known hosts.
  hadoop-slave2: WARNING: HADOOP_SECURE_DN_LOG_DIR has been replaced by HADOOP_SECURE_LOG_DIR. Using value of HADOOP_SECURE_DN_LOG_DIR.
  hadoop-slave2: WARNING: HADOOP_DATANODE_OPTS has been replaced by HDFS_DATANODE_OPTS. Using value of HADOOP_DATANODE_OPTS.
  hadoop-slave1: WARNING: HADOOP_SECURE_DN_LOG_DIR has been replaced by HADOOP_SECURE_LOG_DIR. Using value of HADOOP_SECURE_DN_LOG_DIR.
  hadoop-slave1: WARNING: HADOOP_DATANODE_OPTS has been replaced by HDFS_DATANODE_OPTS. Using value of HADOOP_DATANODE_OPTS.
  hadoop-slave3: ssh: Could not resolve hostname hadoop-slave3: Name or service not known
  Starting secondary namenodes [hadoop-master]
  hadoop-master: Warning: Permanently added 'hadoop-master' (ED25519) to the list of known hosts.
  hadoop-master: WARNING: HADOOP_SECONDARYNAMENODE_OPTS has been replaced by HDFS_SECONDARYNAMENODE_OPTS. Using value of HADOOP_SECONDARYNAMENODE_OPTS.


  Starting resourcemanager
  Starting nodemanagers
  hadoop-slave1: Warning: Permanently added 'hadoop-slave1' (ED25519) to the list of known hosts.
  hadoop-slave2: Warning: Permanently added 'hadoop-slave2' (ED25519) to the list of known hosts.
  hadoop-slave3: ssh: Could not resolve hostname hadoop-slave3: Name or service not known
  The network connecting the hadoop nodes is also created in this step: network myservice 
  
  # At this stage, you can access the UI of yarn and HDFS via browser 
  
### 3-Test the cluster with spark and delta lake 
Firs add the spark jars to HDFS using:
 ./add-spark-jars-to-hdfs.sh 
 
# test USING SCALA 
write into HDFS: 
spark.range(10).repartition(1).write.mode("overwrite").csv("/test-spark")  //in a folder sous the base dir /) 
# read from hdfs into a df: 
val df = spark.read.format("csv").load("hdfs:///tmp/test-spark")
write dataframe in delta format and write into hdfs 
df.write.format("delta").mode("overwrite").save("/delta")
# test USING PYSPARK:
pyspark
# The sparksession is already started : pyspark: sparkSession: spark , master= yarn, app id = application_1679739312421_0001
# define df
data = spark.range(0, 5)
data.show()
data.write.format("delta").save("/delta/pysp2")
# stop the spark session
spark.stop() 

# B-To define the number of nodes running in the cluster: example N=5 
cd endToend_lakehouse_deltalake
# specify parameter > 1: 2, 3..
# This script just rebuild hadoop image with different workers file, which specifies the name of all worker nodes
### 1- sudo ./resize-cluster.sh 5
# Start containers using same parameter in step 1
### 2- sudo ./start-container.sh 5
# Start Dfs and Yarn using the same script: start-hadoop.sh 
root@hadoop-master:~# ./start-hadoop.sh 
# You can Now test with the new cluster with 5 nodes (step 3 Section A)




  

