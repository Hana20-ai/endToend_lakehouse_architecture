#!/bin/bash
sudo docker network create --driver=bridge myservice

# the default node number is 3
N=${1:-3}


# start hadoop master container
sudo docker rm -f hadoop-master &> /dev/null
echo "start hadoop-master container..."
sudo docker run -itd \
                --net=myservice \
                -p 9870:9870 \
                -p 8088:8088 \
                -p 7077:7077 \
                -p 16010:16010 \
                --name hadoop-master \
                --hostname hadoop-master \
                hadoop-spark-delta:latest &> /dev/null

sudo docker network connect endtoend_lakehouse_deltalake_nifi-network  hadoop-master



# start hadoop slave container
i=1
while [ $i -lt $N ]
do
#delete slave if it already exists 
	sudo docker rm -f hadoop-slave$i &> /dev/null
	echo "start hadoop-slave$i container..."
#create new containers 
	sudo docker run -itd \
	                --net=myservice \
	                --name hadoop-slave$i \
	                --hostname hadoop-slave$i \
	                hadoop-spark-delta:latest &> /dev/null
  #sudo docker network connect endtoend_lakehouse_deltalake_nifi-network  hadoop-slave$i
	i=$(( $i + 1 ))
done 
 
 #add HMS to the network 
 #docker network connect myservice  hive-metastore
 
 #echo ">> Preparing hdfs for hive ..."
  #sudo docker exec -u hadoop -it hadoop-master hdfs dfs -mkdir -p /tmp
  #sudo docker exec -u hadoop -it hadoop-master hdfs dfs -mkdir -p /user/hive/warehouse
  #sudo docker exec -u hadoop -it hadoop-master hdfs dfs -chmod g+w /tmp
  #sudo docker exec -u hadoop -it hadoop-master hdfs dfs -chmod g+w /user/hive/warehouse
  #sleep 5
  #echo ">> Starting Hive Metastore ..."
  #sudo docker exec -u hadoop -d hadoop-master hive --service metastore
  #sudo docker exec -u hadoop -d hadoop-master hive --service hiveserver2

  # Starting Postresql Hive metastore
  #echo ">> Starting postgresql hive metastore ..."
  #docker run -d --net myservice  --hostname psqlhms --name metastor -e POSTGRES_PASSWORD=hive -it postgresql-hms
  #sleep 5

  sudo docker exec -it hadoop-master bash