#!/bin/bash
#sudo docker network create --driver=bridge myservice


# the default node number is 3
N=${1:-3}


# start hadoop master container
sudo docker rm -f hadoop-master &> /dev/null
echo "start hadoop-master container..."
sudo docker run -itd \
                --net=myservice \
                -p 16011:16010 \
                -p 9871:9870 \
                -p 8089:8088 \
                -p 7078:7077 \
                --name hadoop-master \
                --hostname hadoop-master \
                hadoop-spark-delta:latest &> /dev/null

#-v hadoop_master_data:/root/hdfs/datanode \
#-v hadoop_master_files:/root/hdfs/namenode \



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
	i=$(( $i + 1 ))
done 

# get into hadoop master container
sudo docker exec -it hadoop-master bash
