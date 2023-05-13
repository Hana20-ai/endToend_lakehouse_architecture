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



# start hadoop workers
i=1
while [ $i -lt $N ]
do
#delete slave if it already exists 
	sudo docker rm -f hadoop-slave$i &> /dev/null
	echo "start hadoop-worker$i container..."
#create new containers 
	sudo docker run -itd \
	                --net=myservice \
	                --name hadoop-slave$i \
	                --hostname hadoop-slave$i \
	                hadoop-spark-delta:latest &> /dev/null
    #docker network connect endtoend_lakehouse_deltalake_nifi-network hadoop-slave$i
	i=$(( $i + 1 ))
done 

# get into hadoop master container
sudo docker exec -it hadoop-master bash