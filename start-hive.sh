#!/bin/bash


# the default node number is 3
N=${1:-3}
# Bring the services up
function startServices {
    docker start hadoop-master hadoop-slave1 hadoop-slave2
    sleep 5
    echo ">> Starting hdfs ..."
    docker exec -it hadoop-master start-dfs.sh
    sleep 5
    echo ">> Starting yarn ..."
    docker exec -d hadoop-master start-yarn.sh
    sleep 5
  
    echo ">> Preparing hdfs for hive ..."
    docker exec -it hadoop-master hdfs dfs -mkdir -p /tmp
    docker exec -it hadoop-master hdfs dfs -mkdir -p /user/hive/warehouse
    docker exec -it hadoop-master hdfs dfs -chmod g+w /tmp
    docker exec -it hadoop-master hdfs dfs -chmod g+w /user/hive/warehouse
    sleep 5
    echo ">> Starting Hive Metastore ..."
    docker exec -d hadoop-master hive --service metastore
    docker exec -d hadoop-master hive --service hiveserver2
    
  # get into hadoop master container
    sudo docker exec -it hadoop-master bash
}



if [[ $1 = "install" ]]; then
  sudo docker network create --driver=bridge myservice

  # Starting Postresql Hive metastore
    echo ">> Starting postgresql hive metastore ..."
    docker run -d --net myservice --hostname psqlhms --name psqlhms -e POSTGRES_PASSWORD=hive -it postgres_hms
    sleep 5
  
    # 3 nodes
    echo ">> Starting master and worker nodes ..."
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
                hive &> /dev/null
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

 
  startServices
  exit
fi




