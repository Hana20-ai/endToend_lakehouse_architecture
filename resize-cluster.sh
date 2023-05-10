#!/bin/bash

# N is the node number of hadoop cluster
N=$1

if [ $# = 0 ]
then
	echo "Please specify the number of nodes in the hadoop cluster!"
	exit 1
fi

# change slaves file
i=1
rm config/workers
while [ $i -lt $N ]
do
	echo "hadoop-slave$i" >> config/workers
	((i++))
done 

echo ""

echo -e "\nbuild docker hadoop-spark-delta image\n"

# rebuild the image
#sudo docker build -t kiwenlau/hadoop:1.0 .
sudo docker-compose up --build

echo ""