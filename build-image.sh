#!/bin/bash

echo ""

echo -e "\nbuild docker hadoop-spark-delta image\n"
#sudo docker build -t kiwenlau/hadoop:1.0 .
sudo docker-compose up --build

echo ""