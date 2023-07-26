#!/bin/bash

echo ""

echo -e "\nbuild the platform image\n"
#sudo docker build -t kiwenlau/hadoop:1.0 .
sudo docker-compose up --build

echo ""