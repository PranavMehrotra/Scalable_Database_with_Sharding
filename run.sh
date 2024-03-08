#!/bin/bash

docker container prune -f
docker rmi server_img:latest
docker build -t server_img ./server
docker run -p 5000:5000 -e SERVER_ID=123654 --name server server_img 