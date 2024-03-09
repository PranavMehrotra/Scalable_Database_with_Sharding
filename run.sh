#!/bin/bash

docker stop server
docker container prune -f
docker rmi server_img:latest
docker build -t server_img ./server
docker run -p 5000:5000 -e SERVER_ID=235647 --name server server_img 

# docker stop db_server
# docker container prune -f
# docker rmi db_server_img:latest
# docker build -t db_server_img ./db_server
# docker run -p 5000:5000 -e SERVER_ID=db_server --name db_server db_server_img 