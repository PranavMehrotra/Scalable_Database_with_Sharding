#!/bin/bash
server_id=$(SERVER_ID)
mysql_img_path="mysql/mysql-server:8.0"
pwd=$server_id@123
docker pull $mysql_img_path

docker run --name $server_id -p 3306:3306 -e MYSQL_ROOT_PASSWORD=$pwd -d $mysql_img_path

# docker run --name $ -p 3307:3306 -e MY_SQL_ROOT_PASSWORD=root -d mysql/mysql-server:8.0