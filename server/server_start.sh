#!/bin/bash

SQLFLOW_MYSQL_HOST=${SQLFLOW_MYSQL_HOST:-0.0.0.0}

echo "Start mysqld ..."
# sed -i "s/.*bind-address.*/bind-address = ${SQLFLOW_MYSQL_HOST}/" \
#     /etc/mysql/mariadb.conf.d/50-server.cnf

    # /etc/mysql/mysql.conf.d/mysqld.cnf
service mariadb start
service mariadb status
echo "Sleep until MySQL server is ready ..."
# sleep 3
cntr=0
until mysql -u root -e "SHOW DATABASES;"; do
    sleep 1
    read -r -p "Can't connect, retrying..."
    echo "Retrying..."
    cntr=$((cntr+1))
    if [ $cntr -gt 5 ]; then
        echo "Failed to start MySQL server."
        exit 1
    fi
done

# mysql -u root


# Grant all privileges to all the remote hosts so that the sqlflow
# server can be scaled to more than one replicas.
#
# NOTE: should notice this authorization on the production
# environment, it's not safe.
# echo "MySQL server is ready, grant all privileges to root user ..."
# python 'print("MySQL server is ready, grant all privileges to root user ...", flush=True)'



# mysql -uroot -proot \
#       -e "GRANT ALL PRIVILEGES ON *.* TO 'root'@'' IDENTIFIED BY 'root' WITH GRANT OPTION;"

# sleep infinity
# echo "MySQL server is ready."
# python 'print("Running test1.py", flush=True)'
python test1.py