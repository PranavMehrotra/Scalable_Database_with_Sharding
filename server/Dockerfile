FROM python:latest



RUN apt-get update
RUN apt-get install -y python3
RUN apt-get install -y python3-pip
RUN pip install --upgrade pip

# Set the working directory
WORKDIR /server/
COPY ./requirements.txt /server/
RUN pip install -r requirements.txt

# ARG server_id=${SERVER_ID}
# RUN echo "$server_id"
# ARG server_password=root@123

# add group and user for mysql
# RUN groupadd -r $server_id && useradd -r -g $server_id $server_id


RUN echo "mysql-server mysql-server/root_password password root@123" | \
    debconf-set-selections
RUN echo "mysql-server mysql-server/root_password_again password root@123" | \
    debconf-set-selections

# Download .deb file for mysql
# RUN apt-get install -y wget

# RUN wget https://dev.mysql.com/get/mysql-apt-config_0.8.29-1_all.deb

# RUN dpkg -i mysql-apt-config_0.8.29-1_all.deb

RUN apt-get install -y default-mysql-server
    # && mkdir -p /var/lib/mysql /var/run/mysqld \
    # && chown -R mysql:mysql /var/lib/mysql /var/run/mysqld \
    # && chmod 777 /var/run/mysqld

# RUN chmod +x server.py
# RUN chmod +x test1.py

ARG SQLFLOW_MYSQL_PORT="3306"
ENV SQLFLOW_MYSQL_PORT=$SQLFLOW_MYSQL_PORT
EXPOSE $SQLFLOW_MYSQL_PORT

# RUN sed -i 's/bind-address/#bind-address/' /etc/mysql/mysql.conf.d/mysqld.cnf
# RUN sed -i 's/bind-address/#bind-address/' /etc/mysql/mariadb.conf.d/50-server.cnf
# RUN service mysql start
# EXPOSE 3306 
# RUN service mariadb start
COPY . /server/
CMD ["bash","./server_start.sh"]