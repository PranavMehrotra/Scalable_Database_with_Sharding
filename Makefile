IMAGES := $(shell sudo docker images -a | awk 'NR>1 && ($$1 ~ /^server_img/ || $$1 ~ /^load_balancer_img/) {print $$3}')
CONTAINERS := $(shell sudo docker ps -a | awk 'NR>1 && ($$2 ~ /^server_img/ || $$2 ~ /^load_balancer_img/) {print $$1}')

deploy: clean
	sudo docker-compose up

install: build servers

build:
	sudo docker-compose build

servers:
	sudo docker build -t server_img ./server

clean:
	-sudo docker stop $(CONTAINERS)
	-sudo docker container prune -f

deepclean: clean
	-sudo docker rmi $(IMAGES)