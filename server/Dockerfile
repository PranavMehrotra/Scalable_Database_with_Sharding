FROM python:latest


RUN apt-get update
RUN apt-get install -y python3
RUN apt-get install -y python3-pip

RUN pip install --upgrade pip
# Set the working directory
WORKDIR /server/

COPY requirements.txt /server/
RUN pip install -r requirements.txt

COPY . /server/
RUN chmod +x server.py

CMD ["python3", "server.py"]