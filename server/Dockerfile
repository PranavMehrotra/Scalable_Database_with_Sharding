FROM python:latest

COPY . /server/

# Set the working directory
WORKDIR /server/

RUN apt-get update
RUN apt-get install -y python3
RUN apt-get install -y python3-pip


RUN pip install -r requirements.txt
RUN chmod +x server.py

CMD ["python", "server.py"]