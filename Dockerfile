#Download base image ubuntu 16.04
FROM ubuntu:16.04
FROM python:3
#FROM docker.elastic.co/elasticsearch/elasticsearch:7.9.1@sha256:0a5308431aee029636858a6efe07e409fa699b02549a78d7904eb931b8c46920

WORKDIR /app

RUN apt-get update 

#RUN sudo docker run -d -p 9200:9200 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch


COPY ./requirements.txt .


#RUN pip install -r requirements.txt

RUN git clone https://github.com/gariciodaro/arXiv-haystack-app.git