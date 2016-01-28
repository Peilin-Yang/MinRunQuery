# Pull base image
FROM yangpeilyn/debian:latest

MAINTAINER Peilin Yang yangpeilyn@gmail.com

ADD . /home/MinRunQuery/
WORKDIR /home/MinRunQuery/
RUN ./configure && make

