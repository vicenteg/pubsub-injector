FROM ubuntu:16.04
MAINTAINER Vince Gonzalez <vincegonzalez@google.com>
RUN apt-get -y update && apt-get -y install python python-pip python-virtualenv python-dateutil
RUN apt-get -y install git
RUN pip install google-cloud-pubsub google-cloud-storage tabulator
RUN git clone https://github.com/vicenteg/pubsub-injector.git
WORKDIR pubsub-injector
COPY injector.py bin/injector.py
RUN git submodule update --init
WORKDIR tableschema-py
COPY tableschema-py tableschema-py
RUN pip install .
WORKDIR /
