FROM ubuntu:16.04
MAINTAINER Vince Gonzalez <vincegonzalez@google.com>
RUN apt-get -y update && apt-get -y install python python-pip python-virtualenv
RUN apt-get -y install python-dateutil
RUN pip install google-cloud-pubsub google-cloud-storage tabulator
COPY injector.py bin/injector.py
COPY tableschema-py tableschema-py
WORKDIR tableschema-py
RUN pip install .
WORKDIR /
