# docker image build -t sccwrp/python:sdturfsync-psql .
#FROM debian:latest
FROM postgres:latest
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update 
RUN apt-get install -y apt-utils
RUN apt-get install -y python3-pip python3-dev build-essential libssl-dev libffi-dev
RUN apt-get install -y libpcre3 libpcre3-dev
RUN apt-get install -y vim
RUN apt-get install -y libpq-dev && pip3 install psycopg2
RUN pip3 install sqlalchemy
RUN pip3 install wheel ipython pandas xlsxwriter openpyxl requests
RUN pip3 install msal
RUN pip3 install Office365-REST-Python-Client
RUN pip3 install shareplum
RUN pip3 install asttokens executing
RUN pip3 install wget
RUN apt-get install -y libkrb5-dev
RUN apt-get install -y libjpeg62-turbo-dev zlib1g-dev
RUN pip3 install Pillow==6.2.2
RUN pip3 install arcgis
RUN pip3 install boto3

RUN mkdir -p /var/www/sync/
WORKDIR /var/www/sync/

