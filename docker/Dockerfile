# sccwrp/python:sdturfsync
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

# Try to install postgres for the sake of using the psql copy command
# we are most likely going to be moving large amounts of data
#RUN apt-get update
RUN apt-get install wget lsb-core
RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN apt-get -y install postgresql


RUN mkdir -p /var/www/sync/
WORKDIR /var/www/sync/

