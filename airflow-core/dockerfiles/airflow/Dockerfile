FROM apache/airflow:latest-python3.10
COPY requirements.txt /home

RUN pip3 install --upgrade pip
RUN pip3 install -r /home/requirements.txt

USER root

WORKDIR /opt/
# install wget
RUN apt-get update && apt-get install -y wget
# install unzip
RUN apt-get update && apt-get install -y unzip

# Pentaho Data Integration https://sourceforge.net/projects/pentaho/files/latest/download
#RUN wget https://sonik.dl.sourceforge.net/project/pentaho/Pentaho-9.3/client-tools/pdi-ce-9.3.0.0-428.zip -O pdi.zip
# extract the zip file to the folder /opt/data-integration
#RUN unzip pdi.zip -d /opt/

#tika
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 467B942D3A79BD29 && apt update -y && apt-get install -y software-properties-common && \
    apt-add-repository 'deb http://archive.debian.org/debian stretch main' && apt update -y && \
    apt-get install -y openjdk-8-jdk-headless &&\
    export JAVA_HOME &&\
    apt-get clean

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
ENV PATH $JAVA_HOME/bin:$PATH

#oracle
WORKDIR    /opt/oracle
RUN        apt-get update && apt-get install -y libaio1 wget unzip \
            && wget https://download.oracle.com/otn_software/linux/instantclient/instantclient-basiclite-linuxx64.zip \
            && unzip instantclient-basiclite-linuxx64.zip \
            && rm -f instantclient-basiclite-linuxx64.zip \
            && cd /opt/oracle/instantclient* \
            && rm -f *jdbc* *occi* *mysql* *README *jar uidrvci genezi adrci \
            && echo /opt/oracle/instantclient* > /etc/ld.so.conf.d/oracle-instantclient.conf \
            && ldconfig

WORKDIR /opt/airflow
COPY ./jars/*.jar /home/airflow/.local/lib/python3.9/site-packages/pyspark/jars/

