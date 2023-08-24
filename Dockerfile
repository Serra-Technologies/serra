FROM apache/spark-py
USER root
ARG PIP=pip3

WORKDIR "/home"

RUN apt-get -y update
RUN apt-get -y install git
RUN apt-get -y install python3.10-venv

ENV VIRTUAL_ENV=/opt/venv
RUN python3.10 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Setup location for google bigquery service account 
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/workspace/bigquery.json

# Install serra
RUN ${PIP} install --upgrade pip
RUN git clone https://github.com/Serra-Technologies/serra.git
RUN ${PIP} install ./serra

# Some helpful things for development
RUN apt-get -y install vim
RUN apt-get -y install screen


WORKDIR "/app"