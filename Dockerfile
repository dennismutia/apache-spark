FROM mcr.microsoft.com/devcontainers/python:1-3.12-bullseye
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-11-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

ADD . /

WORKDIR /

RUN pip install --upgrade pip

RUN pip install -r requirements.txt
# RUN mkdir /app

EXPOSE 5000