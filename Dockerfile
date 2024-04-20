FROM ubuntu:latest

RUN apt-get update && apt-get install -y python3-pip openjdk-8-jdk
WORKDIR /app
VOLUME /app/data
COPY . /app

RUN pip install -r dep/requirements.txt
ENV PYTHONPATH="$PYTHONPATH:/app:/app/build:/app/build/scripts:/app/src"
ENV JAVA_HOME="/usr/lib/jvm/java-8-openjdk-arm64"