FROM python:3.10-slim
WORKDIR /harmonizer
RUN apt-get update
RUN apt-get install -y git
RUN apt-get install -y gcc
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
# docker buildx build --platform linux/amd64,linux/arm64 --push -t 1l41bgc7.c1.gra9.container-registry.ovh.net/infraestructures/harmonizer:prod .
