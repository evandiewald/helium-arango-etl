FROM python:3.9.8-slim-buster

RUN apt-get -y update && apt-get -y install git python3-dev postgresql postgresql-contrib openssl libpq-dev build-essential

COPY requirements.txt requirements.txt

RUN mkdir logs

RUN pip3 install -r requirements.txt # faster debugging if we cache dependencies

COPY . .

ENTRYPOINT python3 src/etl.py