# syntax=docker/dockerfile:1
FROM python:3.9.14-slim-buster
RUN apt-get update && apt-get install -yq --no-install-recommends \
  default-jdk gcc build-essential python-dev python3-dev libevent-dev \
  && apt-get clean && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app
ENV DAGSTER_HOME=/opt/dagster/dagster_home/
#COPY ./dagster.yaml /opt/dagster/dagster_home/
COPY --chmod=777 ./dagster_home /opt/dagster/dagster_home
WORKDIR /opt/dagster/app
COPY --chmod=777 ./app .
RUN pip install --upgrade pip
RUN pip install -U pip setuptools wheel
RUN pip install -r requirements.txt
#RUN pip install dagster dagit
EXPOSE 3000
VOLUME ["/opt/dagster/app"]
ENTRYPOINT ["dagit", "-h", "0.0.0.0", "-p", "3000"]