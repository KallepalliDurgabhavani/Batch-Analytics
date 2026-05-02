FROM apache/airflow:2.8.0

USER root

RUN apt-get update && \
    apt-get install -y curl && \
    apt-get clean

USER airflow