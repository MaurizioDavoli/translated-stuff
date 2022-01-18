# airflow image version is set here and after the build is passed to docker-compose to run it
FROM apache/airflow

USER root
RUN apt-get update && apt-get install -y git

ARG GIT_TOKEN
ENV GIT_TOKEN=$GIT_TOKEN


USER $AIRFLOW_UID

COPY requirements.txt .
COPY dags/loader ./dags/loader

RUN pip3 install -r requirements.txt
