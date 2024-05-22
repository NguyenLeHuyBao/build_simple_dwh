FROM bitnami/python:3.8.15
RUN mkdir /airflow
WORKDIR /airflow
RUN pip install apache-airflow==2.4.3