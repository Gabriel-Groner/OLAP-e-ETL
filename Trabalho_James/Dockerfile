FROM apache/airflow:2.10.3

USER root


RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
         freetds-dev \
         libkrb5-dev \
         libssl-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install apache-airflow-providers-microsoft-mssql
