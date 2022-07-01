FROM apache/airflow:2.1.2-python3.8 AS base

ARG ROOT_PATH=/opt/airflow

# Install user requirements
COPY requirements.txt /tmp/requirements.txt

RUN pip install --upgrade pip && \
    pip install -r /tmp/requirements.txt

WORKDIR ${ROOT_PATH}

ENV PYTHONPATH=${PYTHONPATH}:${ROOT_PATH}/dags:${ROOT_PATH}/plugins
