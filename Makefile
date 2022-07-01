include .env

default: build-all

build-all: build-all-clusters

build-all-clusters: build-airflow-cluster

build-airflow-cluster:
	@docker build \
		-f ./Dockerfile \
		-t airflow-base .

run-airflow: build-airflow-cluster
	@docker-compose up postgres redis airflow-webserver airflow-scheduler airflow-worker airflow-init flower
