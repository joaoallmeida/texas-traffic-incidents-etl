#!/bin/bash

echo "Copy requirements.txt to folder docker/"
cp requirements.txt docker/

cd docker/

echo "Setting environment variables"
export HOME_DIR=`pwd`
export AIRFLOW_UID=0
export DOWNLOAD_URL="https://github.com/enqueue/metabase-clickhouse-driver/releases/download/1.2.1/clickhouse.metabase-driver.jar"

echo "Downloading the driver from $DOWNLOAD_URL"=
curl -L -o clickhouse.metabase-driver.jar $DOWNLOAD_URL

echo "Building airflow image ..."
docker build -f Dockerfile.airflow --tag my-airflow:v0.0.1 .

echo "Building metabase image..."
docker build -f Dockerfile.metabase --tag my-metabase:v0.0.1 .

rm clickhouse.metabase-driver.jar
rm requirements.txt