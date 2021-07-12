#!/bin/sh

########################################################################
# title:          Build Complete Project
# author:         Alexsandro souza (https://apssouza.com.br)
# url:            https://github.com/apssouza22
# description:    Build complete Big data pipeline
# usage:          sh ./project-orchestrate.sh
########################################################################

set -ex

# Create casandra schema
docker exec cassandra-iot cqlsh --username cassandra --password cassandra  -f /schema.cql

# Create Kafka topic "iot-data-event"
docker exec kafka-iot kafka-topics --create --topic iot-data-event --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181

# Create our folders on Hadoop file system and total permission to those
docker exec namenode hdfs dfs -rm /lambda-arch
docker exec namenode hdfs dfs -mkdir /lambda-arch
docker exec namenode hdfs dfs -mkdir /lambda-arch/checkpoint
docker exec namenode hdfs dfs -chmod -R 777 /lambda-arch
docker exec namenode hdfs dfs -chmod -R 777 /lambda-arch/checkpoint

# Install libc6-compat lib in both sparks containers
#docker exec spark-master apk add --no-cache libc6-compat
#docker exec spark-worker-1 apk add --no-cache libc6-compat

