#!/bin/sh

########################################################################
# title:          Build Complete Project
# author:         Alexsandro souza (https://apssouza.com.br)
# url:            https://github.com/apssouza22
# description:    Build complete Big data pipeline
# usage:          sh ./project-orchestrate.sh
########################################################################

#set -ex


docker exec cassandra-iot cqlsh --username cassandra --password cassandra  -f /schema.cql
docker exec kafka-iot kafka-topics --create --topic iot-data-event --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
docker exec spark-master apk add --no-cache libc6-compat
docker exec spark-worker-1 apk add --no-cache libc6-compat
docker exec namenode hdfs dfs -mkdir /lambda-arch
docker exec namenode hdfs dfs -mkdir /lambda-arch/checkpoint
docker exec namenode hdfs dfs -chmod -R 777 /lambda-arch
docker exec namenode hdfs dfs -chmod -R 777 /lambda-arch/checkpoint



