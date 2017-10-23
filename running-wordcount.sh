#!/usr/bin/env bash
MAIN_CLASS=$1
MAIN_JAR=target/wordcount-0.0.1-SNAPSHOT.jar
JARS=$(echo target/dependencies/*.jar | tr ' ' ',')
spark-submit --master yarn --deploy-mode client --jars $JARS \
	--num-executors 1 --executor-memory 1g --executor-cores 1 \
--class $MAIN_CLASS $MAIN_JAR $2 $3 $4
