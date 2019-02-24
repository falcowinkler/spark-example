#!/usr/bin/env bash
#set -euo pipefail
#./gradlew clean shadowJar
#docker build -t falcowinkler/spark-example:latest .
#docker push falcowinkler/spark-example:latest
# requires kubectl proxy to run
# --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.0,org.apache.spark:spark-avro_2.12:2.4.0 \
spark-submit --class de.haw.tweetspace.Recommender \
             --master k8s://http://127.0.0.1:8001  \
             --deploy-mode cluster             \
             --conf spark.kubernetes.container.image=falcowinkler/spark-example:master\
             --executor-memory 2G              \
             --num-executors 3  \
             --conf spark.kubernetes.namespace=aca534-tweetspace \
             --conf spark.kubernetes.authenticate.driver.serviceAccountName=gitlab-serviceaccount \
             /opt/spark/jars/spark-example-0.1.0-SNAPSHOT-standalone.jar
