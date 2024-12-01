#!/bin/bash

# Description: This script is used to run a Spark application in the cluster mode.
APP_NAME="Flight"
MAIN_CLASS="Main"
DEPLOYED_MODE="client"
SPARK_MASTER="local[*]"
#SPARK_MASTER="spark://vmhadoopmaster.cluster.lamsade.dauphine.fr:7077"
EXECUTOR_CORES="2"
EXECUTOR_MEMORY="2G"
NUM_EXECUTORS="2"
JAR_FILE="target/scala-2.12/Flight-assembly-0.1.0-SNAPSHOT.jar"
#JAR_FILE="./workspace/Flight-assembly-0.1.0-SNAPSHOT.jar"

# Run the Spark application
spark-submit \
    --class $MAIN_CLASS \
    --deploy-mode $DEPLOYED_MODE \
    --name $APP_NAME \
    --master $SPARK_MASTER \
    --executor-cores $EXECUTOR_CORES \
    --executor-memory $EXECUTOR_MEMORY \
    --num-executors $NUM_EXECUTORS \
    $JAR_FILE \
    "data/"



