#!/bin/bash

# Description: This script is used to run a Spark application in the cluster mode.
APP_NAME="FlightClassification"
MAIN_CLASS="Main"
DEPLOYED_MODE="client"
SPARK_MASTER="spark://vmhadoopmaster.cluster.lamsade.dauphine.fr:7077"
CONFIG_FILE="src/main/resources/config.yaml"
EXECUTOR_CORES="4"
EXECUTOR_MEMORY="1G"
NUM_EXECUTORS="2"
JAR_FILE=""

# Run the Spark application
spark-submit \
    --class $MAIN_CLASS \
    --deploy-mode $DEPLOYED_MODE \
    --name $APP_NAME \
    --master $SPARK_MASTER \
    --executor-cores $EXECUTOR_CORES \
    --executor-memory $EXECUTOR_MEMORY \
    --num-executors $NUM_EXECUTORS \
    --files $CONFIG_FILE \
    $JAR_FILE