#!/bin/bash

# 변수 설정
CONTAINER_NAME="etl_data-spark-master-1"
JAR_FILE="resources/elasticsearch-spark-30_2.12-8.4.3.jar"
PY_FILE="jobs/main2.py"
MASTER_URL="spark://spark-master:7077"

# 메모리 설정
DRIVER_MEMORY="2g"
EXECUTOR_MEMORY="4g"

# command
docker exec $CONTAINER_NAME spark-submit \
    --jars $JAR_FILE \
    --master $MASTER_URL \
    --conf spark.driver.memory=$DRIVER_MEMORY \
    --conf spark.executor.memory=$EXECUTOR_MEMORY \
    $PY_FILE