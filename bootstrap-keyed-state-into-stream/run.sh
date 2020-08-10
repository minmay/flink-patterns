#!/bin/bash

mkdir -p $SAVE_POINT_PATH && \
chmod 777 $SAVE_POINT_PATH && \
/opt/flink/bin/flink run --jobmanager jobmanager:8081 \
  --class $MAIN_CLASS_NAME \
  $JAR_FILE_NAME \
  --bootstrap write \
  --jdbc-driver $JDBC_DRIVER \
  --jdbc-url $JDBC_URL \
  --jdbc-username $JDBC_USERNAME \
  --jdbc-password $JDBC_PASSWORD \
  --save-point-path $SAVE_POINT_PATH \
  && \
  /opt/flink/bin/flink run -s $SAVE_POINT_PATH --jobmanager jobmanager:8081 \
  --class $MAIN_CLASS_NAME \
  $JAR_FILE_NAME \
  --bootstrap read \
  --jdbc-driver $JDBC_DRIVER \
  --jdbc-url $JDBC_URL \
  --jdbc-username $JDBC_USERNAME \
  --jdbc-password $JDBC_PASSWORD
