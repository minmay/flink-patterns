#!/bin/bash

/opt/flink/bin/flink run --jobmanager jobmanager:8081 \
  --class $MAIN_CLASS_NAME \
  $JAR_FILE_NAME \
  --jdbc-driver $JDBC_DRIVER \
  --jdbc-url $JDBC_URL \
  --jdbc-username $JDBC_USERNAME \
  --jdbc-password $JDBC_PASSWORD \
  --save-point-path $SAVE_POINT_PATH
