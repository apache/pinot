#!/bin/bash


CONFIG_DIR="/opt/thirdeye/config"
JAR_PATH="/opt/thirdeye/bin/thirdeye-pinot-1.0.0-SNAPSHOT.jar"

cd /opt/thirdeye
java -cp ${JAR_PATH} ${CLASSPATH} ${CONFIG_DIR}

#java -cp "${JAR_PATH}" org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyApplication ${CONFIG_DIR}