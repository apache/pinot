#!/bin/bash


CONFIG_DIR="./config"
JAR_PATH="./bin/thirdeye-pinot-1.0.0-SNAPSHOT.jar"
java -cp "${JAR_PATH}" org.apache.pinot.thirdeye.dashboard.ThirdEyeDashboardApplication ${CONFIG_DIR}

#java -cp "${JAR_PATH}" org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyApplication ${CONFIG_DIR}