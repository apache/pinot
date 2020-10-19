#!/bin/bash
echo "*******************************************************"
echo "Launching ThirdEye backend worker in demo mode"
echo "*******************************************************"

cd thirdeye-dashboard
java -Dlog4j.configurationFile=log4j2.xml -cp "./target/thirdeye-dashboard-1.0.0-SNAPSHOT.jar" org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyApplication "./config"
cd ..
