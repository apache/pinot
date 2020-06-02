#!/bin/bash
echo "*******************************************************"
echo "Launching ThirdEye backend worker in demo mode"
echo "*******************************************************"

cd thirdeye-pinot
java -cp "./target/thirdeye-pinot-1.0.0-SNAPSHOT.jar" org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyApplication "./config"
cd ..
