#!/bin/bash
echo "*******************************************************"
echo "Launching ThirdEye Dashboard in demo mode"
echo "*******************************************************"

cd thirdeye-pinot
java -Dlog4j.configurationFile=log4j2.xml -cp "./target/thirdeye-pinot-1.0.0-SNAPSHOT.jar" org.apache.pinot.thirdeye.dashboard.ThirdEyeDashboardApplication "./config"
cd ..
