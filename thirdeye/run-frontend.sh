#!/bin/bash
echo "*******************************************************"
echo "Launching ThirdEye Dashboard in demo mode"
echo "*******************************************************"

cd thirdeye-pinot
java -cp "./target/thirdeye-pinot-1.0.0-SNAPSHOT.jar" org.apache.pinot.thirdeye.dashboard.ThirdEyeDashboardApplication "./config"
cd ..
