#!/bin/bash
echo "*******************************************************"
echo "Attempting to run ThirdEye Dashboard in demo mode"
echo "*******************************************************"

cd thirdeye-pinot
java -cp "./target/thirdeye-pinot-1.0-SNAPSHOT.jar" com.linkedin.thirdeye.dashboard.ThirdEyeDashboardApplication "./config"
cd ..
