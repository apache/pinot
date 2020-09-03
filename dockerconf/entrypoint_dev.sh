#!/bin/sh
set -euo pipefail

app_type=$1
if [[ "${app_type}" == "frontend" ]]; then
echo "Starting TE Frontend"
sleep 15
java -Dlog4j.configurationFile=log4j2.xml -cp "/opt/thirdeye/bin/thirdeye-pinot.jar" org.apache.pinot.thirdeye.dashboard.ThirdEyeDashboardApplication /opt/thirdeye/config/default
elif [[ "${app_type}" == "backend" ]]; then
echo "Starting TE Backend"
sleep 15
java -Dlog4j.configurationFile=log4j2.xml -cp "/opt/thirdeye/bin/thirdeye-pinot.jar" org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyApplication /opt/thirdeye/config/default
fi

