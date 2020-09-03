#!/bin/sh
set -euo pipefail

## Initialize files from environment variables. These should come from the kubernetes secrets configured
## CONFIG_DIR is an environment variable, ideally equal to /opt/thirdeye/config/default. Look at the Dockerfile.thirdeye for reference

echo "Setting Config File : $CONFIG_DIR/persistence.yml"

sed -e 's/MYSQL_HOSTNAME/'"$MYSQL_HOSTNAME"'/' -e 's/MYSQL_PORT/'"$MYSQL_PORT"'/' -e 's/THIRDEYE_DATABASE/'"$THIRDEYE_DATABASE"'/' -e 's/MYSQL_PASSWORD/'"$MYSQL_PASSWORD"'/' $CONFIG_DIR/persistence.yml.tmpl > $CONFIG_DIR/persistence.yml

echo "Setting Config File : $CONFIG_DIR/data-sources/data-sources-config.yml"
sed -e 's/POSTGRES_HOSTNAME/'"$POSTGRES_HOSTNAME"'/' -e 's/POSTGRES_PORT/'"$POSTGRES_PORT"'/' -e 's/POSTGRES_DATABASE/'"$POSTGRES_DATABASE"'/' -e 's/POSTGRESQL_PASSWORD/'"$POSTGRESQL_PASSWORD"'/' $CONFIG_DIR/data-sources/data-sources-config.yml.tmpl > $CONFIG_DIR/data-sources/data-sources-config.yml

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

