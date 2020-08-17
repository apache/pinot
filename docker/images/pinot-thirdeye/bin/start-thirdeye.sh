#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

if [[ "$#" -gt 0 ]]
then
  CONFIG_DIR="./config/$1"
else
  CONFIG_DIR="./config/default"
fi

echo "Starting H2 database server"
java -cp "./bin/thirdeye-pinot.jar" org.h2.tools.Server -tcp -baseDir "${CONFIG_DIR}/.." &
sleep 1

echo "Creating ThirdEye database schema"
java -cp "./bin/thirdeye-pinot.jar" org.h2.tools.RunScript -user "sa" -password "sa" -url "jdbc:h2:tcp:localhost/h2db" -script "zip:./bin/thirdeye-pinot.jar!/schema/create-schema.sql"

if [ -f "${CONFIG_DIR}/bootstrap.sql" ]; then
  echo "Running database bootstrap script ${CONFIG_DIR}/bootstrap.sql"
  java -cp "./bin/thirdeye-pinot.jar" org.h2.tools.RunScript -user "sa" -password "sa" -url "jdbc:h2:tcp:localhost/h2db" -script "${CONFIG_DIR}/bootstrap.sql"
fi

echo "Running thirdeye backend config: ${CONFIG_DIR}"
[ -f "${CONFIG_DIR}/data-sources/data-sources-config-backend.yml" ] && cp "${CONFIG_DIR}/data-sources/data-sources-config-backend.yml" "${CONFIG_DIR}/data-sources/data-sources-config.yml"
java -Dlog4j.configurationFile=log4j2.xml -cp "./bin/thirdeye-pinot.jar" org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyApplication "${CONFIG_DIR}" &
sleep 10

echo "Running thirdeye frontend config: ${CONFIG_DIR}"
[ -f "${CONFIG_DIR}/data-sources/data-sources-config-frontend.yml" ] && cp "${CONFIG_DIR}/data-sources/data-sources-config-frontend.yml" "${CONFIG_DIR}/data-sources/data-sources-config.yml"
java -Dlog4j.configurationFile=log4j2.xml -cp "./bin/thirdeye-pinot.jar" org.apache.pinot.thirdeye.dashboard.ThirdEyeDashboardApplication "${CONFIG_DIR}" &

wait
