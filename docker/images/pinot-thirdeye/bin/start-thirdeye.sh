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

# Script Usage
# ---------------------------------------------
# ./start-thirdeye.sh ${CONFIG_DIR} ${MODE}
#
# - CONFIG_DIR: path to the thirdeye configuration director
# - MODE: Choices: {frontend, backend, * }
#       frontend: Start the frontend server only
#       backend: Start the backend server only
#       For any other value, defaults to starting all services with an h2 db.
#

if [[ "$#" -gt 0 ]]
then
  CONFIG_DIR="./config/$1"
else
  CONFIG_DIR="./config/default"
fi


function start_server {
  class_ref=$1
  config_dir=$2
  java -Dlog4j.configurationFile=log4j2.xml -cp "./bin/thirdeye-pinot.jar" ${class_ref} "${config_dir}"
}

function start_frontend {
  echo "Running Thirdeye frontend config: ${CONFIG_DIR}"
  start_server org.apache.pinot.thirdeye.dashboard.ThirdEyeDashboardApplication "${CONFIG_DIR}"
}

function start_backend {
  echo "Running Thirdeye backend config: ${CONFIG_DIR}"
  start_server  org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyApplication "${CONFIG_DIR}"
}

function start_all {
  echo "Starting H2 database server"
  java -cp "./bin/thirdeye-pinot.jar" org.h2.tools.Server -tcp -baseDir "${CONFIG_DIR}/.." &
  sleep 1

  echo "Creating ThirdEye database schema"
  java -cp "./bin/thirdeye-pinot.jar" org.h2.tools.RunScript -user "sa" -password "sa" -url "jdbc:h2:tcp:localhost/h2db" -script "zip:./bin/thirdeye-pinot.jar!/schema/create-schema.sql"

  if [ -f "${CONFIG_DIR}/bootstrap.sql" ]; then
    echo "Running database bootstrap script ${CONFIG_DIR}/bootstrap.sql"
    java -cp "./bin/thirdeye-pinot.jar" org.h2.tools.RunScript -user "sa" -password "sa" -url "jdbc:h2:tcp:localhost/h2db" -script "${CONFIG_DIR}/bootstrap.sql"
  fi

  start_backend &
  sleep 10

  start_frontend &
  wait
}

MODE=$2
case ${MODE} in
    "frontend" )  start_frontend ;;
    "backend" )   start_backend ;;
    * )           start_all ;;
esac
