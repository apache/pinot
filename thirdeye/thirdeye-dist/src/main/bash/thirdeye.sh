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
#

# Script Usage
# ---------------------------------------------
# ./thirdeye.sh ${MODE}
#
# - MODE: Choices: {frontend, backend, * }
#       frontend: Start the frontend server only
#       backend: Start the backend server only
#       database: Start the H@ db service. Also initializes the db with the required tables
#       For any other value, defaults to starting all services with an h2 db.
#

# Attempt to set APP_HOME
# Resolve links: $0 may be a link
PRG="$0"
# Need this for relative symlinks.
while [ -h "$PRG" ] ; do
    ls=`ls -ld "$PRG"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '/.*' > /dev/null; then
        PRG="$link"
    else
        PRG=`dirname "$PRG"`"/$link"
    fi
done
SAVED="`pwd`"
cd "`dirname \"$PRG\"`/.." >/dev/null
APP_HOME="`pwd -P`"
cd "$SAVED" >/dev/null

CONFIG_DIR="${APP_HOME}/config"
LIB_DIR="${APP_HOME}/lib"

CLASSPATH="${APP_HOME}/lib/fat/*"

function start_server {
  class_ref=$1
  config_dir=$2
  java -Dlog4j.configurationFile=log4j2.xml -cp "${CLASSPATH}" ${class_ref} "${config_dir}"
}

function start_frontend {
  echo "Running Thirdeye frontend config: ${CONFIG_DIR}"
  start_server org.apache.pinot.thirdeye.dashboard.ThirdEyeDashboardApplication "${CONFIG_DIR}"
}

function start_backend {
  echo "Running Thirdeye backend config: ${CONFIG_DIR}"
  start_server  org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyApplication "${CONFIG_DIR}"
}


function start_db {
  echo "Starting H2 database server"
  java -cp "${CLASSPATH}" org.h2.tools.Server -tcp -baseDir "${CONFIG_DIR}/.." &
  sleep 1

  echo "Creating ThirdEye database schema"
  java -cp "${CLASSPATH}" org.h2.tools.RunScript -user "sa" -password "sa" -url "jdbc:h2:tcp:localhost/h2db" -script "zip:./bin/thirdeye-pinot.jar!/schema/create-schema.sql"

  if [ -f "${CONFIG_DIR}/bootstrap.sql" ]; then
    echo "Running database bootstrap script ${CONFIG_DIR}/bootstrap.sql"
    java -cp "${CLASSPATH}" org.h2.tools.RunScript -user "sa" -password "sa" -url "jdbc:h2:tcp:localhost/h2db" -script "${CONFIG_DIR}/bootstrap.sql"
  fi
}

function start_coordinator {
  CLASSPATH=""
  for filepath in "${LIB_DIR}"/*; do
    CLASSPATH="${CLASSPATH}:${filepath}"
  done
  class_ref="org.apache.pinot.thirdeye.ThirdEyeServer"

  java -Dlog4j.configurationFile=log4j2.xml -cp "${CLASSPATH}" ${class_ref} "${config_dir}"
}

function start_all {
  start_db

  start_backend &
  sleep 10

  start_frontend &
  wait
}

MODE=$1
case ${MODE} in
    "coordinator" )  start_coordinator ;;
    "frontend" )  start_frontend ;;
    "backend"  )  start_backend ;;
    "database" )  start_db;;
    * )           echo "blah" #start_all ;;
esac
