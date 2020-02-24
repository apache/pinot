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
  TYPE=$1
  if [[ "${TYPE^^}" == "DASHBOARD" ]]
  then
    APP_CLASS_NAME="org.apache.pinot.thirdeye.dashboard.ThirdEyeDashboardApplication"
  else
    if [[ "${TYPE^^}" == "BACKEND" ]]
    then
      APP_CLASS_NAME="org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyApplication"
    else
      echo "Unknow type: $1"
      exit 1
    fi
  fi
else
  echo "Need at least one parameter."
  exit 1
fi

if [[ "$#" -gt 1 ]]
then
  CONFIG_DIR=$2
else
  CONFIG_DIR="./config"
fi

echo "Trying to run thirdeye with class ${APP_CLASS_NAME} and Config dir: ${CONFIG_DIR}"
java -cp "./bin/thirdeye-pinot-1.0-SNAPSHOT.jar" ${APP_CLASS_NAME} ${CONFIG_DIR}