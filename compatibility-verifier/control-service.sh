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

# A helper script that starts/stops given component for a given build of Pinot
# This script is intended to be called stand-alone or from comp-verifier.sh 
# command syntax: ./control-service.sh command component target_dir pinot_version

# verify correct usage of this script 
if [[ $# -ne 4 ]]; then
  printf "You used %s arguments \n" "$#"
  printf "Usage: \n ./control-service.sh command component target_dir pinot_version \n"
  exit 1
fi

# get arguments
command=$1
component=$2
target_dir=$3
pinot_version=$4

declare -a commands=("start" "stop")
declare -a components=("controller" "broker" "server" "zookeeper")

if [[ ! -d "$target_dir"/incubator-pinot ]]; then
  printf "%s/incubator-pinot does not exist \n " "$target_dir"
  exit 1
fi

# Handle invalid arguments
if [[ ! " ${commands[*]} " =~ $command  ]]; then
  printf "%s is not a valid command. Command must be one of: start and stop\n" "$command"
  printf "Usage: \n ./controlService.sh command component target_dir \n"
  exit 1
fi
if [[ ! " ${components[*]} " =~ $component  ]]; then
  printf "Not a valid component. Needs to be one of: %s \n" "${components[*]}"
  exit 1
fi

# Define ports for the test
zkPort=2181
localhost=2181
controllerPort=9001
brokerPort=7001
serverPort=8001
serverAdminPort=8011

if [[ "$command" == "start" ]]; then
  # Navigate to the directory containing the scripts
  if [[ $pinot_version == "0.3.0" ]]; then
  pushd "$target_dir"/incubator-pinot/pinot-distribution/target/apache-pinot-incubating-"$pinot_version"-SNAPSHOT-bin/apache-pinot-incubating-"$pinot_version"-SNAPSHOT-bin/bin || exit 1
  elif [[ $pinot_version == "0.4.0" ]]; then
  pushd "$target_dir"/incubator-pinot/pinot-distribution/target/apache-pinot-incubating-"$pinot_version"-SNAPSHOT-bin/apache-pinot-incubating-"$pinot_version"-SNAPSHOT-bin/bin || exit 1
  else
  pushd "$target_dir"/incubator-pinot/pinot-distribution/target/apache-pinot-incubating-"$pinot_version"-SNAPSHOT-bin/apache-pinot-incubating-"$pinot_version"-SNAPSHOT-bin/bin || exit 1
  fi

  # Start the desired component
  # Upon start, save the pid of the process for a component into a file in /tmp/{component}.pid, which is then used to stop it
  if [[ "$component" == "controller" ]]; then
    sh -c 'echo $$ > /tmp/controller.pid; exec ./pinot-admin.sh StartController -zkAddress localhost:$0 -clusterName PinotCluster -controllerPort $1 > /tmp/pinot-controller.log' ${localhost} "${controllerPort}"
  elif [[ "$component" == "broker" ]]; then
    sh -c 'echo $$ > /tmp/broker.pid; exec ./pinot-admin.sh StartBroker -zkAddress localhost:$0 -clusterName PinotCluster -brokerPort $1 > /tmp/pinot-broker.log' ${localhost} "${brokerPort}"
  elif [[ "$component" == "server" ]]; then
    sh -c 'echo $$ > /tmp/server.pid; exec ./pinot-admin.sh StartServer -zkAddress localhost:$0 -clusterName PinotCluster -serverPort $1 -serverAdminPort $2 > /tmp/pinot-server.log' ${localhost} "${serverPort}" "${serverAdminPort}"
  else
    sh -c 'echo $$ > /tmp/zookeeper.pid; exec ./pinot-admin.sh StartZookeeper -zkPort $0 > /tmp/pinot-zookeeper.log' "${zkPort}"
  fi
  popd || exit 1
elif [[ "$command" == "stop" ]]; then
  # Stop the desired component
  if [[ "$component" == "controller" && -f /tmp/controller.pid ]]; then
    controller_pid=$(</tmp/controller.pid)
    kill -9 "$controller_pid" 
  elif [[ "$component" == "broker" && -f /tmp/broker.pid ]]; then
    broker_pid=$(</tmp/broker.pid)
    kill -9 "$broker_pid"
  elif [[ "$component" == "server" && -f /tmp/server.pid ]]; then
    server_pid=$(</tmp/server.pid)
    kill -9 "$server_pid"
  elif [[  "$component" == "zookeeper" && -f /tmp/zookeeper.pid ]]; then
    zookeeper_pid=$(</tmp/zookeeper.pid)
    kill -9 "$zookeeper_pid"
  else
    printf "Can't stop component %s since can't component %s is not known to have been started." "${component}" "${component}"
  fi
fi 
