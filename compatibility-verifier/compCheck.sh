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

# A script that does rolling upgrade of Pinot components
# from one version to the other given 2 commit hashes. It first builds
# Pinot in the 2 given directories and then upgrades in the following order:
# Controller -> Broker -> Server
#
# TODO Some ideas to explore:
#  It will be nice to have the script take arguments about what is to be done.
#  For example, we may want to verify the upgrade path in a different order.
#  Better yet, test all orders to decide that the upgrade can be done in any order.
#  Or, we may want to test upgrade of a specific component only.
#
#  For now, this script runs specific yaml files as a part of testing between
#  component upgrades/rollbacks. Perhaps we can change it to take a directory name
#  and run all the scripts in the directory in alpha order, one script at each
#  "stage" of upgrade.
#
#  We may modify to choose a minimal run in which the same set of operations are run
#  between any two component upgrades/rollbacks -- this may consist of adding
#  one more segment to table, adding some more rows to the stream topic, and
#  running some queries with the new data.


# get usage of the script
function usage() {
  command=$1
  echo "Usage: $command [workingDir]"
  exit 1
}

function waitForZkReady() {
  # TODO: check ZK to be ready instead of sleep
  sleep 60
  echo "zookeeper is ready"
}

function waitForControllerReady() {
  # TODO: check Controller to be ready instead of sleep
  sleep 60
  echo "controller is ready"
}

function waitForKafkaReady() {
  # TODO: check kafka to be ready instead of sleep
  sleep 10
  echo "kafka is ready"
}

function waitForClusterReady() {
  # TODO: check cluster to be ready instead of sleep
  sleep 2
  echo "Cluster ready."
}
# Given a component and directory, start that version of the specific component
function startService() {
  serviceName=$1
  dirName=$2
  # Upon start, save the pid of the process for a component into a file in /working_dir/{component}.pid, which is then used to stop it
  pushd "$dirName"/pinot-tools/target/pinot-tools-pkg/bin  || exit 1
  if [ "$serviceName" = "zookeeper" ]; then
    sh -c 'rm -rf ${0}/zkdir'
    sh -c 'echo $$ > $0/zookeeper.pid; exec ./pinot-admin.sh StartZookeeper -dataDir ${0}/zkdir > ${0}/zookeeper.log 2>&1' "${dirName}" &
  elif [ "$serviceName" = "controller" ]; then
    sh -c 'echo $$ > $0/controller.pid; exec ./pinot-admin.sh StartController > ${0}/controller.log 2>&1' "${dirName}" &
  elif [ "$serviceName" = "broker" ]; then
    sh -c 'echo $$ > $0/broker.pid; exec ./pinot-admin.sh StartBroker > ${0}/broker.log 2>&1' "${dirName}" &
  elif [ "$serviceName" = "server" ]; then
    sh -c 'echo $$ > $0/server.pid; exec ./pinot-admin.sh StartServer > ${0}/server.log 2>&1' "${dirName}" &
  elif [ "$serviceName" = "kafka" ]; then
    sh -c 'echo $$ > $0/kafka.pid; exec ./pinot-admin.sh StartKafka -zkAddress localhost:2181/kafka > ${0}/kafka.log 2>&1' "${dirName}" &
  fi

  echo "${serviceName} started"
  popd || exit 1
}

# Given a component, check if it known to be running and stop that specific component
function stopService() {
  serviceName=$1
  dirName=$2
  if [ -f "${dirName}/${serviceName}".pid ]; then
    servicePid=$(<"${dirName}/${serviceName}".pid)
    rm "${dirName}/${serviceName}".pid
    if [ -n "$servicePid" ]; then
      kill -9 "$servicePid"
    fi
  else
    echo "Pid file ${dirName}/${serviceName}.pid  not found. Failed to stop component ${serviceName}"
  fi
  echo "${serviceName} stopped"
}

# Starts a Pinot cluster given a specific target directory
function startServices() {
  dirName=$1
  startService zookeeper "$dirName"
  # Controller depends on zookeeper, if not wait zookeeper to be ready, controller will crash.
  waitForZkReady
  startService controller "$dirName"
  # Broker depends on controller, if not wait controller to be ready, broker will crash.
  waitForControllerReady
  startService broker "$dirName"
  startService server "$dirName"
  startService kafka "$dirName"
  waitForKafkaReady
  echo "Cluster started."
  waitForClusterReady
}

# Stops the currently running Pinot cluster
function stopServices() {
  dirName=$1
  stopService controller "$dirName"
  stopService broker "$dirName"
  stopService server "$dirName"
  stopService zookeeper "$dirName"
  stopService kafka "$dirName"
  echo "Cluster stopped."
}

# Setup the path and classpath prefix for compatibility tester executable
function setupCompatTester() {
  COMPAT_TESTER="$(dirname $0)/../${COMPAT_TESTER_PATH}"
  local pinotIntegTestsRelDir="$(dirname $0)/../pinot-integration-tests/target"
  local pinotIntegTestsAbsDir=`(cd ${pinotIntegTestsRelDir};pwd)`
  CLASSPATH_PREFIX=$(ls ${pinotIntegTestsAbsDir}/pinot-integration-tests-*-tests.jar)
  export CLASSPATH_PREFIX
}

#
# Main
#

if [ $# -ne 1 ] ; then
  usage compCheck
fi

COMPAT_TESTER_PATH="pinot-integration-tests/target/pinot-integration-tests-pkg/bin/pinot-compat-test-runner.sh"

# create subdirectories for given commits
workingDir=$1
oldTargetDir="$workingDir"/oldTargetDir
newTargetDir="$workingDir"/newTargetDir

setupCompatTester

# check that the default ports are open
if [ "$(lsof -t -i:8097 -s TCP:LISTEN)" ] || [ "$(lsof -t -i:8098 -sTCP:LISTEN)" ] || [ "$(lsof -t -i:8099 -sTCP:LISTEN)" ] ||
     [ "$(lsof -t -i:9000 -sTCP:LISTEN)" ] || [ "$(lsof -t -i:2181 -sTCP:LISTEN)" ]; then
  echo "Cannot start the components since the default ports are not open. Check any existing process that may be using the default ports."
  exit 1
fi


# Setup initial cluster with olderCommit and do rolling upgrade
startServices "$oldTargetDir"
#$COMPAT_TESTER pre-controller-upgrade.yaml 1; if [ $? -ne 0 ]; then exit 1; fi
stopService controller "$oldTargetDir"
startService controller "$newTargetDir"
waitForControllerReady
#$COMPAT_TESTER pre-broker-upgrade.yaml 2; if [ $? -ne 0 ]; then exit 1; fi
stopService broker "$oldTargetDir"
startService broker "$newTargetDir"
#$COMPAT_TESTER pre-server-upgrade.yaml 3; if [ $? -ne 0 ]; then exit 1; fi
stopService server "$oldTargetDir"
startService server "$newTargetDir"
#$COMPAT_TESTER post-server-upgrade.yaml 4; if [ $? -ne 0 ]; then exit 1; fi

# Upgrade completed, now do a rollback
stopService server "$newTargetDir"
startService server "$oldTargetDir"
#$COMPAT_TESTER post-server-rollback.yaml 5; if [ $? -ne 0 ]; then exit 1; fi
stopService broker "$newTargetDir"
startService broker "$oldTargetDir"
#$COMPAT_TESTER post-broker-rollback.yaml 6; if [ $? -ne 0 ]; then exit 1; fi
stopService controller "$newTargetDir"
startService controller "$oldTargetDir"
waitForControllerReady
#$COMPAT_TESTER post-controller-rollback.yaml 7; if [ $? -ne 0 ]; then exit 1; fi
stopServices "$oldTargetDir"

exit 0