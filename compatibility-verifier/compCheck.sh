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

RM="/bin/rm"
logCount=1
#Declare the number of mandatory args
cmdName=`basename $0`
source `dirname $0`/utils.inc

# get usage of the script
function usage() {
  echo "Usage: $cmdName -w <workingDir> -t <testSuiteDir> [-k]"
  echo -e "MANDATORY:"
  echo -e "  -w, --working-dir                      Working directory where olderCommit and newCommit target files reside."
  echo -e "  -t, --test-suite-dir                   Test suite directory\n"
  echo -e "OPTIONAL:"
  echo -e "  -k, --keep-cluster-on-failure          Keep cluster on test failure"
  echo -e "  -h, --help                             Prints this help\n"
}

function waitForZkReady() {
  status=1
  while [ $status -ne 0 ]; do
    sleep 1
    echo Checking port 2181 for zk ready
    echo x | nc localhost 2181 1>/dev/null 2>&1
    status=$(echo $?)
  done
}

function waitForControllerReady() {
  # TODO: Check real controller port if config file is specified.
  status=1
  while [ $status -ne 0 ]; do
    sleep 1
    echo Checking port 9000 for controller ready
    curl localhost:9000/health 1>/dev/null 2>&1
    status=$(echo $?)
  done
}

function waitForKafkaReady() {
  status=1
  while [ $status -ne 0 ]; do
    sleep 1
    echo Checking port 19092 for kafka ready
    echo x | nc localhost 19092 1>/dev/null 2>&1
    status=$(echo $?)
  done
}

function waitForBrokerReady() {
  # TODO: We are checking for port 8099. Check for real broker port from config if needed.
  local status=1
  while [ $status -ne 0 ]; do
    sleep 1
    echo Checking port 8099 for broker ready
    curl localhost:8099/debug/routingTable 1>/dev/null 2>&1
    status=$(echo $?)
  done
}

function waitForServerReady() {
  # TODO: We are checking for port 8097. Check for real server port from config if needed,
  local status=1
  while [ $status -ne 0 ]; do
    sleep 1
    echo Checking port 8097 for server ready
    curl localhost:8097/health 1>/dev/null 2>&1
    status=$(echo $?)
  done
}

function waitForClusterReady() {
  waitForBrokerReady
  waitForServerReady
  waitForKafkaReady
}

#set config file is present or not
function setConfigFileArg() {
  if [[ -f $1 ]]; then
    echo "-configFileName ${1}"
  fi
}

# Given a component and directory, start that version of the specific component
# Start the service in background.
# Record the pid file in $2/$1.pid
# $1 is service name
# $2 is directory name
# TODO get rid of exit from this function. Exit only returns from a function.
function startService() {
  serviceName=$1
  dirName=$2
  echo Starting $serviceName in $dirName
  local configFileArg=$(setConfigFileArg "$3")
  # Upon start, save the pid of the process for a component into a file in /working_dir/{component}.pid, which is then used to stop it
  pushd "$dirName"/pinot-tools/target/pinot-tools-pkg/bin 1>/dev/null || exit 1
  if [ "$serviceName" = "zookeeper" ]; then
    # Remove all previous zk data
    ${RM} -rf ${dirName}/zkdir
    ./pinot-admin.sh StartZookeeper -dataDir ${LOG_DIR}/zkdir 1>${LOG_DIR}/zookeeper.${logCount}.log 2>&1 &
    echo $! >${PID_DIR}/zookeeper.pid
  elif [ "$serviceName" = "controller" ]; then
    ./pinot-admin.sh StartController ${configFileArg} 1>${LOG_DIR}/controller.${logCount}.log 2>&1 &
    echo $! >${PID_DIR}/controller.pid
  elif [ "$serviceName" = "broker" ]; then
    ./pinot-admin.sh StartBroker ${configFileArg} 1>${LOG_DIR}/broker.${logCount}.log 2>&1 &
    echo $! >${PID_DIR}/broker.pid
  elif [ "$serviceName" = "server" ]; then
    ./pinot-admin.sh StartServer ${configFileArg} 1>${LOG_DIR}/server.${logCount}.log 2>&1 &
    echo $! >${PID_DIR}/server.pid
  elif [ "$serviceName" = "kafka" ]; then
    ./pinot-admin.sh StartKafka -zkAddress localhost:2181/kafka 1>${LOG_DIR}/kafka.${logCount}.log 2>&1 &
    echo $! >${PID_DIR}/kafka.pid
  fi
  # Keep log files distinct so we can debug
  logCount=$((logCount + 1))

  echo "${serviceName} started"
  popd 1>/dev/null || exit 1
}

# Given a component, check if it known to be running and stop that specific component
function stopService() {
  serviceName=$1
  if [ -f "${PID_DIR}/${serviceName}".pid ]; then
    pid=$(cat "${PID_DIR}/${serviceName}".pid)
    kill -9 $pid 1>/dev/null 2>&1
    # TODO Kill without -9 and add a while loop waiting for process to die
    status=0
    while [ $status -ne 1 ]; do
      echo "Waiting for $serviceName (pid $pid) to die"
      sleep 1
      ps -p $pid
      status=$(echo $?)
    done
    ${RM} -f "${PID_DIR}/${serviceName}".pid
    echo "${serviceName} stopped"
  else
    echo "Pid file ${PID_DIR}/${serviceName}.pid  not found. Failed to stop component ${serviceName}"
  fi
}

# Starts a Pinot cluster given a specific target directory
function startServices() {
  dirName=$1
  startService zookeeper "$dirName" "unused"
  # Controller depends on zookeeper, if not wait zookeeper to be ready, controller will crash.
  waitForZkReady
  startService controller "$dirName" "$CONTROLLER_CONF"
  # Broker depends on controller, if not wait controller to be ready, broker will crash.
  waitForControllerReady
  startService broker "$dirName" "$BROKER_CONF"
  startService server "$dirName" "$SERVER_CONF"
  startService kafka "$dirName" "unused"
  echo "Cluster started."
  waitForClusterReady
}

# Stops the currently running Pinot cluster
function stopServices() {
  stopService controller
  stopService broker
  stopService server
  stopService zookeeper
  stopService kafka
  echo "Cluster stopped."
}

# Setup the path and classpath prefix for compatibility tester executable
function setupCompatTester() {
  COMPAT_TESTER="$(dirname $0)/../${COMPAT_TESTER_PATH}"
  local pinotIntegTestsRelDir="$(dirname $0)/../pinot-integration-tests/target"
  local pinotIntegTestsAbsDir=$( (
    cd ${pinotIntegTestsRelDir}
    pwd
  ))
  CLASSPATH_PREFIX=$(ls ${pinotIntegTestsAbsDir}/pinot-integration-tests-*-tests.jar)
  export CLASSPATH_PREFIX
}

#
# Main
#

# create subdirectories for given commits
workingDir=
testSuiteDir=
keepClusterOnFailure="false"

# Args while-loop
while [ "$1" != "" ]; do
  case $1 in
  -w | --working-dir)
    shift
    workingDir=$(absPath $1)
    ;;
  -t | --test-suite-dir)
    shift
    testSuiteDir=$(absPath $1)
    ;;
  -k | keep-cluster-on-failure)
    keepClusterOnFailure="true"
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  *)
    echo "illegal option $1"
    usage
    exit 1 # error
    ;;
  esac
  shift
done

if [ -z "$workingDir" -o -z "$testSuiteDir" ]; then
  usage
  exit 1
fi

COMPAT_TESTER_PATH="pinot-integration-tests/target/pinot-integration-tests-pkg/bin/pinot-compat-test-runner.sh"

BROKER_CONF=${testSuiteDir}/config/BrokerConfig.conf
CONTROLLER_CONF=${testSuiteDir}/config/ControllerConfig.conf
SERVER_CONF=${testSuiteDir}/config/ServerConfig.conf
PID_DIR=${workingDir}/pids
LOG_DIR=${workingDir}/logs
${RM} -rf ${PID_DIR}
${RM} -rf ${LOG_DIR}

mkdir ${PID_DIR}
mkdir ${LOG_DIR}

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
# Provide abspath of filepath to $COMPAT_TESTER
echo "Setting up cluster before upgrade"
startServices "$oldTargetDir"

genNum=0
if [ -f $testSuiteDir/pre-controller-upgrade.yaml ]; then
  genNum=$((genNum+1))
  $COMPAT_TESTER $testSuiteDir/pre-controller-upgrade.yaml $genNum
  if [ $? -ne 0 ]; then
    if [ $keepClusterOnFailure == "false" ]; then
      stopServices
    fi
    echo Failed before controller upgrade
    exit 1
  fi
fi
echo "Upgrading controller"
stopService controller
startService controller "$newTargetDir" "$CONTROLLER_CONF"
waitForControllerReady

if [ -f $testSuiteDir/pre-broker-upgrade.yaml ]; then
  genNum=$((genNum+1))
  echo "Running tests after controller upgrade"
  $COMPAT_TESTER $testSuiteDir/pre-broker-upgrade.yaml $genNum
  if [ $? -ne 0 ]; then
    if [ $keepClusterOnFailure == "false" ]; then
      stopServices
    fi
    echo Failed before broker upgrade
    exit 1
  fi
fi
echo "Upgrading broker"
stopService broker
startService broker "$newTargetDir" "$BROKER_CONF"
waitForBrokerReady
if [ -f $testSuiteDir/pre-server-upgrade.yaml ]; then
  echo "Running tests after broker upgrade"
  genNum=$((genNum+1))
  $COMPAT_TESTER $testSuiteDir/pre-server-upgrade.yaml $genNum
  if [ $? -ne 0 ]; then
    if [ $keepClusterOnFailure == "false" ]; then
      stopServices
    fi
    echo Failed before server upgrade
    exit 1
  fi
fi
echo "Upgrading server"
stopService server
startService server "$newTargetDir" "$SERVER_CONF"
waitForServerReady
if [ -f $testSuiteDir/post-server-upgrade.yaml ]; then
  echo "Running tests after server upgrade"
  genNum=$((genNum+1))
  $COMPAT_TESTER $testSuiteDir/post-server-upgrade.yaml $genNum
  if [ $? -ne 0 ]; then
    if [ $keepClusterOnFailure == "false" ]; then
      stopServices
    fi
    echo Failed after server upgrade
    exit 1
  fi
fi

echo "Downgrading server"
# Upgrade completed, now do a rollback
stopService server
startService server "$oldTargetDir" "$SERVER_CONF"
waitForServerReady
if [ -f $testSuiteDir/post-server-rollback.yaml ]; then
  echo "Running tests after server downgrade"
  genNum=$((genNum+1))
  $COMPAT_TESTER $testSuiteDir/post-server-rollback.yaml $genNum
  if [ $? -ne 0 ]; then
    if [ $keepClusterOnFailure == "false" ]; then
      stopServices
    fi
    echo Failed after server downgrade
    exit 1
  fi
fi
echo "Downgrading broker"
stopService broker
startService broker "$oldTargetDir" "$BROKER_CONF"
waitForBrokerReady
if [ -f $testSuiteDir/post-broker-rollback.yaml ]; then
  echo "Running tests after broker downgrade"
  genNum=$((genNum+1))
  $COMPAT_TESTER $testSuiteDir/post-broker-rollback.yaml $genNum
  if [ $? -ne 0 ]; then
    if [ $keepClusterOnFailure == "false" ]; then
      stopServices
    fi
    echo Failed after broker downgrade
    exit 1
  fi
fi
echo "Downgrading controller"
stopService controller
startService controller "$oldTargetDir" "$CONTROLLER_CONF"
waitForControllerReady
waitForControllerReady
if [ -f $testSuiteDir/post-controller-rollback.yaml ]; then
  echo "Running tests after controller downgrade"
  genNum=$((genNum+1))
  $COMPAT_TESTER $testSuiteDir/post-controller-rollback.yaml $genNum
  if [ $? -ne 0 ]; then
    if [ $keepClusterOnFailure == "false" ]; then
      stopServices
    fi
    echo Failed after controller downgrade
    exit 1
  fi
fi
stopServices

echo "All tests passed"
exit 0
