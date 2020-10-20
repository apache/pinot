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
#  We may modify to choose a minimal run in which the same set of operatons are run 
#  between any two component upgrades/rollbacks -- this may consist of adding
#  one more segment to table, adding some more rows to the stream topic, and
#  running some queries with the new data.

# get a temporary directory in case the workingDir is not provided by user   
TMP_DIR=$(mktemp -d 2>/dev/null || mktemp -d -t 'mytmpdir')

COMPAT_TESTER_PATH="pinot-integration-tests/target/pinot-integration-tests-pkg/bin/pinot-compat-test-runner.sh"

# get usage of the script 
function usage() {
  command=$1
  echo "Usage: $command olderCommit newerCommit [workingDir]"
  exit 1
}

# cleanup the temporary directory when exiting the script
function cleanup() {
  if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR" ] && [ "$workingDir" = "$TMP_DIR" ] ; then
    echo "The temporary directory $TMP_DIR needs to be cleaned up."
  fi
}

# This function builds Pinot given a specific commit hash and target directory
function checkoutAndBuild() {
  commitHash=$1
  targetDir=$2
  
  pushd "$targetDir" || exit 1
  git init
  git remote add origin https://github.com/apache/incubator-pinot
  git fetch --depth 1 origin "$commitHash"
  git checkout FETCH_HEAD
  mvn install package -DskipTests -Pbin-dist
  popd || exit 1
}

# Given a component and directory, start that version of the specific component 
function startService() {
  serviceName=$1
  dirName=$2
  # Upon start, save the pid of the process for a component into a file in /tmp/{component}.pid, which is then used to stop it
  pushd "$dirName"/pinot-tools/target/pinot-tools-pkg/bin  || exit 1
  if [ "$serviceName" = "zookeeper" ]; then
    sh -c 'echo $$ > $0/zookeeper.pid; exec ./pinot-admin.sh StartZookeeper' "${dirName}" &
  elif [ "$serviceName" = "controller" ]; then 
    sh -c 'echo $$ > $0/controller.pid; exec ./pinot-admin.sh StartController' "${dirName}" &
  elif [ "$serviceName" = "broker" ]; then
    sh -c 'echo $$ > $0/broker.pid; exec ./pinot-admin.sh StartBroker' "${dirName}" &
  elif [ "$serviceName" = "server" ]; then
    sh -c 'echo $$ > $0/server.pid; exec ./pinot-admin.sh StartServer' "${dirName}" &
  fi 
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
}

# Starts a Pinot cluster given a specific target directory
function startServices() {
  dirName=$1
  startService zookeeper "$dirName"
  startService controller "$dirName"
  startService broker "$dirName"
  startService server "$dirName"
  echo "Cluster started."
}

# Stops the currently running Pinot cluster
function stopServices() {
  dirName=$1
  stopService controller "$dirName"
  stopService broker "$dirName"
  stopService server "$dirName"
  stopService zookeeper "$dirName"
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

# cleanp the temporary directory when the bash script exits 
trap cleanup EXIT

setupCompatTester

if [ $# -lt 2 ] || [ $# -gt 3 ] ; then
  usage compCheck
fi

# get arguments
olderCommit=$1
newerCommit=$2 

if [ -n "$3" ]; then
  workingDir=$3
  if [ -d "$workingDir" ]; then
    echo "Directory ${workingDir} already exists. Use a new directory."
    exit 1
  fi
else
  # use the temp directory in case workingDir is not provided
  workingDir=$TMP_DIR
fi

# create subdirectories for given commits
oldTargetDir="$workingDir"/oldTargetDir
newTargetDir="$workingDir"/newTargetDir

if ! mkdir -p "$oldTargetDir"; then
  echo "Failed to create target directory ${oldTargetDir}"
  exit 1
fi
if ! mkdir -p "$newTargetDir"; then
  echo "Failed to create target directory ${newTargetDir}"
  exit 1
fi

# Building targets
echo "Building the old version ... "
checkoutAndBuild "$olderCommit" "$oldTargetDir"
echo "Building the new version ..."
checkoutAndBuild "$newerCommit" "$newTargetDir"

# check that the default ports are open
if [ "$(lsof -t -i:8097 -s TCP:LISTEN)" ] || [ "$(lsof -t -i:8098 -sTCP:LISTEN)" ] || [ "$(lsof -t -i:8099 -sTCP:LISTEN)" ] || 
     [ "$(lsof -t -i:9000 -sTCP:LISTEN)" ] || [ "$(lsof -t -i:2181 -sTCP:LISTEN)" ]; then
  echo "Cannot start the components since the default ports are not open. Check any existing process that may be using the default ports."
  exit 1
fi

# Setup initial cluster with olderCommit and do rolling upgrade
startServices "$oldTargetDir"
#$COMPAT_TESTER pre-controller-upgrade.yaml; if [ $? -ne 0 ]; then exit 1; fi
stopService controller "$oldTargetDir"
startService controller "$newTargetDir"
#$COMPAT_TESTER pre-broker-upgrade.yaml; if [ $? -ne 0 ]; then exit 1; fi
stopService broker "$oldTargetDir"
startService broker "$newTargetDir"
#$COMPAT_TESTER pre-server-upgrade.yaml; if [ $? -ne 0 ]; then exit 1; fi
stopService server "$oldTargetDir"
startService server "$newTargetDir"
#$COMPAT_TESTER post-server-upgrade.yaml; if [ $? -ne 0 ]; then exit 1; fi

# Upgrade complated, now do a rollback
stopService controller "$newTargetDir"
startService controller "$oldTargetDir"
#$COMPAT_TESTER post-server-rollback.yaml; if [ $? -ne 0 ]; then exit 1; fi
stopService broker "$newTargetDir"
startService broker "$oldTargetDir"
#$COMPAT_TESTER post-broker-rollback.yaml; if [ $? -ne 0 ]; then exit 1; fi
stopService server "$newTargetDir"
startService server "$oldTargetDir"
#$COMPAT_TESTER post-controller-rollback.yaml; if [ $? -ne 0 ]; then exit 1; fi
stopServices "$oldTargetDir"

exit 0
