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

# get usage of the script 
function usage() {
  command=$1
  if [[ "$command" =~ compCheck.sh$ ]] ; then
    echo "Usage: $command olderCommit newerCommit [workingDir]"
  elif [[ "$command" =~ checkoutAndBuild$ ]]; then
    echo "Usage: $command commitHash dirName"
  elif [[ "$command" =~ startService$ ]]; then
    echo "Usage: $command serviceName dirName"
  elif [[ "$command" =~ stopService$ ]]; then
    echo "Usage: $command serviceName"
  fi
  exit 1
}

# cleanup the temporary directory when exiting the script
function cleanup() {
  if [ -n "$tmpDir" ] && [ -d "$tmpDir" ]; then
    rm -rf "$tmpDir"
  fi
}

trap cleanup EXIT

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
  # use a temp directory in case workingDir is not provided
  tmpDir=$(mktemp -d 2>/dev/null || mktemp -d -t 'mytmpdir')
  workingDir=$tmpDir
fi


# This function builds Pinot given a specific commit hash and target directory
function checkoutAndBuild() {
  if [ $# -ne 2 ]; then 
    usage checkoutAndBuild
  fi
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

function startService() {
  if [ $# -ne 2 ]; then 
    usage startService
  fi
  serviceName=$1
  dirName=$2
  pushd "$dirName"/pinot-tools/target/pinot-tools-pkg/bin  || exit 1
  if [ "$serviceName" = "zookeeper" ]; then
    sh -c 'echo $$ > /tmp/zookeeper.pid; exec ./pinot-admin.sh StartZookeeper' &
  elif [ "$serviceName" = "controller" ]; then 
    sh -c 'echo $$ > /tmp/controller.pid; exec ./pinot-admin.sh StartController' &  
  elif [ "$serviceName" = "broker" ]; then
    sh -c 'echo $$ > /tmp/broker.pid; exec ./pinot-admin.sh StartBroker' &
  elif [ "$serviceName" = "server" ]; then
    sh -c 'echo $$ > /tmp/server.pid; exec ./pinot-admin.sh StartServer' &
  fi 
  popd || exit 1
}

function stopService() {
  if [ $# -ne 1 ]; then 
    usage stopService
  fi
  serviceName=$1
  if [ "$serviceName" = "zookeeper" ]; then
    zookeeperPid=$(</tmp/zookeeper.pid)
    kill -9 "$zookeeperPid"
  elif [ "$serviceName" = "controller" ]; then 
    controllerPid=$(</tmp/controller.pid)
    kill -9 "$controllerPid" 
  elif [ "$serviceName" = "broker" ]; then
    brokerPid=$(</tmp/broker.pid)
    kill -9 "$brokerPid"
  elif [ "$serviceName" = "server" ]; then
    serverPid=$(</tmp/server.pid)
    kill -9 "$serverPid"
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
  stopService controller
  stopService broker
  stopService server
  stopService zookeeper
  echo "Cluster stopped."
} 

# upgrades the cluster by upgrading components one by one (Controller -> Broker -> Server) 
function switchComponents() {
  dirName=$1
  stopService controller
  startService controller "$dirName"
  stopService broker
  startService broker "$dirName"
  stopService server
  startService server "$dirName"
}

# Do rolling upgrade from olderCommit to newerCommit
function upgradeAndTest() {
  switchComponents "$newTargetDir"
  echo "Rolling upgrade complete."
}

# Do rolling upgrade from newerCommit to olderCommit
function downgradeAndTest() {
  switchComponents "$oldTargetDir"
  echo "Rolling downgrade complete."
}

# create subdirectories for given commits
oldTargetDir="$workingDir"/oldTargetDir
newTargetDir="$workingDir"/newTargetDir

if ! mkdir -p "$oldTargetDir" "$newTargetDir"; then
  echo "Failed to create target directory"
  exit 1
fi

# Building targets
echo "Building the first target ... "
checkoutAndBuild "$olderCommit" "$oldTargetDir"
echo "Building the second target ..."
checkoutAndBuild "$newerCommit" "$newTargetDir"

# check that the default ports are available
if [ "$(lsof -t -i:8097 -s TCP:LISTEN)" ] || [ "$(lsof -t -i:8098 -sTCP:LISTEN)" ] || [ "$(lsof -t -i:8099 -sTCP:LISTEN)" ] || 
     [ "$(lsof -t -i:9000 -sTCP:LISTEN)" ] || [ "$(lsof -t -i:2181 -sTCP:LISTEN)" ]; then
  echo "Cannot start the components since the default ports are not available. Check any existing process that may be using the default ports."
  exit 1
fi

# Setup initial cluster with olderCommit and do rolling upgrade
startServices "$oldTargetDir"
sleep 20
upgradeAndTest
sleep 20
downgradeAndTest
sleep 20
stopServices

exit 0
