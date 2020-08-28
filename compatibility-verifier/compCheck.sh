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
  fi
  exit 1
}

# cleanup the temporary directory when exiting the script
function cleanup() {
  if [ -n "$tmpDir" ] && [ -d "$tmpDir" ]; then
    rm -rf "$tmpDir"
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

# cleanp the temporary directory when the bash script exits 
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

# create subdirectories for given commits
oldTargetDir="$workingDir"/oldTargetDir
newTargetDir="$workingDir"/newTargetDir

if ! mkdir -p "$oldTargetDir" "$newTargetDir"; then
  echo "Failed to create target directory"
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
sleep 20
stopService controller "$oldTargetDir"
startService controller "$newTargetDir"
stopService broker "$oldTargetDir"
startService broker "$newTargetDir"
stopService server "$oldTargetDir"
startService server "$newTargetDir"
sleep 20
stopService controller "$newTargetDir"
startService controller "$oldTargetDir"
stopService broker "$newTargetDir"
startService broker "$oldTargetDir"
stopService server "$newTargetDir"
startService server "$oldTargetDir"
sleep 20
stopServices "$oldTargetDir"

exit 0
