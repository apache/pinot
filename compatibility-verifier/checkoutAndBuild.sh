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


# get a temporary directory in case the workingDir is not provided by user
TMP_DIR=$(mktemp -d 2>/dev/null || mktemp -d -t 'mytmpdir')

# get usage of the script
function usage() {
  command=$1
  echo "Usage: $command olderCommit newerCommit [workingDir]"
  exit 1
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
  echo "workingDir: ${workingDir}"
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
