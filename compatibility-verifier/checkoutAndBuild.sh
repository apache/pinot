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
cmdName=`baseName $0`

# get usage of the script
function usage() {
  echo "Usage: $cmdName -o olderCommit -n newerCommit [-w workingDir]"
  exit 1
}

#compute absolute path from a relative path
function absPath() {
  local relPath=$1
  if [[ ! "$relPath" == /* ]]; then
    #relative path
    absolutePath=$(
      cd "$relPath"
      pwd
    )
  fi
  echo "$absolutePath"
}

# This function builds Pinot given a specific commit hash and target directory
function checkoutAndBuild() {
  commitHash=$1
  targetDir=$2

  pushd "$targetDir" || exit 1
  git init
  git remote add origin https://github.com/apache/incubator-pinot
  git pull origin master
  git checkout $commitHash
  mvn install package -DskipTests -Pbin-dist
  popd || exit 1
}

# get arguments
# Args while-loop
while [ "$1" != "" ]; do
  case $1 in
  -w | --working-dir)
    shift
    workingDir=$1
    ;;
  -o | --old-commit-hash)
    shift
    olderCommit=$1
    ;;
  -n | --new-commit-hash)
    shift
    newerCommit=$1
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

if [ -z "$olderCommit" -o -z "$newerCommit" ]; then
  usage
  exit 1
fi

if [ -z $workingDir ]; then
  workingDir=$TMP_DIR
  echo "Using working directory $TMP_DIR"
else 
  if [ -d $workingDir ]; then
    echo "Directory ${workingDir} already exists. Use a new directory."
    usage
    exit 1
  else
    mkdir -p ${workingDir}
    if [ $? -ne 0 ]; then
      echo "Could not create ${workingDir}"
      exit 1
    fi
    workingDir=$(absPath "$workingDir")
  fi
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
