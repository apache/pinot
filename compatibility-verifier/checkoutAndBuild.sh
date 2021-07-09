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

cmdName=`basename $0`
cmdDir=`dirname $0`
source ${cmdDir}/utils.inc

# get usage of the script
function usage() {
  echo "This command sets up the two builds to test compatibility in the working directory"
  echo "Usage: $cmdName [-o olderCommit] [-n newerCommit] -w workingDir"
  echo -e "  -w, --working-dir                      Working directory where olderCommit and newCommit target files reside\n"
  echo -e "  -o, --old-commit-hash                  git hash (or tag) for old commit\n"
  echo -e "  -n, --new-commit-hash                  git hash (or tag) for new commit\n"
  echo -e "If -n is not specified, then current commit is assumed"
  echo -e "If -o is not specified, then previous commit is assumed (expected -n is also empty)"
  echo -e "Examples:"
  echo -e "    To compare this checkout with previous commit: '${cmdName} -w /tmp/wd'"
  echo -e "    To compare this checkout with some older tag or hash: '${cmdName} -o release-0.7.1 -w /tmp/wd'"
  echo -e "    To compare any two previous tags or hashes: '${cmdName} -o release-0.7.1 -n 637cc3494 -w /tmp/wd"
  echo -e "Environment:"
  echo -e "    Additional maven build options can be passed in via environment variable PINOT_MAVEN_OPTS"
  exit 1
}

# This function builds Pinot given a specific commit hash and target directory
function checkOut() {
  local commitHash=$1
  local targetDir=$2

  pushd "$targetDir"  1>&2 || exit 1
  git init 1>&2 || exit 1
  git remote add origin https://github.com/apache/incubator-pinot 1>&2 || exit 1
  git pull origin master 1>&2 || exit 1
  # Pull the tag list so that we can check out by tag name
  git fetch --tags 1>&2 || exit 1
  git checkout $commitHash 1>&2 || exit 1

  popd
}

function build() {
  local outFile=$1
  local buildTests=$2

  mvn install package -DskipTests -Pbin-dist -D jdk.version=8 ${PINOT_MAVEN_OPTS} 1>${outFile} 2>&1
  if [ $? -ne 0 ]; then exit 1; fi
  mvn -pl pinot-tools package -DskipTests -Djdk.version=8 ${PINOT_MAVEN_OPTS} 1>>${outFile} 2>&1
  if [ $? -ne 0 ]; then exit 1; fi
  if [ $buildTests -eq 1 ]; then
    mvn -pl pinot-integration-tests package -DskipTests -Djdk.version=8 ${PINOT_MAVEN_OPTS} 1>>${outFile} 2>&1
    if [ $? -ne 0 ]; then exit 1; fi
  fi

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


# Validate and create working directory; set up build path names
if [ -z $workingDir ]; then
  echo "Working directory must be specified"
  usage
  exit 1
fi
if [ -d $workingDir ]; then
  echo "Directory ${workingDir} already exists. Use a new directory."
  usage
  exit 1
fi
mkdir -p ${workingDir}
if [ $? -ne 0 ]; then
  echo "Could not create ${workingDir}"
  exit 1
fi
workingDir=$(absPath "$workingDir")
cmdDir=$(absPath "$cmdDir")
oldBuildOutFile="${workingDir}/oldBuild.out"
oldTargetDir="$workingDir"/oldTargetDir
newBuildOutFile="${workingDir}/newBuild.out"
newTargetDir="$workingDir"/newTargetDir
curBuildOutFile="${workingDir}/currentBuild.out"

# Find value of olderCommit hash
if [ -z "$olderCommit" -a ! -z "$newerCommit" ]; then
   echo "If old commit hash is not given, then new commit should not be given either"
   usage
   exit 1
fi
if [ -z "$olderCommit" ]; then
  # Newer commit must also be null, so take the previous commit as the older one.
  olderCommit=`git log  --pretty=format:'%h' -n 2|tail -1`
fi

buildNewTarget=0

if [ -z "$newerCommit" ]; then
  echo "Assuming current tree as newer version"
  ln -s "${cmdDir}/.." "${newTargetDir}"
else
  if ! mkdir -p "$newTargetDir"; then
    echo "Failed to create target directory ${newTargetDir}"
    exit 1
  fi
  echo "Checking out new version at commit \"${newerCommit}\""
  checkOut "$newerCommit" "$newTargetDir"
  buildNewTarget=1
fi

if ! mkdir -p "$oldTargetDir"; then
  echo "Failed to create target directory ${oldTargetDir}"
  exit 1
fi

echo "Checking out old version at commit \"${olderCommit}\""
checkOut "$olderCommit" "$oldTargetDir"

# Start builds in parallel.
# We need to build current directory and old commit for sure.
# In addition, if the newer version is not the current one,
# we will need to build that as well.
echo Starting build for compat checker at ${cmdDir}
(cd ${cmdDir}/..; build ${curBuildOutFile} 1) &
curBuildPid=$!
echo Starting build for old version at ${oldTargetDir}
(cd ${oldTargetDir}; build ${oldBuildOutFile} 0) &
oldBuildPid=$!
if [ ${buildNewTarget} -eq 1 ]; then
  echo Starting build for new version at ${newTargetDir}
  (cd ${newTargetDir}; build ${newBuildOutFile} 0) &
  newBuildPid=$!
fi

echo Awaiting build complete for old commit
while true ; do
  printf "."
  ps -p ${oldBuildPid} 1>/dev/null 2>&1
  if [ $? -ne 0 ]; then
    printf "\n"
    wait ${oldBuildPid}
    oldBuildStatus=$?
    break
  fi
  sleep 5
done

echo Awaiting build complete for compat checker
while true ; do
  printf "."
  ps -p ${curBuildPid} 1>/dev/null 2>&1
  if [ $? -ne 0 ]; then
    printf "\n"
    wait ${curBuildPid}
    curBuildStatus=$?
    break
  fi
  sleep 5
done

if [ ${buildNewTarget} -eq 1 ]; then
  echo Awaiting build complete for new commit
  while true ; do
    printf "."
    ps -p ${newBuildPid} 1>/dev/null 2>&1
    if [ $? -ne 0 ]; then
      printf "\n"
      wait ${newBuildPid}
      newBuildStatus=$?
      break
    fi
    sleep 5
  done

fi

exitStatus=0

if [ ${oldBuildStatus} -eq 0 ]; then
  echo Old version build completed successfully
else
  echo Old version build failed. See ${oldBuildOutFile}
  exitStatus=1
fi

if [ ${curBuildStatus} -eq 0 ]; then
  echo Compat checker build completed successfully
else
  echo Compat checker build failed. See ${curBuildOutFile}
fi

if [ ${buildNewTarget} -eq 1 ]; then
  if [ ${newBuildStatus} -eq 0 ]; then
    echo New version build completed successfully
  else
    echo New version build failed. See ${newBuildOutFile}
    exitStatus=1
  fi
fi

exit 0
