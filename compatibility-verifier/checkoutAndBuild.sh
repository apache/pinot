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
  echo -e "    Additional maven build options can be passed in via environment varibale PINOT_MAVEN_OPTS"
  exit 1
}

# This function builds Pinot given a specific commit hash and target directory
function checkoutAndBuild() {
  commitHash=$1
  targetDir=$2

  pushd "$targetDir" || exit 1
  git init || exit 1
  git remote add origin https://github.com/apache/incubator-pinot || exit 1
  git pull origin master || exit 1
  # Pull the tag list so that we can check out by tag name
  git fetch --tags || exit 1
  git checkout $commitHash || exit 1
  mvn install package -DskipTests -Pbin-dist -T 4 -Djdk.version=8 ${PINOT_MAVEN_OPTS} || exit 1
  popd || exit 1
  exit 0
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

if [ -z "$olderCommit" -a ! -z "$newerCommit" ]; then
   echo "If old commit hash is not given, then new commit has should not be given either"
   usage
   exit 1
fi

if [ -z "$olderCommit" ]; then
  # Newer commit must also be null, so take the previous commit as the older one.
  olderCommit=`git log  --pretty=format:'%h' -n 2|tail -1`
fi

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

newTargetDir="$workingDir"/newTargetDir
if [ -z "$newerCommit" ]; then
  echo "Compiling current tree as newer version"
  (cd $cmdDir/.. && mvn install package -DskipTests -Pbin-dist -T 4 -D jdk.version=8 ${PINOT_MAVEN_OPTS} && mvn -pl pinot-tools package -T 4 -DskipTests -Djdk.version=8 ${PINOT_MAVEN_OPTS} && mvn -pl pinot-integration-tests package -T 4 -DskipTests -Djdk.version=8 ${PINOT_MAVEN_OPTS})
  if [ $? -ne 0 ]; then
    echo Compile failed.
    exit 1
  fi
  ln -s $(absPath "${cmdDir}/..") "${newTargetDir}"
else
  if ! mkdir -p "$newTargetDir"; then
    echo "Failed to create target directory ${newTargetDir}"
    exit 1
  fi
  echo "Building the new version ..."
  checkoutAndBuild "$newerCommit" "$newTargetDir"
  if [ $? -ne 0 ];then
    echo Could not build new version at "${newerCommit}"
    exit 1
  fi
fi

oldTargetDir="$workingDir"/oldTargetDir
if ! mkdir -p "$oldTargetDir"; then
  echo "Failed to create target directory ${oldTargetDir}"
  exit 1
fi
echo "Building the old version ... "
checkoutAndBuild "$olderCommit" "$oldTargetDir"
if [ $? -ne 0 ]; then
  echo Could not build older version at "${olderCommit}"
  exit 1
fi
exit 0
