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
MVN_CACHE_DIR="mvn-compat-cache"
df -h
du -hd2 /home/runner

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

# This function clines pinot code from the apache repo
# given a specific commit hash and target directory
function checkOut() {
  local commitHash=$1
  local targetDir=$2

  pushd "$targetDir"  1>&2 || exit 1
  git init 1>&2 || exit 1
  git remote add origin https://github.com/apache/pinot 1>&2 || exit 1
  git pull origin master 1>&2 || exit 1
  # Pull the tag list so that we can check out by tag name
  git fetch --tags 1>&2 || exit 1
  # Return different error code if old version is not on master
  git checkout $commitHash 1>&2 || exit 22 # invalid argument error code

  popd
}

# This function builds pinot code and tools necessary to run pinot-admin commands
# Optionally, it also builds the integration test jars needed to run the pinot
# compatibility tester
# It uses a different version for each build, since we build trees in parallel.
# Building the same version on all trees in parallel causes unstable builds.
# Using independent buildIds will cause maven cache to fill up, so we use a
# dedicated maven cache for these builds, and remove the cache after build is
# completed.
# If buildId is less than 0, then the mvn version is not changed in pom files.
function build() {
  local outFile=$1
  local buildTests=$2
  local buildId=$3
  local buildCompatibilityVerifier=$4
  local repoOption=""
  local versionOption="-Djdk.version=11"
  local maxRetry=5

  mkdir -p ${MVN_CACHE_DIR}
  mkdir -p ${mvnCache}

  if [ ${buildId} -gt 0 ]; then
    # Build it in a different env under different version so that maven cache does
    # not collide
    local pomVersion=$(grep -E "<version>(.*)-SNAPSHOT</version>" pom.xml | cut -d'>' -f2 | cut -d'<' -f1 | cut -d'-' -f1)
    mvn versions:set -DnewVersion="${pomVersion}-compat-${buildId}" -q -B 1>${outFile} 2>&1
    mvn versions:commit -q -B 1>${outFile} 2>&1
    repoOption="-Dmaven.repo.local=${mvnCache}/${buildId}"
  fi
  buildComponents=":pinot-tools"
  if [ $buildCompatibilityVerifier -gt 0 ]; then
    buildComponents=":pinot-tools,:pinot-compatibility-verifier"
  fi
  for i in $(seq 1 $maxRetry); do
    mvn clean package -am -pl ${buildComponents} -DskipTests -T1C ${versionOption} ${repoOption} ${PINOT_MAVEN_OPTS} 1>${outFile} 2>&1
    if [ $? -eq 0 ]; then break; fi
    if [ $i -eq $maxRetry ]; then exit 1; fi
    echo ""
    echo "Build failed, see lamast 1000 lines of output below."
    tail -1000 ${outFile}
    echo "Retrying after 30 seconds..."
    sleep 30
  done
  if [ $buildTests -eq 1 ]; then
    for i in $(seq 1 $maxRetry); do
      mvn package -am -pl :pinot-integration-tests -DskipTests -T1C ${versionOption} ${repoOption} ${PINOT_MAVEN_OPTS} 1>>${outFile} 2>&1
      if [ $? -eq 0 ]; then break; fi
      if [ $i -eq $maxRetry ]; then exit 1; fi
      echo ""
      echo "Build failed, see last 500 lines of output below."
      tail -500 ${outFile}
      echo "Retrying after 30 seconds..."
      sleep 30
    done
  fi
  # DELETE the mvn cache
  du -hd1 ${mvnCache}
  df -h
  rm -rf ${mvnCache}
  du -hd2 /home/runner
  df -h
  rm -rf /home/runner/.m2/repository
  df -h
}

#
# Main
#

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
mvnCache=${workingDir}/${MVN_CACHE_DIR}
mkdir -p ${mvnCache}

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

exitStatus=0

# Start builds in sequential.
# First build the current tree. We need it so that we can
# run the compatibility tester.
echo Starting build for compat checker at ${cmdDir}, buildId none.
(cd ${cmdDir}/..; build ${curBuildOutFile} 1 -1 1) &
curBuildPid=$!

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

if [ ${curBuildStatus} -eq 0 ]; then
  echo Compat checker build completed successfully
else
  echo Compat checker build failed. See ${curBuildOutFile}
  echo ======== Build output ========
  cat ${curBuildOutFile}
  echo ==== End Build output ========
  exitStatus=1
  /bin/rm -r ${mvnCache}
  exit ${exitStatus}
fi

# The old commit has been cloned in oldTargetDir, build it.
buildId=$(date +%s)
echo Starting build for old version at ${oldTargetDir} buildId ${buildId}
(cd ${oldTargetDir}; build ${oldBuildOutFile} 0 ${buildId} 0) &
oldBuildPid=$!

# We may have potentially started three builds above (at least two).
# Wait for each of them to complete.
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

if [ ${oldBuildStatus} -eq 0 ]; then
  echo Old version build completed successfully
else
  echo Old version build failed. See ${oldBuildOutFile}
  echo ======== Build output ========
  cat ${oldBuildOutFile}
  echo ==== End Build output ========
  exitStatus=1
  /bin/rm -r ${mvnCache}
  exit ${exitStatus}
fi

# In case the user specified the current tree as newer commit, then
# We don't need to build newer commit tree (we have already built the
# current tree above and linked newTargetDir). Otherwise, build the newTargetDir
if [ ${buildNewTarget} -eq 1 ]; then
  buildId=$((buildId+1))
  echo Starting build for new version at ${newTargetDir} buildId ${buildId}
  (cd ${newTargetDir}; build ${newBuildOutFile} 0 ${buildId} 0) &
  newBuildPid=$!
fi

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

if [ ${buildNewTarget} -eq 1 ]; then
  if [ ${newBuildStatus} -eq 0 ]; then
    echo New version build completed successfully
  else
    echo New version build failed. See ${newBuildOutFile}
    echo ======== Build output ========
    cat ${newBuildOutFile}
    echo ==== End Build output ========
    exitStatus=1
    exit ${exitStatus}
  fi
fi

/bin/rm -r ${mvnCache}

exit ${exitStatus}
