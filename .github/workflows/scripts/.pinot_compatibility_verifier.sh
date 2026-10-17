#!/bin/bash -x
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

# Java version
java -version

# Check network
ifconfig
netstat -i

df -h

SETTINGS_FILE="../settings.xml"

echo "<settings xmlns=\"http://maven.apache.org/SETTINGS/1.0.0\""> ${SETTINGS_FILE}
echo "      xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"">> ${SETTINGS_FILE}
echo "      xsi:schemaLocation=\"http://maven.apache.org/SETTINGS/1.0.0">> ${SETTINGS_FILE}
echo "                          https://maven.apache.org/xsd/settings-1.0.0.xsd\">">> ${SETTINGS_FILE}
echo "  <mirrors>">> ${SETTINGS_FILE}
echo "    <mirror>">> ${SETTINGS_FILE}
echo "      <id>confluent-mirror</id>">> ${SETTINGS_FILE}
echo "      <mirrorOf>confluent</mirrorOf>">> ${SETTINGS_FILE}
echo "      <url>https://packages.confluent.io/maven/</url>">> ${SETTINGS_FILE}
echo "      <blocked>false</blocked>">> ${SETTINGS_FILE}
echo "    </mirror>">> ${SETTINGS_FILE}
echo "  </mirrors>">> ${SETTINGS_FILE}

echo "  <servers>">> ${SETTINGS_FILE}
echo "    <server>">> ${SETTINGS_FILE}
echo "      <id>central</id>">> ${SETTINGS_FILE}
echo "      <configuration>">> ${SETTINGS_FILE}
echo "        <httpConfiguration>">> ${SETTINGS_FILE}
echo "          <all>">> ${SETTINGS_FILE}
echo "            <connectionTimeout>120000</connectionTimeout>">> ${SETTINGS_FILE}
echo "            <readTimeout>120000</readTimeout>">> ${SETTINGS_FILE}
echo "            <retries>3</retries>">> ${SETTINGS_FILE}
echo "          </all>">> ${SETTINGS_FILE}
echo "        </httpConfiguration>">> ${SETTINGS_FILE}
echo "      </configuration>">> ${SETTINGS_FILE}
echo "    </server>">> ${SETTINGS_FILE}
echo "  </servers>">> ${SETTINGS_FILE}

echo "</settings>">> ${SETTINGS_FILE}

# PINOT_MAVEN_OPTS is used to provide additional maven options to the checkoutAndBuild.sh command
export PINOT_MAVEN_OPTS="${PINOT_MAVEN_OPTS:-} -s $(pwd)/${SETTINGS_FILE}"

# Compare commit hash for compatibility verification
git fetch --all --tags
function commitHash() {
  local commit=$1
  git log -1 --pretty=format:'%h' "${commit}" 2>/dev/null \
    || git log -1 --pretty=format:'%h' "origin/${commit}" 2>/dev/null \
    || git log -1 --pretty=format:'%h' "refs/tags/${commit}" 2>/dev/null
}

NEW_COMMIT_HASH=$(commitHash HEAD)
if [ ! -z "${NEW_COMMIT}" ]; then
  NEW_COMMIT_HASH=$(commitHash "${NEW_COMMIT}")
fi
OLD_COMMIT_HASH=$(commitHash "${OLD_COMMIT}")
if [ -z "${OLD_COMMIT_HASH}" ]; then
  echo "Failed to get commit hash for commit: \"${OLD_COMMIT}\""
  exit 1
fi
if [ -z "${NEW_COMMIT_HASH}" ]; then
  echo "Failed to get commit hash for commit: \"${NEW_COMMIT:-HEAD}\""
  exit 1
fi
if [ "${NEW_COMMIT_HASH}" == "${OLD_COMMIT_HASH}" ]; then
  echo "No changes between old commit: \"${OLD_COMMIT}\" and new commit: \"${NEW_COMMIT}\""
  exit 0
fi

if [ -z "${NEW_COMMIT}" ]; then
  echo "Running compatibility regression test against \"${OLD_COMMIT}\""
  compatibility-verifier/checkoutAndBuild.sh -w $WORKING_DIR -o $OLD_COMMIT
else
  echo "Running compatibility regression test against \"${OLD_COMMIT}\" and \"${NEW_COMMIT}\""
  compatibility-verifier/checkoutAndBuild.sh -w $WORKING_DIR -o $OLD_COMMIT -n $NEW_COMMIT
fi

compatibility-verifier/compCheck.sh -w $WORKING_DIR -t $TEST_SUITE
