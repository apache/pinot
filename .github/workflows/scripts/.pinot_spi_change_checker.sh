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
export PINOT_MAVEN_OPTS="-s $(pwd)/${SETTINGS_FILE}"

# Compare commit hash for compatibility verification
git fetch --all
OLD_COMMIT_HASH=$(git log -1 --pretty=format:'%h' "${OLD_COMMIT}")
if [ $? -ne 0 ]; then
  echo "Failed to get commit hash for commit: \"${OLD_COMMIT}\""
  OLD_COMMIT_HASH=$(git log -1 --pretty=format:'%h' origin/"${OLD_COMMIT}")
fi
NEW_COMMIT_HASH=$(git log -1 --pretty=format:'%h' HEAD) # TODO: consider removing this
if [ $? -ne 0 ]; then
  echo "Failed to get commit hash for commit: \"${NEW_COMMIT}\""
  NEW_COMMIT_HASH=$(git log -1 --pretty=format:'%h' origin/"${NEW_COMMIT}")
fi
if [ "${NEW_COMMIT_HASH}" == "${OLD_COMMIT_HASH}" ]; then
  echo "No changes between old commit: \"${OLD_COMMIT}\" and new commit: \"${NEW_COMMIT}\""
  exit 0
fi

FILES_TO_CHECK=("pinot/pinot-spi/src/main/java/org/apache/pinot/spi/config/table
                 /TableConfig.java")
len_arr="${#FILES_TO_CHECK[@]}"

for ((i=0; i < len_arr; i++)); do
  DIFF=`git diff "${OLD_COMMIT_HASH}".."${NEW_COMMIT_HASH}" "${FILES_TO_CHECK[i]}"`
  echo "$DIFF" > temp_diff_file.txt
  CONC=$(java git_diff_checker.java temp_diff_file.txt)
  if [[ "$CONC" == "true" ]]; then
    echo "unwanted change found"
    exit 1
  fi
  rm temp_diff_file.txt
done