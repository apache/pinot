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

# Compare commit hash for compatibility verification
git fetch --all
echo "$OLD_COMMIT_HASH"
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

FILES_TO_CHECK=("pinot-spi/src/main/java/org/apache/pinot/spi/config/table/TableConfig.java" "pinot-spi/src/main/java/org/apache/pinot/spi/metrics/PinotMetricsRegistry.java")
len_arr="${#FILES_TO_CHECK[@]}"
javac -d pinot-spi-change-checker/target/classes pinot-spi-change-checker/src/main/java/org/apache/pinot/changecheck/GitDiffChecker.java

for ((i=0; i < len_arr; i++)); do
  DIFF=$(git diff "${OLD_COMMIT_HASH}".."${NEW_COMMIT_HASH}" "${FILES_TO_CHECK[i]}")
  #DIFF=$(git diff 2bc229738fad28ac625a905e4bb78c448717b12e..ad3117fe38380a8a5949da29a8ad2d0f2f7d3806 "${FILES_TO_CHECK[i]}")
  echo "$DIFF" > temp_diff_file.txt
  CONC=$(java -cp pinot-spi-change-checker/target/classes org.apache.pinot.changecheck.GitDiffChecker temp_diff_file.txt)
  rm temp_diff_file.txt
  if [[ "$CONC" != -1 ]]; then
    echo "Incorrect SPI change found in ${FILES_TO_CHECK[i]} at line $CONC"
    exit 1
  fi

rm -rf pinot-spi-change-checker/target/
echo "No incorrect SPI changes found!"
done