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
FILES_TO_CHECK=("pinot-spi/src/main/java/org/apache/pinot/spi/metrics/PinotMetricsRegistry.java" "pinot-spi/src/main/java/org/apache/pinot/spi/config/table/TableConfig.java")
len_arr="${#FILES_TO_CHECK[@]}"
javac -d pinot-spi-change-checker/target/classes pinot-spi-change-checker/src/main/java/org/apache/pinot/changecheck/GitDiffChecker.java

for ((i=0; i < len_arr; i++)); do
  DIFF=$(git diff origin/master -- "${FILES_TO_CHECK[i]}")
  echo "$DIFF" > pinot-spi-change-checker/temp_diff_file.txt
  CONC=$(java -cp pinot-spi-change-checker/target/classes org.apache.pinot.changecheck.GitDiffChecker pinot-spi-change-checker/temp_diff_file.txt)
  rm pinot-spi-change-checker/temp_diff_file.txt
  if [[ "$CONC" != "0" ]]; then
    echo "Incorrect SPI change found in ${FILES_TO_CHECK[i]} at this line in the original file: '$CONC'."
    exit 1
  fi
done

rm -rf pinot-spi-change-checker/target/
echo "No incorrect SPI changes found!"