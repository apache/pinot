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

print_surefire_dumps() {
  local reports_dir="target/surefire-reports"
  local dump_files

  if [ ! -d "${reports_dir}" ]; then
    echo "Surefire reports directory not found: ${reports_dir}"
    return
  fi

  dump_files="$(find "${reports_dir}" -maxdepth 1 -type f \
    \( -name "*.dump" -o -name "*.dumpstream" -o -name "*jvmRun*" \) | sort)"
  if [ -z "${dump_files}" ]; then
    echo "No Surefire dump files found under ${reports_dir}"
    return
  fi

  echo "Printing Surefire dump files from ${reports_dir}"
  while IFS= read -r dump_file; do
    [ -z "${dump_file}" ] && continue
    echo "===== BEGIN ${dump_file} ====="
    cat "${dump_file}"
    echo "===== END ${dump_file} ====="
  done <<< "${dump_files}"
}

# Integration Tests
cd pinot-integration-tests || exit 1
if [ "$RUN_TEST_SET" == "1" ]; then
  mvn test \
      -P github-actions,codecoverage,integration-tests-set-1 || {
    print_surefire_dumps
    exit 1
  }
  exit 0
fi
if [ "$RUN_TEST_SET" == "2" ]; then
  mvn test \
      -DargLine="-Xms1g -Xmx2g -Dlog4j2.configurationFile=log4j2.xml" \
      -P github-actions,codecoverage,integration-tests-set-2 || {
    print_surefire_dumps
    exit 1
  }
  exit 0
fi

echo "Unsupported RUN_TEST_SET value: ${RUN_TEST_SET}"
exit 1
