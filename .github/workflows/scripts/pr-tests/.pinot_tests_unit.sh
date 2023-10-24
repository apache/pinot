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

# Unit Tests
#   - TEST_SET#1 runs install and test together so the module list must ensure no additional modules were tested
#     due to the -am flag (include dependency modules)
if [ "$RUN_TEST_SET" == "1" ]; then
  mvn test -T 16 \
      -pl 'pinot-spi' \
      -pl 'pinot-segment-spi' \
      -pl 'pinot-common' \
      -pl 'pinot-core' \
      -pl 'pinot-query-planner' \
      -pl 'pinot-query-runtime' \
      -P github-actions,no-integration-tests || exit 1
fi
if [ "$RUN_TEST_SET" == "2" ]; then
  mvn test \
    -pl '!pinot-spi' \
    -pl '!pinot-segment-spi' \
    -pl '!pinot-common' \
    -pl '!pinot-core' \
    -pl '!pinot-query-planner' \
    -pl '!pinot-query-runtime' \
    -P github-actions,no-integration-tests || exit 1
fi
