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
mvn clean install -DskipTests -T 16 || exit 1

TEST_ARG=""
if [ "$STRESS_TEST_TARGET" != "SKIP" ]; then
  TEST_ARG="-Dtest=$STRESS_TEST_TARGET"
fi
it=0
while [ $it -ne 20 ]
do
  it=$(($it+1))
  mvn test -pl $STRESS_TEST_MODULE $TEST_ARG -B -P github-actions,no-integration-tests || exit 1
done
mvn clean > /dev/null
