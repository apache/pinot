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

echo "<settings xmlns=\"http://maven.apache.org/SETTINGS/1.0.0\""> ../settings.xml
echo "      xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"">> ../settings.xml
echo "      xsi:schemaLocation=\"http://maven.apache.org/SETTINGS/1.0.0">> ../settings.xml
echo "                          https://maven.apache.org/xsd/settings-1.0.0.xsd\">">> ../settings.xml
echo "  <mirrors>">> ../settings.xml
echo "    <mirror>">> ../settings.xml
echo "      <id>confluent-mirror</id>">> ../settings.xml
echo "      <mirrorOf>confluent</mirrorOf>">> ../settings.xml
echo "      <url>https://packages.confluent.io/maven/</url>">> ../settings.xml
echo "      <blocked>false</blocked>">> ../settings.xml
echo "    </mirror>">> ../settings.xml
echo "  </mirrors>">> ../settings.xml
echo "</settings>">> ../settings.xml

export PINOT_MAVEN_OPTS="-s $(pwd)/settings.xml"
compatibility-verifier/checkoutAndBuild.sh -w $WORKING_DIR -o $OLD_COMMIT

compatibility-verifier/compCheck.sh -w $WORKING_DIR -t $TEST_SUITE
