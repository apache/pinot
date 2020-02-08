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

# ThirdEye related changes
git diff --name-only "${TRAVIS_COMMIT_RANGE}" | egrep '^(thirdeye)'
if [ $? -eq 0 ]; then
  echo 'Skip ThirdEye tests for Quickstart'
  rm -rf ~/.m2/repository/com/linkedin/pinot ~/.m2/repository/com/linkedin/thirdeye
  exit 0
fi

# Remove Pinot files from local Maven repository to avoid a useless cache rebuild
rm -rf ~/.m2/repository/com/linkedin/pinot

# Quickstart
DIST_BIN_DIR=`ls -d pinot-distribution/target/apache-pinot-*/apache-pinot-*`
cd ${DIST_BIN_DIR}

# Test quick-start-batch
bin/quick-start-batch.sh &
PID=$!
sleep 60
COUNT_STAR_RES=`curl -X POST --header 'Accept: application/json'  -d '{"sql":"select count(*) from baseballStats limit 1","trace":false}' http://localhost:8000/query/sql | jq '.resultTable.rows[0][0]'`
if [ "${COUNT_STAR_RES}" -ne 97889 ]; then
  echo 'Batch Quickstart: Incorrect result for count star query.'
  exit 1
fi
kill $PID
sleep 30

# Test quick-start-streaming
bin/quick-start-streaming.sh &
PID=$!
sleep 60
COUNT_STAR_RES_1=`curl -X POST --header 'Accept: application/json'  -d '{"sql":"select count(*) from meetupRsvp limit 1","trace":false}' http://localhost:8000/query/sql | jq '.resultTable.rows[0][0]'`
sleep 15
COUNT_STAR_RES_2=`curl -X POST --header 'Accept: application/json'  -d '{"sql":"select count(*) from meetupRsvp limit 1","trace":false}' http://localhost:8000/query/sql | jq '.resultTable.rows[0][0]'`
sleep 15
COUNT_STAR_RES_3=`curl -X POST --header 'Accept: application/json'  -d '{"sql":"select count(*) from meetupRsvp limit 1","trace":false}' http://localhost:8000/query/sql | jq '.resultTable.rows[0][0]'`
if [ "${COUNT_STAR_RES_3}" -le "${COUNT_STAR_RES_2}" ] || [ "${COUNT_STAR_RES_2}" -le "${COUNT_STAR_RES_1}" ]; then
  echo 'Streaming Quickstart: Not getting incremental counts for 3 consecutive count star queries with 15 seconds interval.'
  exit 1
fi
kill $PID
sleep 30

# Test quick-start-hybrid
bin/quick-start-hybrid.sh &
PID=$!
sleep 60
COUNT_STAR_RES_1=`curl -X POST --header 'Accept: application/json'  -d '{"sql":"select count(*) from airlineStats limit 1","trace":false}' http://localhost:8000/query/sql | jq '.resultTable.rows[0][0]'`
sleep 15
COUNT_STAR_RES_2=`curl -X POST --header 'Accept: application/json'  -d '{"sql":"select count(*) from airlineStats limit 1","trace":false}' http://localhost:8000/query/sql | jq '.resultTable.rows[0][0]'`
sleep 15
COUNT_STAR_RES_3=`curl -X POST --header 'Accept: application/json'  -d '{"sql":"select count(*) from airlineStats limit 1","trace":false}' http://localhost:8000/query/sql | jq '.resultTable.rows[0][0]'`
if [ "${COUNT_STAR_RES_3}" -le "${COUNT_STAR_RES_2}" ] || [ "${COUNT_STAR_RES_2}" -le "${COUNT_STAR_RES_1}" ]; then
  echo 'Hybrid Quickstart: Not getting incremental counts for 3 consecutive count star queries with 15 seconds interval.'
  exit 1
fi
# kill $PID
# sleep 30

exit 0
