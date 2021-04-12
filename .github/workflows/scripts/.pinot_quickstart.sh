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

# Print environment variables
printenv

# Java version
java -version

# Build
PASS=0
for i in $(seq 1 5)
do
  mvn clean install -B -DskipTests=true -Pbin-dist -Dmaven.javadoc.skip=true
  if [ $? -eq 0 ]; then
    PASS=1
    break;
  fi
done
if [ "${PASS}" != 1 ]; then
    exit 1;
fi

# Quickstart
DIST_BIN_DIR=`ls -d pinot-distribution/target/apache-pinot-*/apache-pinot-*`/bin
cd "${DIST_BIN_DIR}"

# Test quick-start-batch
./quick-start-batch.sh &
PID=$!

# Print the JVM settings
jps -lvm

PASS=0

# Wait for 1 minute for table to be set up, then at most 5 minutes to reach the desired state
sleep 60
for i in $(seq 1 150)
do
  QUERY_RES=`curl -X POST --header 'Accept: application/json'  -d '{"sql":"select count(*) from baseballStats limit 1","trace":false}' http://localhost:8000/query/sql`
  if [ $? -eq 0 ]; then
    COUNT_STAR_RES=`echo "${QUERY_RES}" | jq '.resultTable.rows[0][0]'`
    if [[ "${COUNT_STAR_RES}" =~ ^[0-9]+$ ]] && [ "${COUNT_STAR_RES}" -eq 97889 ]; then
      PASS=1
      break
    fi
  fi
  sleep 2
done

cleanup () {
  # Terminate the process and wait for the clean up to be done
  kill "$1"
  while true;
  do
    kill -0 "$1" && sleep 1 || break
  done

  # Delete ZK directory
  rm -rf '/tmp/PinotAdmin/zkData'
}

cleanup "${PID}"
if [ "${PASS}" -eq 0 ]; then
  echo 'Batch Quickstart failed: Cannot get correct result for count star query.'
  exit 1
fi

# Test quick-start-batch-with-minion
./quick-start-batch-with-minion.sh &
PID=$!

# Print the JVM settings
jps -lvm

PASS=0

# Wait for 1 minute for table to be set up, then at most 5 minutes to reach the desired state
sleep 60
for i in $(seq 1 150)
do
  QUERY_RES=`curl -X POST --header 'Accept: application/json'  -d '{"sql":"select count(*) from baseballStats limit 1","trace":false}' http://localhost:8000/query/sql`
  if [ $? -eq 0 ]; then
    COUNT_STAR_RES=`echo "${QUERY_RES}" | jq '.resultTable.rows[0][0]'`
    if [[ "${COUNT_STAR_RES}" =~ ^[0-9]+$ ]] && [ "${COUNT_STAR_RES}" -eq 97889 ]; then
      PASS=1
      break
    fi
  fi
  sleep 2
done

cleanup "${PID}"
if [ "${PASS}" -eq 0 ]; then
  echo 'Batch Quickstart with Minion failed: Cannot get correct result for count star query.'
  exit 1
fi

# Test quick-start-streaming
./quick-start-streaming.sh &
PID=$!

PASS=0
RES_1=0

# Wait for 1 minute for table to be set up, then at most 5 minutes to reach the desired state
sleep 60
for i in $(seq 1 150)
do
  QUERY_RES=`curl -X POST --header 'Accept: application/json'  -d '{"sql":"select count(*) from meetupRsvp limit 1","trace":false}' http://localhost:8000/query/sql`
  if [ $? -eq 0 ]; then
    COUNT_STAR_RES=`echo "${QUERY_RES}" | jq '.resultTable.rows[0][0]'`
    if [[ "${COUNT_STAR_RES}" =~ ^[0-9]+$ ]] && [ "${COUNT_STAR_RES}" -gt 0 ]; then
      if [ "${RES_1}" -eq 0 ]; then
        RES_1="${COUNT_STAR_RES}"
        continue
      elif [ "${COUNT_STAR_RES}" -gt "${RES_1}" ]; then
        PASS=1
        break
      fi
    fi
  fi
  sleep 2
done

cleanup "${PID}"
if [ "${PASS}" -eq 0 ]; then
  if [ "${RES_1}" -eq 0 ]; then
    echo 'Streaming Quickstart test failed: Cannot get correct result for count star query.'
    exit 1
  fi
  echo 'Streaming Quickstart test failed: Cannot get incremental counts for count star query.'
  exit 1
fi

# Test quick-start-hybrid
./quick-start-hybrid.sh &
PID=$!

# Print the JVM settings
jps -lvm

PASS=0
RES_1=0

# Wait for 1 minute for table to be set up, then at most 5 minutes to reach the desired state
sleep 60
for i in $(seq 1 150)
do
  QUERY_RES=`curl -X POST --header 'Accept: application/json'  -d '{"sql":"select count(*) from airlineStats limit 1","trace":false}' http://localhost:8000/query/sql`
  if [ $? -eq 0 ]; then
    COUNT_STAR_RES=`echo "${QUERY_RES}" | jq '.resultTable.rows[0][0]'`
    if [[ "${COUNT_STAR_RES}" =~ ^[0-9]+$ ]] && [ "${COUNT_STAR_RES}" -gt 0 ]; then
      if [ "${RES_1}" -eq 0 ]; then
        RES_1="${COUNT_STAR_RES}"
        continue
      elif [ "${COUNT_STAR_RES}" -gt "${RES_1}" ]; then
        PASS=1
        break
      fi
    fi
  fi
  sleep 2
done

cleanup "${PID}"
if [ "${PASS}" -eq 0 ]; then
  if [ "${RES_1}" -eq 0 ]; then
    echo 'Hybrid Quickstart test failed: Cannot get correct result for count star query.'
    exit 1
  fi
  echo 'Hybrid Quickstart test failed: Cannot get incremental counts for count star query.'
  exit 1
fi

cd ../../../../../
pwd
mvn clean > /dev/null

exit 0
