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

# Print environment variables
printenv

# Check network
ifconfig
netstat -i

# Java version
java -version
jdk_version() {
  IFS='
'
  # remove \r for Cygwin
  lines=$(java -Xms32M -Xmx32M -version 2>&1 | tr '\r' '\n')
  for line in $lines; do
    if test -z $result && echo "$line" | grep -q 'version "'
    then
      ver=$(echo $line | sed -e 's/.*version "\(.*\)"\(.*\)/\1/; 1q')
      # on macOS, sed doesn't support '?'
      if case $ver in "1."*) true;; *) false;; esac;
      then
        result=$(echo $ver | sed -e 's/1\.\([0-9]*\)\(.*\)/\1/; 1q')
      else
        result=$(echo $ver | sed -e 's/\([0-9]*\)\(.*\)/\1/; 1q')
      fi
    fi
  done
  unset IFS
  echo "$result"
}
JAVA_VER="$(jdk_version)"

# Build
PASS=0
for i in $(seq 1 2)
do
  if [ "$JAVA_VER" -gt 11 ] ; then
    mvn clean install -B -Dmaven.test.skip=true -Pbin-dist -Dmaven.javadoc.skip=true -Djdk.version=11
  else
    mvn clean install -B -Dmaven.test.skip=true -Pbin-dist -Dmaven.javadoc.skip=true -Djdk.version=${JAVA_VER}
  fi
  if [ $? -eq 0 ]; then
    PASS=1
    break;
  fi
done
if [ "${PASS}" != 1 ]; then
    exit 1;
fi

# Quickstart
DIST_BIN_DIR=`ls -d build/`
cd "${DIST_BIN_DIR}"

# Test standalone pinot. Configure JAVA_OPTS for smaller memory, and don't use System.exit
export JAVA_OPTS="-Xms1G -Dlog4j2.configurationFile=conf/log4j2.xml"

bin/pinot-admin.sh StartZookeeper &
ZK_PID=$!
sleep 10
# Print the JVM settings
jps -lvm

bin/pinot-admin.sh StartServiceManager -bootstrapConfigPaths conf/pinot-controller.conf conf/pinot-broker.conf conf/pinot-server.conf conf/pinot-minion.conf&
PINOT_PID=$!
# Print the JVM settings
jps -lvm

# Wait for at most 6 minutes for all services up.
sleep 60
for i in $(seq 1 150)
do
  if [[ `curl localhost:9000/health` = "OK" ]]; then
    if [[ `curl localhost:8099/health` = "OK" ]]; then
      if [[ `curl localhost:8097/health` = "OK" ]]; then
        break
      fi
    fi
  fi
  sleep 2
done

# Add Table
bin/pinot-admin.sh AddTable -tableConfigFile examples/batch/baseballStats/baseballStats_offline_table_config.json -schemaFile examples/batch/baseballStats/baseballStats_schema.json -exec
if [ $? -ne 0 ]; then
  echo 'Failed to create table baseballStats.'
  exit 1
fi

# Ingest Data
d=`pwd`
INSERT_INTO_RES=`curl -X POST --header 'Content-Type: application/json'  -d "{\"sql\":\"INSERT INTO baseballStats FROM FILE '${d}/examples/batch/baseballStats/rawdata'\",\"trace\":false}" http://localhost:8099/query/sql`
if [ $? -ne 0 ]; then
  echo 'Failed to ingest data for table baseballStats.'
  exit 1
fi
PASS=0

# Wait for 10 Seconds for table to be set up, then query the total count.
sleep 10
for i in $(seq 1 150)
do
  QUERY_RES=`curl -X POST --header 'Accept: application/json'  -d '{"sql":"select count(*) from baseballStats limit 1","trace":false}' http://localhost:8099/query/sql`
  if [ $? -eq 0 ]; then
    COUNT_STAR_RES=`echo "${QUERY_RES}" | jq '.resultTable.rows[0][0]'`
    if [[ "${COUNT_STAR_RES}" =~ ^[0-9]+$ ]] && [ "${COUNT_STAR_RES}" -eq 97889 ]; then
      PASS=1
      break
    fi
  fi
  sleep 2
done

cleanup "${PINOT_PID}"
cleanup "${ZK_PID}"
if [ "${PASS}" -eq 0 ]; then
  echo 'Standalone test failed: Cannot get correct result for count star query.'
  exit 1
fi

# Test quick-start-batch
bin/quick-start-batch.sh &
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
  echo 'Batch Quickstart failed: Cannot get correct result for count star query.'
  exit 1
fi

# Test quick-start-streaming
# TODO: Streaming test is disabled because Meetup RSVP stream is retired. Find a replacement and re-enable this test.
#bin/quick-start-streaming.sh &
#PID=$!
#
#PASS=0
#RES_1=0
#
## Wait for 1 minute for table to be set up, then at most 5 minutes to reach the desired state
#sleep 60
#for i in $(seq 1 150)
#do
#  QUERY_RES=`curl -X POST --header 'Accept: application/json'  -d '{"sql":"select count(*) from meetupRsvp limit 1","trace":false}' http://localhost:8000/query/sql`
#  if [ $? -eq 0 ]; then
#    COUNT_STAR_RES=`echo "${QUERY_RES}" | jq '.resultTable.rows[0][0]'`
#    if [[ "${COUNT_STAR_RES}" =~ ^[0-9]+$ ]] && [ "${COUNT_STAR_RES}" -gt 0 ]; then
#      if [ "${RES_1}" -eq 0 ]; then
#        RES_1="${COUNT_STAR_RES}"
#        continue
#      elif [ "${COUNT_STAR_RES}" -gt "${RES_1}" ]; then
#        PASS=1
#        break
#      fi
#    fi
#  fi
#  sleep 2
#done
#
#cleanup "${PID}"
#if [ "${PASS}" -eq 0 ]; then
#  if [ "${RES_1}" -eq 0 ]; then
#    echo 'Streaming Quickstart test failed: Cannot get correct result for count star query.'
#    exit 1
#  fi
#  echo 'Streaming Quickstart test failed: Cannot get incremental counts for count star query.'
#  exit 1
#fi

# Test quick-start-hybrid
bin/quick-start-hybrid.sh &
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
