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
  # Terminate the process gracefully and wait up to 1 minute for it to exit
  kill "$1"
  timeout=60  # Max wait time in seconds

  while ((timeout > 0)); do
    if kill -0 "$1" 2>/dev/null; then
      sleep 1  # Process still running, wait for 1 second
      ((timeout--))
    else
      break  # Process exited successfully
    fi
  done

  # If the process is still running, kill it forcefully
  if kill -0 "$1" 2>/dev/null; then
    echo "Process $1 did not terminate within 60 seconds. Killing it forcefully."
    kill -9 "$1"
  fi

  # Delete ZK directory
  rm -rf '/tmp/PinotAdmin/zkData'
  rm -rf '/tmp/pinot/data'
}

# Polls a query until its response satisfies the given jq filter, for at most 5 minutes.
# Usage: wait_for_query <broker-port> <sql> <jq-filter> <description>
wait_for_query () {
  local PORT="$1"
  local SQL="$2"
  local FILTER="$3"
  local DESC="$4"
  local BODY
  local QUERY_RES
  BODY=$(jq -n --arg sql "${SQL}" '{sql: $sql, trace: false}')
  for i in $(seq 1 150)
  do
    QUERY_RES=$(curl -s --max-time 30 -X POST --header 'Accept: application/json' \
      --header 'Content-Type: application/json' -d "${BODY}" "http://localhost:${PORT}/query/sql")
    if [ -n "${QUERY_RES}" ] && echo "${QUERY_RES}" | jq -e "${FILTER}" > /dev/null 2>&1; then
      echo "Query check passed: ${DESC}"
      return 0
    fi
    sleep 2
  done
  echo "Query check FAILED: ${DESC}"
  echo "Query        : ${SQL}"
  echo "Last response: ${QUERY_RES}"
  return 1
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
echo "Building Pinot Using JDK ${JAVA_VER}"
PASS=0
for i in $(seq 1 2)
do
  mvn clean install -B -ntp -T1C -DskipTests -Pbin-dist -Dmaven.javadoc.skip=true
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

bin/pinot-admin.sh AddTable -tableConfigFile examples/batch/dimBaseballTeams/dimBaseballTeams_offline_table_config.json -schemaFile examples/batch/dimBaseballTeams/dimBaseballTeams_schema.json -exec
if [ $? -ne 0 ]; then
  echo 'Failed to create table dimBaseballTeams.'
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


INSERT_INTO_RES=`curl -X POST --header 'Content-Type: application/json'  -d "{\"sql\":\"INSERT INTO dimBaseballTeams FROM FILE '${d}/examples/batch/dimBaseballTeams/rawdata'\",\"trace\":false}" http://localhost:8099/query/sql`
if [ $? -ne 0 ]; then
  echo 'Failed to ingest data for table baseballStats.'
  exit 1
fi
PASS=0

# Wait for 10 Seconds for table to be set up, then query the total count.
sleep 10
# Validate V1 query count(*) result
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

PASS=0

# Validate V2 query count(*) result
for i in $(seq 1 150)
do
  QUERY_RES=`curl -X POST --header 'Accept: application/json'  -d '{"sql":"SET useMultistageEngine=true; select count(*) from baseballStats limit 1","trace":false}' http://localhost:8099/query/sql`
  if [ $? -eq 0 ]; then
    COUNT_STAR_RES=`echo "${QUERY_RES}" | jq '.resultTable.rows[0][0]'`
    if [[ "${COUNT_STAR_RES}" =~ ^[0-9]+$ ]] && [ "${COUNT_STAR_RES}" -eq 97889 ]; then
      PASS=1
      break
    fi
  fi
  sleep 2
done

PASS=0

# Validate V2 join query results
for i in $(seq 1 150)
do
  QUERY_RES=`curl -X POST --header 'Accept: application/json'  -d '{"sql":"SET useMultistageEngine=true;SELECT a.playerName, a.teamID, b.teamName FROM baseballStats_OFFLINE AS a JOIN dimBaseballTeams_OFFLINE AS b ON a.teamID = b.teamID LIMIT 10","trace":false}' http://localhost:8099/query/sql`
  if [ $? -eq 0 ]; then
    RES_0=`echo "${QUERY_RES}" | jq '.resultTable.rows[0][0]'`
    if [[ "${RES_0}" = "\"David Allan\"" ]]; then
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

# Test quick-start-batch. The batch quickstart absorbed the MULTI_STAGE, JOIN, TIMESTAMP, JSON_INDEX and
# COMPLEX_TYPE quickstarts, so one cluster now covers every batch feature they used to demo individually.
bin/pinot-admin.sh QuickStart -type BATCH &
PID=$!

# Print the JVM settings
jps -lvm

PASS=1

# Wait for 1 minute for the tables to be set up; each check below then polls for at most 5 minutes.
sleep 60

# Single-stage engine. This doubles as the liveness check: if the cluster never comes up there is no point
# spending the full polling budget on the seven checks after it, so bail out immediately.
if ! wait_for_query 8000 \
  'select count(*) from baseballStats limit 1' \
  '.resultTable.rows[0][0] == 97889' \
  'single-stage count(*) on baseballStats'
then
  cleanup "${PID}"
  echo 'Batch Quickstart failed: baseballStats never reached the expected row count.'
  exit 1
fi

# Multi-stage engine
wait_for_query 8000 \
  'SET useMultistageEngine=true; select count(*) from baseballStats limit 1' \
  '.resultTable.rows[0][0] == 97889' \
  'multi-stage count(*) on baseballStats' || PASS=0

# Ordered so the assertion does not depend on how segments are spread across the 3 servers. Every row must have
# resolved a team name, otherwise the join produced rows without matching the dimension table.
wait_for_query 8000 \
  'SET useMultistageEngine=true; SELECT a.playerName, a.teamID, b.teamName FROM baseballStats_OFFLINE AS a JOIN dimBaseballTeams_OFFLINE AS b ON a.teamID = b.teamID ORDER BY a.playerName, a.teamID LIMIT 10' \
  '(.exceptions | length) == 0 and (.resultTable.rows | length) == 10 and (all(.resultTable.rows[]; .[2] != null and .[2] != ""))' \
  'multi-stage join between baseballStats and dimBaseballTeams' || PASS=0

wait_for_query 8000 \
  'SET useMultistageEngine=true; SELECT a.playerID, a.runs, a.yearID, b.runs, b.yearID FROM baseballStats_OFFLINE AS a JOIN baseballStats_OFFLINE AS b ON a.playerID = b.playerID WHERE a.runs > 160 AND b.runs < 2 LIMIT 10' \
  '(.exceptions | length) == 0 and (.resultTable.rows | length) > 0 and (all(.resultTable.rows[]; .[1] > 160 and .[3] < 2))' \
  'multi-stage self join on baseballStats' || PASS=0

# Star schema benchmark. These tables ingest through their ingestionJobSpec.yaml rather than a minion task, so
# assert on a non-zero row count: a mis-named or broken spec creates the table successfully but leaves it empty.
wait_for_query 8000 \
  'SET useMultistageEngine=true; select count(*) from lineorder' \
  '(.exceptions | length) == 0 and .resultTable.rows[0][0] > 0' \
  'SSB lineorder is populated' || PASS=0

wait_for_query 8000 \
  'SET useMultistageEngine=true; select c.C_NATION, sum(lo.LO_REVENUE) as revenue from lineorder lo join customer c on lo.LO_CUSTKEY = c.C_CUSTKEY group by c.C_NATION order by revenue desc limit 10' \
  '(.exceptions | length) == 0 and (.resultTable.rows | length) > 0 and (all(.resultTable.rows[]; .[0] != null and .[1] > 0))' \
  'SSB star-schema join between lineorder and customer' || PASS=0

# Lookup join (formerly the JOIN quickstart). lookup() returns a row per fact row whether or not the dimension
# table resolved, so assert that every returned value actually resolved to a team name.
wait_for_query 8000 \
  "select playerName, teamID, lookup('dimBaseballTeams', 'teamName', 'teamID', teamID) from baseballStats where teamID = 'BOS' order by playerName limit 10" \
  '(.exceptions | length) == 0 and (.resultTable.rows | length) == 10 and (all(.resultTable.rows[]; .[2] != null and .[2] != ""))' \
  'lookup() join against dimBaseballTeams' || PASS=0

# JSON index (formerly the BATCH_JSON_INDEX quickstart)
wait_for_query 8000 \
  $'select json_extract_scalar(repo, \'$.name\', \'STRING\'), count(*) from githubEvents where json_match(actor, \'"$.login"=\'\'LombiqBot\'\'\') group by 1 order by 2 desc limit 10' \
  '(.exceptions | length) == 0 and (.resultTable.rows | length) > 0' \
  'json_match on githubEvents' || PASS=0

# Complex type handling (formerly the BATCH_COMPLEX_TYPE quickstart)
wait_for_query 8000 \
  'select id, "payload.commits.author.name", "payload.commits.author.email" from githubComplexTypeEvents limit 10' \
  '(.exceptions | length) == 0 and (.resultTable.rows | length) == 10' \
  'flattened complex type columns on githubComplexTypeEvents' || PASS=0

# Timestamp index (formerly the TIMESTAMP quickstart)
wait_for_query 8000 \
  'select ts, $ts$DAY, $ts$WEEK, $ts$MONTH from airlineStats limit 1' \
  '(.exceptions | length) == 0 and (.resultTable.rows | length) == 1' \
  'timestamp index generated columns on airlineStats' || PASS=0

cleanup "${PID}"
if [ "${PASS}" -eq 0 ]; then
  echo 'Batch Quickstart failed: see the query check failures above.'
  exit 1
fi

# Test quick-start-streaming
bin/quick-start-streaming.sh &
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
