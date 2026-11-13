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

# Resolve this before compCheck.sh changes directory to a distribution's bin directory.
COMPAT_SERVER_LOG4J_CONFIG="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/server-log4j2.xml"

# Shared by compCheck.sh and the readiness regression tests. Each server start gets its own
# log directory, including both servers in the MSE suite.
function startServerWithLogs() {
  local serviceName=$1
  local launcher=$2
  local configFile=$3
  local serverLogDir="${LOG_DIR}/${serviceName}.${logCount}"
  mkdir -p "$serverLogDir" || exit 1
  echo "$serverLogDir" > "${PID_DIR}/${serviceName}.logdir"
  (
    # Flush server and Helix logs before stopService kills the JVM. Use an environment variable
    # so configuration paths with spaces work without changing the launcher's JAVA_OPTS.
    export LOG4J_CONFIGURATION_FILE="$COMPAT_SERVER_LOG4J_CONFIG"
    export LOG_ROOT="$serverLogDir"
    export PINOT_COMPONENT="$serviceName"
    if [ -f "$configFile" ]; then
      exec "$launcher" StartServer -configFileName "$configFile"
    else
      exec "$launcher" StartServer
    fi
  ) > "${LOG_DIR}/${serviceName}.${logCount}.log" 2>&1 &
  echo $! > "${PID_DIR}/${serviceName}.pid"
}

function waitForServerReadiness() {
  local serviceName=$1
  local port=$2
  local timeoutSeconds=${SERVER_READY_TIMEOUT_SECONDS:-600}
  local pid serverLogDir remaining requestTimeout
  case "$timeoutSeconds" in
    ''|*[!0-9]*|0) echo "SERVER_READY_TIMEOUT_SECONDS must be a positive integer"; return 1 ;;
  esac
  pid=$(cat "${PID_DIR}/${serviceName}.pid") || return 1
  serverLogDir=$(cat "${PID_DIR}/${serviceName}.logdir") || return 1
  timeoutSeconds=$((10#$timeoutSeconds))
  if [ "$timeoutSeconds" -le 0 ]; then
    echo "SERVER_READY_TIMEOUT_SECONDS must be a positive integer"
    return 1
  fi
  local deadline=$((SECONDS + timeoutSeconds))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "$serviceName (pid $pid) exited before becoming ready"
      return 1
    fi
    remaining=$((deadline - SECONDS))
    if [ "$remaining" -le 0 ]; then
      break
    fi
    requestTimeout=5
    if [ "$remaining" -lt "$requestTimeout" ]; then
      requestTimeout=$remaining
    fi
    echo "Checking port $port for $serviceName readiness"
    # curl normally exits successfully for HTTP 503. Require a successful health response,
    # and keep each request within the remaining overall startup budget.
    if curl --fail --silent --show-error --connect-timeout 2 --max-time "$requestTimeout" \
        --write-out '\nHTTP %{http_code}\n' "http://localhost:${port}/health" \
        >> "$serverLogDir/readiness.log" 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "$serviceName did not become ready within ${timeoutSeconds}s"
  return 1
}

function collectServerDiagnostics() {
  local serviceName=$1
  local port=$2
  local pid serverLogDir dumpPid dumpDeadline jstackCommand
  serverLogDir=$(cat "${PID_DIR}/${serviceName}.logdir" 2>/dev/null) || return 0
  pid=$(cat "${PID_DIR}/${serviceName}.pid" 2>/dev/null) || return 0
  echo "Collecting diagnostics for $serviceName (pid $pid) in $serverLogDir"
  ps -p "$pid" -o pid,ppid,stat,args > "$serverLogDir/process.txt" 2>&1
  curl --silent --show-error --connect-timeout 2 --max-time 5 \
      --write-out '\nHTTP %{http_code}\n' "http://localhost:${port}/health" \
      > "$serverLogDir/health-on-failure.txt" 2>&1
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "Server process has exited; no thread dump available" > "$serverLogDir/threads.txt"
    return 0
  fi
  jstackCommand=jstack
  if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/jstack" ]; then
    jstackCommand="$JAVA_HOME/bin/jstack"
  fi
  # Attach before stopServices kills the JVM, but do not let a stuck attach block cleanup.
  "$jstackCommand" -l "$pid" > "$serverLogDir/threads.txt" 2>&1 &
  dumpPid=$!
  dumpDeadline=$((SECONDS + 10))
  while kill -0 "$dumpPid" 2>/dev/null; do
    if [ "$SECONDS" -ge "$dumpDeadline" ]; then
      kill -9 "$dumpPid" 2>/dev/null
      echo "Thread dump timed out after 10s" >> "$serverLogDir/threads.txt"
      break
    fi
    sleep 1
  done
  wait "$dumpPid" 2>/dev/null || true
}

function abortCompatibilityCheck() {
  echo "$1"
  collectServerDiagnostics server "$SERVER_ADMIN_PORT"
  if [ -f "${SERVER_CONF_2}" ]; then
    collectServerDiagnostics server2 "$SERVER_2_ADMIN_PORT"
  fi
  if [ "$keepClusterOnFailure" = "false" ]; then
    stopServices
  fi
  exit 1
}

function waitForServerReady() {
  waitForServerReadiness server "$SERVER_ADMIN_PORT" || abortCompatibilityCheck "Server readiness check failed"
}

function waitForServer2Ready() {
  waitForServerReadiness server2 "$SERVER_2_ADMIN_PORT" || abortCompatibilityCheck "Server 2 readiness check failed"
}
