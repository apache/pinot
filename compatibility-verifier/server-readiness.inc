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

# Single-server checks write to the supplied, existing log directory. The caller owns
# launching/stopping services and deciding what to do when readiness fails.
# Returns success only for a live process with a successful health response within the timeout.
function waitForServerReadiness() {
  local serviceName=$1
  local pid=$2
  local port=$3
  local serverLogDir=$4
  local timeoutSeconds=$5
  local remaining requestTimeout
  case "$timeoutSeconds" in
    ''|*[!0-9]*|0) echo "Readiness timeout must be a positive integer: $timeoutSeconds"; return 1 ;;
  esac
  timeoutSeconds=$((10#$timeoutSeconds))
  if [ "$timeoutSeconds" -le 0 ]; then
    echo "Readiness timeout must be a positive integer: $timeoutSeconds"
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

# Best-effort, bounded diagnostics for one server. Does not stop the server or exit the caller.
function collectServerDiagnostics() {
  local serviceName=$1
  local pid=$2
  local port=$3
  local serverLogDir=$4
  local dumpPid dumpDeadline jstackCommand
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
  # Do not let a stuck attach block the caller's failure handling.
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
