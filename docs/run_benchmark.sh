#!/bin/bash
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

# Benchmark: SPARSE_MAP vs Flattened — 15 query pairs, 5 iterations each, 1s delay
set -euo pipefail

BROKER="http://dca24-x6a:25634/query/sql"
PROXY="socks5h://localhost:8002"
ITERATIONS=5
DELAY=1

run_query() {
  local sql="$1"
  local result
  result=$(curl -s -x "$PROXY" "$BROKER" \
    -H "Content-Type: application/json" \
    -d "{\"sql\": \"$sql\"}" --max-time 30)

  local timeMs=$(echo "$result" | jq -r '.timeUsedMs // "ERR"')
  local docsScanned=$(echo "$result" | jq -r '.numDocsScanned // 0')
  local filterScanned=$(echo "$result" | jq -r '.numEntriesScannedInFilter // 0')
  local postScanned=$(echo "$result" | jq -r '.numEntriesScannedPostFilter // 0')
  local totalDocs=$(echo "$result" | jq -r '.totalDocs // 0')
  local numRows=$(echo "$result" | jq -r '.resultTable.rows | length // 0' 2>/dev/null || echo 0)
  local exceptions=$(echo "$result" | jq -r '.exceptions | length // 0' 2>/dev/null || echo 0)

  if [ "$exceptions" != "0" ]; then
    local errMsg=$(echo "$result" | jq -r '.exceptions[0].message // "unknown"' | head -c 120)
    echo "ERR|0|0|0|0|0|0|$errMsg"
  else
    echo "${timeMs}|${docsScanned}|${filterScanned}|${postScanned}|${totalDocs}|${numRows}|${exceptions}"
  fi
}

bench() {
  local label="$1"
  local sql="$2"
  local times=()
  local lastResult=""

  for i in $(seq 1 $ITERATIONS); do
    lastResult=$(run_query "$sql")
    local t=$(echo "$lastResult" | cut -d'|' -f1)
    if [ "$t" = "ERR" ]; then
      echo "$label|ERR|$lastResult"
      return
    fi
    times+=("$t")
    [ "$i" -lt "$ITERATIONS" ] && sleep "$DELAY"
  done

  # Compute avg/min/max from times array
  local sum=0 min=${times[0]} max=${times[0]}
  for t in "${times[@]}"; do
    sum=$((sum + t))
    [ "$t" -lt "$min" ] && min=$t
    [ "$t" -gt "$max" ] && max=$t
  done
  local avg=$((sum / ITERATIONS))

  local docs=$(echo "$lastResult" | cut -d'|' -f2)
  local filter=$(echo "$lastResult" | cut -d'|' -f3)
  local post=$(echo "$lastResult" | cut -d'|' -f4)
  local total=$(echo "$lastResult" | cut -d'|' -f5)
  local rows=$(echo "$lastResult" | cut -d'|' -f6)

  printf "%-8s | avg=%4dms  min=%4dms  max=%4dms | docs=%-8s filter=%-8s post=%-8s total=%-8s rows=%-6s\n" \
    "$label" "$avg" "$min" "$max" "$docs" "$filter" "$post" "$total" "$rows"
}

# ---- Queries ----
declare -a LABELS=(
  "Q01:Single key projection"
  "Q02:Multi-key projection (3 keys, same map)"
  "Q03:Cross-map projection (3 maps)"
  "Q04:EQ filter (inverted key)"
  "Q05:IN filter (inverted key)"
  "Q06:NOT_EQ filter (absence semantics)"
  "Q07:Range filter (no inverted index)"
  "Q08:BETWEEN filter (inverted LONG key)"
  "Q09:GROUP BY + COUNT"
  "Q10:GROUP BY + SUM"
  "Q11:GROUP BY cross-map + AVG"
  "Q12:Combined multi-map filter"
  "Q13:Mixed: regular filter + sparse proj"
  "Q14:Multi-key GROUP BY"
  "Q15:Heavy projection (6 keys, 3 maps)"
)

declare -a SM_QUERIES=(
  "SELECT orderUUID, userContext['clientName'] AS clientName FROM rta_tarunm_checkout_event_sparse_map ORDER BY createdAtMs DESC LIMIT 100"
  "SELECT orderUUID, userContext['clientName'] AS clientName, userContext['deviceOS'] AS deviceOS, userContext['regionName'] AS regionName FROM rta_tarunm_checkout_event_sparse_map ORDER BY createdAtMs DESC LIMIT 100"
  "SELECT orderUUID, userContext['clientName'] AS clientName, riskError['code'] AS riskCode, selectedDefaultPaymentProfile['selectedPaymentMethodType'] AS pmType FROM rta_tarunm_checkout_event_sparse_map ORDER BY createdAtMs DESC LIMIT 100"
  "SELECT orderUUID, userContext['regionName'] AS regionName, amount FROM rta_tarunm_checkout_event_sparse_map WHERE userContext['regionName'] = 'us-east4' ORDER BY createdAtMs DESC LIMIT 500"
  "SELECT orderUUID, userContext['deviceOS'] AS deviceOS FROM rta_tarunm_checkout_event_sparse_map WHERE userContext['deviceOS'] IN ('iOS', 'Android', 'Web') ORDER BY createdAtMs DESC LIMIT 500"
  "SELECT orderUUID, riskError['code'] AS riskCode FROM rta_tarunm_checkout_event_sparse_map WHERE riskError['code'] != 'NONE' ORDER BY createdAtMs DESC LIMIT 500"
  "SELECT orderUUID, userContext['clientID'] AS clientID FROM rta_tarunm_checkout_event_sparse_map WHERE userContext['clientID'] >= 1000 ORDER BY createdAtMs DESC LIMIT 500"
  "SELECT orderUUID, userContext['cityID'] AS cityID, userContext['regionName'] AS regionName FROM rta_tarunm_checkout_event_sparse_map WHERE userContext['cityID'] >= 100 AND userContext['cityID'] <= 500 ORDER BY createdAtMs DESC LIMIT 500"
  "SELECT userContext['deviceOS'] AS deviceOS, COUNT(*) AS cnt FROM rta_tarunm_checkout_event_sparse_map GROUP BY userContext['deviceOS'] ORDER BY cnt DESC LIMIT 20"
  "SELECT userContext['regionName'] AS regionName, SUM(amount) AS totalAmount FROM rta_tarunm_checkout_event_sparse_map GROUP BY userContext['regionName'] ORDER BY totalAmount DESC LIMIT 20"
  "SELECT selectedDefaultPaymentProfile['selectedPaymentMethodType'] AS pmType, AVG(amount) AS avgAmount, COUNT(*) AS cnt FROM rta_tarunm_checkout_event_sparse_map GROUP BY selectedDefaultPaymentProfile['selectedPaymentMethodType'] ORDER BY cnt DESC LIMIT 20"
  "SELECT orderUUID, userContext['clientName'] AS clientName, rpcError['errorType'] AS errorType FROM rta_tarunm_checkout_event_sparse_map WHERE userContext['deviceOS'] = 'iOS' AND rpcError['errorType'] != '' ORDER BY createdAtMs DESC LIMIT 500"
  "SELECT orderUUID, eventType, userContext['clientName'] AS clientName, userContext['deviceOS'] AS deviceOS, amount FROM rta_tarunm_checkout_event_sparse_map WHERE eventType = 'CHECKOUT_COMPLETED' AND amount > 0 ORDER BY createdAtMs DESC LIMIT 500"
  "SELECT userContext['deviceOS'] AS deviceOS, userContext['regionName'] AS regionName, COUNT(*) AS cnt FROM rta_tarunm_checkout_event_sparse_map GROUP BY userContext['deviceOS'], userContext['regionName'] ORDER BY cnt DESC LIMIT 50"
  "SELECT orderUUID, userContext['clientName'] AS clientName, userContext['deviceOS'] AS deviceOS, riskError['code'] AS riskCode, riskError['title'] AS riskTitle, selectedDefaultPaymentProfile['selectedPaymentMethodType'] AS pmType, selectedDefaultPaymentProfile['paymentProfileFundingMethod'] AS fundingMethod, amount FROM rta_tarunm_checkout_event_sparse_map WHERE eventType = 'CHECKOUT_STARTED' ORDER BY createdAtMs DESC LIMIT 200"
)

declare -a FLAT_QUERIES=(
  "SELECT orderUUID, userContext_clientName AS clientName FROM rta_tarunm_checkout_event_flattened ORDER BY createdAtMs DESC LIMIT 100"
  "SELECT orderUUID, userContext_clientName AS clientName, userContext_deviceOS AS deviceOS, userContext_regionName AS regionName FROM rta_tarunm_checkout_event_flattened ORDER BY createdAtMs DESC LIMIT 100"
  "SELECT orderUUID, userContext_clientName AS clientName, riskError_code AS riskCode, selectedDefaultPaymentProfile_selectedPaymentMethodType AS pmType FROM rta_tarunm_checkout_event_flattened ORDER BY createdAtMs DESC LIMIT 100"
  "SELECT orderUUID, userContext_regionName AS regionName, amount FROM rta_tarunm_checkout_event_flattened WHERE userContext_regionName = 'us-east4' ORDER BY createdAtMs DESC LIMIT 500"
  "SELECT orderUUID, userContext_deviceOS AS deviceOS FROM rta_tarunm_checkout_event_flattened WHERE userContext_deviceOS IN ('iOS', 'Android', 'Web') ORDER BY createdAtMs DESC LIMIT 500"
  "SELECT orderUUID, riskError_code AS riskCode FROM rta_tarunm_checkout_event_flattened WHERE riskError_code != 'NONE' ORDER BY createdAtMs DESC LIMIT 500"
  "SELECT orderUUID, userContext_clientID AS clientID FROM rta_tarunm_checkout_event_flattened WHERE userContext_clientID >= 1000 ORDER BY createdAtMs DESC LIMIT 500"
  "SELECT orderUUID, userContext_cityID AS cityID, userContext_regionName AS regionName FROM rta_tarunm_checkout_event_flattened WHERE userContext_cityID >= 100 AND userContext_cityID <= 500 ORDER BY createdAtMs DESC LIMIT 500"
  "SELECT userContext_deviceOS AS deviceOS, COUNT(*) AS cnt FROM rta_tarunm_checkout_event_flattened GROUP BY userContext_deviceOS ORDER BY cnt DESC LIMIT 20"
  "SELECT userContext_regionName AS regionName, SUM(amount) AS totalAmount FROM rta_tarunm_checkout_event_flattened GROUP BY userContext_regionName ORDER BY totalAmount DESC LIMIT 20"
  "SELECT selectedDefaultPaymentProfile_selectedPaymentMethodType AS pmType, AVG(amount) AS avgAmount, COUNT(*) AS cnt FROM rta_tarunm_checkout_event_flattened GROUP BY selectedDefaultPaymentProfile_selectedPaymentMethodType ORDER BY cnt DESC LIMIT 20"
  "SELECT orderUUID, userContext_clientName AS clientName, rpcError_errorType AS errorType FROM rta_tarunm_checkout_event_flattened WHERE userContext_deviceOS = 'iOS' AND rpcError_errorType != '' ORDER BY createdAtMs DESC LIMIT 500"
  "SELECT orderUUID, eventType, userContext_clientName AS clientName, userContext_deviceOS AS deviceOS, amount FROM rta_tarunm_checkout_event_flattened WHERE eventType = 'CHECKOUT_COMPLETED' AND amount > 0 ORDER BY createdAtMs DESC LIMIT 500"
  "SELECT userContext_deviceOS AS deviceOS, userContext_regionName AS regionName, COUNT(*) AS cnt FROM rta_tarunm_checkout_event_flattened GROUP BY userContext_deviceOS, userContext_regionName ORDER BY cnt DESC LIMIT 50"
  "SELECT orderUUID, userContext_clientName AS clientName, userContext_deviceOS AS deviceOS, riskError_code AS riskCode, riskError_title AS riskTitle, selectedDefaultPaymentProfile_selectedPaymentMethodType AS pmType, selectedDefaultPaymentProfile_paymentProfileFundingMethod AS fundingMethod, amount FROM rta_tarunm_checkout_event_flattened WHERE eventType = 'CHECKOUT_STARTED' ORDER BY createdAtMs DESC LIMIT 200"
)

echo "================================================================================"
echo "SPARSE_MAP vs Flattened Benchmark | ${ITERATIONS} iterations | ${DELAY}s delay"
echo "Broker: $BROKER"
echo "================================================================================"
echo ""

for i in "${!LABELS[@]}"; do
  label="${LABELS[$i]}"
  qnum="${label%%:*}"
  desc="${label#*:}"

  echo "──────────────────────────────────────────────────────────────────────────────────"
  printf "%-4s %s\n" "$qnum" "$desc"
  echo "──────────────────────────────────────────────────────────────────────────────────"

  bench "SM  " "${SM_QUERIES[$i]}"
  bench "FLAT" "${FLAT_QUERIES[$i]}"
  echo ""
done

echo "================================================================================"
echo "DONE"
echo "================================================================================"
