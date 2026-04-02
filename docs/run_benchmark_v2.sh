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

# Benchmark v2: SPARSE_MAP vs Flattened — real filter values, 5 iterations, 1s delay
set -euo pipefail

BROKER="http://dca24-x6a:25634/query/sql"
PROXY="socks5h://localhost:8002"
ITERATIONS=5
DELAY=1

run_query() {
  local sql="$1"
  curl -s -x "$PROXY" "$BROKER" \
    -H "Content-Type: application/json" \
    -d "{\"sql\": \"$sql\"}" --max-time 30
}

bench() {
  local label="$1"
  local sql="$2"
  local times=()
  local lastRaw=""

  for i in $(seq 1 $ITERATIONS); do
    lastRaw=$(run_query "$sql")
    local exc=$(echo "$lastRaw" | jq -r '.exceptions | length' 2>/dev/null)
    if [ "$exc" != "0" ] && [ "$exc" != "" ] && [ "$exc" != "null" ]; then
      local errMsg=$(echo "$lastRaw" | jq -r '.exceptions[0].message' 2>/dev/null | head -c 150)
      printf "  %-6s ERROR: %s\n" "$label" "$errMsg"
      return
    fi
    local t=$(echo "$lastRaw" | jq -r '.timeUsedMs')
    times+=("$t")
    [ "$i" -lt "$ITERATIONS" ] && sleep "$DELAY"
  done

  local sum=0 min=${times[0]} max=${times[0]}
  for t in "${times[@]}"; do
    sum=$((sum + t))
    (( t < min )) && min=$t
    (( t > max )) && max=$t
  done
  local avg=$((sum / ITERATIONS))

  local docs=$(echo "$lastRaw" | jq -r '.numDocsScanned')
  local filter=$(echo "$lastRaw" | jq -r '.numEntriesScannedInFilter')
  local post=$(echo "$lastRaw" | jq -r '.numEntriesScannedPostFilter')
  local total=$(echo "$lastRaw" | jq -r '.totalDocs')
  local rows=$(echo "$lastRaw" | jq -r '.resultTable.rows | length')

  printf "  %-6s avg=%5dms  min=%5dms  max=%5dms  | docs=%-10s filter=%-10s post=%-10s total=%-10s rows=%s\n" \
    "$label" "$avg" "$min" "$max" "$docs" "$filter" "$post" "$total" "$rows"
}

echo "================================================================================"
echo "SPARSE_MAP vs Flattened Benchmark v2 | ${ITERATIONS} itr | ${DELAY}s delay"
echo "================================================================================"

SM="rta_tarunm_checkout_event_sparse_map"
FL="rta_tarunm_checkout_event_flattened"

# ---------- Q01: Single key projection ----------
echo ""
echo "--- Q01: Single key projection ---"
bench "SM" "SELECT orderUUID, userContext['clientName'] AS clientName FROM $SM ORDER BY createdAtMs DESC LIMIT 100"
bench "FLAT" "SELECT orderUUID, userContext_clientName AS clientName FROM $FL ORDER BY createdAtMs DESC LIMIT 100"

# ---------- Q02: Multi-key projection (3 keys, same map) ----------
echo ""
echo "--- Q02: Multi-key projection (3 keys, same map) ---"
bench "SM" "SELECT orderUUID, userContext['clientName'] AS cn, userContext['deviceOS'] AS dos, userContext['regionName'] AS rn FROM $SM ORDER BY createdAtMs DESC LIMIT 100"
bench "FLAT" "SELECT orderUUID, userContext_clientName AS cn, userContext_deviceOS AS dos, userContext_regionName AS rn FROM $FL ORDER BY createdAtMs DESC LIMIT 100"

# ---------- Q03: Cross-map projection ----------
echo ""
echo "--- Q03: Cross-map projection (3 maps) ---"
bench "SM" "SELECT orderUUID, userContext['clientName'] AS cn, riskError['code'] AS rc, selectedDefaultPaymentProfile['selectedPaymentMethodType'] AS pm FROM $SM ORDER BY createdAtMs DESC LIMIT 100"
bench "FLAT" "SELECT orderUUID, userContext_clientName AS cn, riskError_code AS rc, selectedDefaultPaymentProfile_selectedPaymentMethodType AS pm FROM $FL ORDER BY createdAtMs DESC LIMIT 100"

# ---------- Q04: EQ filter (inverted key, real value) ----------
echo ""
echo "--- Q04: EQ filter regionName='sao_paulo' ---"
bench "SM" "SELECT orderUUID, userContext['regionName'] AS rn, amount FROM $SM WHERE userContext['regionName'] = 'sao_paulo' ORDER BY createdAtMs DESC LIMIT 500"
bench "FLAT" "SELECT orderUUID, userContext_regionName AS rn, amount FROM $FL WHERE userContext_regionName = 'sao_paulo' ORDER BY createdAtMs DESC LIMIT 500"

# ---------- Q05: IN filter (inverted key, real values) ----------
echo ""
echo "--- Q05: IN filter deviceOS IN ('16','15','14') ---"
bench "SM" "SELECT orderUUID, userContext['deviceOS'] AS dos FROM $SM WHERE userContext['deviceOS'] IN ('16', '15', '14') ORDER BY createdAtMs DESC LIMIT 500"
bench "FLAT" "SELECT orderUUID, userContext_deviceOS AS dos FROM $FL WHERE userContext_deviceOS IN ('16', '15', '14') ORDER BY createdAtMs DESC LIMIT 500"

# ---------- Q06: NOT_EQ filter ----------
echo ""
echo "--- Q06: NOT_EQ riskError code != 'INSUFFICIENT_BALANCE' ---"
bench "SM" "SELECT orderUUID, riskError['code'] AS rc FROM $SM WHERE riskError['code'] != 'INSUFFICIENT_BALANCE' AND riskError['code'] != '' ORDER BY createdAtMs DESC LIMIT 500"
bench "FLAT" "SELECT orderUUID, riskError_code AS rc FROM $FL WHERE riskError_code != 'INSUFFICIENT_BALANCE' AND riskError_code != 'null' AND riskError_code != '' ORDER BY createdAtMs DESC LIMIT 500"

# ---------- Q07: EQ filter on payment method (inverted key, high cardinality) ----------
echo ""
echo "--- Q07: EQ filter pmType='BANK_CARD' ---"
bench "SM" "SELECT orderUUID, selectedDefaultPaymentProfile['selectedPaymentMethodType'] AS pm, amount FROM $SM WHERE selectedDefaultPaymentProfile['selectedPaymentMethodType'] = 'BANK_CARD' ORDER BY createdAtMs DESC LIMIT 500"
bench "FLAT" "SELECT orderUUID, selectedDefaultPaymentProfile_selectedPaymentMethodType AS pm, amount FROM $FL WHERE selectedDefaultPaymentProfile_selectedPaymentMethodType = 'BANK_CARD' ORDER BY createdAtMs DESC LIMIT 500"

# ---------- Q08: EQ + regular column filter combined ----------
echo ""
echo "--- Q08: eventType='ASYNC_CHECKOUT' + regionName='new_york' ---"
bench "SM" "SELECT orderUUID, userContext['regionName'] AS rn, amount FROM $SM WHERE eventType = 'ASYNC_CHECKOUT' AND userContext['regionName'] = 'new_york' ORDER BY createdAtMs DESC LIMIT 500"
bench "FLAT" "SELECT orderUUID, userContext_regionName AS rn, amount FROM $FL WHERE eventType = 'ASYNC_CHECKOUT' AND userContext_regionName = 'new_york' ORDER BY createdAtMs DESC LIMIT 500"

# ---------- Q09: Multi-map filter ----------
echo ""
echo "--- Q09: deviceOS='16' AND pmType='APPLE_PAY' ---"
bench "SM" "SELECT orderUUID, userContext['deviceOS'] AS dos, selectedDefaultPaymentProfile['selectedPaymentMethodType'] AS pm FROM $SM WHERE userContext['deviceOS'] = '16' AND selectedDefaultPaymentProfile['selectedPaymentMethodType'] = 'APPLE_PAY' ORDER BY createdAtMs DESC LIMIT 500"
bench "FLAT" "SELECT orderUUID, userContext_deviceOS AS dos, selectedDefaultPaymentProfile_selectedPaymentMethodType AS pm FROM $FL WHERE userContext_deviceOS = '16' AND selectedDefaultPaymentProfile_selectedPaymentMethodType = 'APPLE_PAY' ORDER BY createdAtMs DESC LIMIT 500"

# ---------- Q10: GROUP BY + COUNT ----------
echo ""
echo "--- Q10: GROUP BY deviceOS + COUNT ---"
bench "SM" "SELECT userContext['deviceOS'] AS dos, COUNT(*) AS cnt FROM $SM GROUP BY userContext['deviceOS'] ORDER BY cnt DESC LIMIT 20"
bench "FLAT" "SELECT userContext_deviceOS AS dos, COUNT(*) AS cnt FROM $FL GROUP BY userContext_deviceOS ORDER BY cnt DESC LIMIT 20"

# ---------- Q11: GROUP BY + SUM ----------
echo ""
echo "--- Q11: GROUP BY regionName + SUM(amount) ---"
bench "SM" "SELECT userContext['regionName'] AS rn, SUM(amount) AS total FROM $SM GROUP BY userContext['regionName'] ORDER BY total DESC LIMIT 20"
bench "FLAT" "SELECT userContext_regionName AS rn, SUM(amount) AS total FROM $FL GROUP BY userContext_regionName ORDER BY total DESC LIMIT 20"

# ---------- Q12: GROUP BY cross-map + AVG ----------
echo ""
echo "--- Q12: GROUP BY pmType + AVG(amount) ---"
bench "SM" "SELECT selectedDefaultPaymentProfile['selectedPaymentMethodType'] AS pm, AVG(amount) AS avg, COUNT(*) AS cnt FROM $SM GROUP BY selectedDefaultPaymentProfile['selectedPaymentMethodType'] ORDER BY cnt DESC LIMIT 20"
bench "FLAT" "SELECT selectedDefaultPaymentProfile_selectedPaymentMethodType AS pm, AVG(amount) AS avg, COUNT(*) AS cnt FROM $FL GROUP BY selectedDefaultPaymentProfile_selectedPaymentMethodType ORDER BY cnt DESC LIMIT 20"

# ---------- Q13: Multi-key GROUP BY ----------
echo ""
echo "--- Q13: GROUP BY deviceOS, regionName ---"
bench "SM" "SELECT userContext['deviceOS'] AS dos, userContext['regionName'] AS rn, COUNT(*) AS cnt FROM $SM GROUP BY userContext['deviceOS'], userContext['regionName'] ORDER BY cnt DESC LIMIT 50"
bench "FLAT" "SELECT userContext_deviceOS AS dos, userContext_regionName AS rn, COUNT(*) AS cnt FROM $FL GROUP BY userContext_deviceOS, userContext_regionName ORDER BY cnt DESC LIMIT 50"

# ---------- Q14: Filtered GROUP BY ----------
echo ""
echo "--- Q14: WHERE eventType='ASYNC_CHECKOUT' GROUP BY regionName + COUNT ---"
bench "SM" "SELECT userContext['regionName'] AS rn, COUNT(*) AS cnt FROM $SM WHERE eventType = 'ASYNC_CHECKOUT' GROUP BY userContext['regionName'] ORDER BY cnt DESC LIMIT 20"
bench "FLAT" "SELECT userContext_regionName AS rn, COUNT(*) AS cnt FROM $FL WHERE eventType = 'ASYNC_CHECKOUT' GROUP BY userContext_regionName ORDER BY cnt DESC LIMIT 20"

# ---------- Q15: Heavy projection (6 keys, 3 maps) + filter ----------
echo ""
echo "--- Q15: Heavy projection (6 keys, 3 maps) + eventType filter ---"
bench "SM" "SELECT orderUUID, userContext['clientName'] AS cn, userContext['deviceOS'] AS dos, riskError['code'] AS rc, riskError['title'] AS rt, selectedDefaultPaymentProfile['selectedPaymentMethodType'] AS pm, selectedDefaultPaymentProfile['paymentProfileFundingMethod'] AS fm, amount FROM $SM WHERE eventType = 'ASYNC_CHECKOUT' ORDER BY createdAtMs DESC LIMIT 200"
bench "FLAT" "SELECT orderUUID, userContext_clientName AS cn, userContext_deviceOS AS dos, riskError_code AS rc, riskError_title AS rt, selectedDefaultPaymentProfile_selectedPaymentMethodType AS pm, selectedDefaultPaymentProfile_paymentProfileFundingMethod AS fm, amount FROM $FL WHERE eventType = 'ASYNC_CHECKOUT' ORDER BY createdAtMs DESC LIMIT 200"

echo ""
echo "================================================================================"
echo "DONE"
echo "================================================================================"
