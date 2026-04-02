-- ============================================================================
-- SPARSE_MAP vs Flattened Column Benchmark Queries
--
-- Tables:
--   rta_tarunm_checkout_event_sparse_map   (SM)
--   rta_tarunm_checkout_event_flattened    (FLAT)
--
-- Column mapping:
--   FLAT: userContext_clientName        => SM: userContext['clientName']
--   FLAT: riskError_code               => SM: riskError['code']
--   FLAT: rpcError_errorCode           => SM: rpcError['errorCode']
--   FLAT: selectedDefaultPaymentProfile_* => SM: selectedDefaultPaymentProfile['*']
-- ============================================================================

-- ============================================================================
-- Q01: Single key projection
-- ============================================================================
-- SM
SELECT orderUUID, userContext['clientName'] AS clientName
FROM rta_tarunm_checkout_event_sparse_map
ORDER BY createdAtMs DESC LIMIT 100;
-- FLAT
SELECT orderUUID, userContext_clientName AS clientName
FROM rta_tarunm_checkout_event_flattened
ORDER BY createdAtMs DESC LIMIT 100;

-- ============================================================================
-- Q02: Multi-key projection (3 keys from same sparse map)
-- ============================================================================
-- SM
SELECT orderUUID, userContext['clientName'] AS clientName, userContext['deviceOS'] AS deviceOS, userContext['regionName'] AS regionName
FROM rta_tarunm_checkout_event_sparse_map
ORDER BY createdAtMs DESC LIMIT 100;
-- FLAT
SELECT orderUUID, userContext_clientName AS clientName, userContext_deviceOS AS deviceOS, userContext_regionName AS regionName
FROM rta_tarunm_checkout_event_flattened
ORDER BY createdAtMs DESC LIMIT 100;

-- ============================================================================
-- Q03: Cross-map projection (keys from multiple sparse maps)
-- ============================================================================
-- SM
SELECT orderUUID, userContext['clientName'] AS clientName, riskError['code'] AS riskCode, selectedDefaultPaymentProfile['selectedPaymentMethodType'] AS pmType
FROM rta_tarunm_checkout_event_sparse_map
ORDER BY createdAtMs DESC LIMIT 100;
-- FLAT
SELECT orderUUID, userContext_clientName AS clientName, riskError_code AS riskCode, selectedDefaultPaymentProfile_selectedPaymentMethodType AS pmType
FROM rta_tarunm_checkout_event_flattened
ORDER BY createdAtMs DESC LIMIT 100;

-- ============================================================================
-- Q04: EQ filter on inverted-indexed key
-- ============================================================================
-- SM
SELECT orderUUID, userContext['regionName'] AS regionName, amount
FROM rta_tarunm_checkout_event_sparse_map
WHERE userContext['regionName'] = 'us-east4'
ORDER BY createdAtMs DESC LIMIT 500;
-- FLAT
SELECT orderUUID, userContext_regionName AS regionName, amount
FROM rta_tarunm_checkout_event_flattened
WHERE userContext_regionName = 'us-east4'
ORDER BY createdAtMs DESC LIMIT 500;

-- ============================================================================
-- Q05: IN filter on inverted-indexed key
-- ============================================================================
-- SM
SELECT orderUUID, userContext['deviceOS'] AS deviceOS
FROM rta_tarunm_checkout_event_sparse_map
WHERE userContext['deviceOS'] IN ('iOS', 'Android', 'Web')
ORDER BY createdAtMs DESC LIMIT 500;
-- FLAT
SELECT orderUUID, userContext_deviceOS AS deviceOS
FROM rta_tarunm_checkout_event_flattened
WHERE userContext_deviceOS IN ('iOS', 'Android', 'Web')
ORDER BY createdAtMs DESC LIMIT 500;

-- ============================================================================
-- Q06: NOT_EQ filter (tests absence-exclusion semantics)
-- ============================================================================
-- SM
SELECT orderUUID, riskError['code'] AS riskCode
FROM rta_tarunm_checkout_event_sparse_map
WHERE riskError['code'] != 'NONE'
ORDER BY createdAtMs DESC LIMIT 500;
-- FLAT
SELECT orderUUID, riskError_code AS riskCode
FROM rta_tarunm_checkout_event_flattened
WHERE riskError_code != 'NONE'
ORDER BY createdAtMs DESC LIMIT 500;

-- ============================================================================
-- Q07: Range filter on LONG key (no inverted index -- expression scan)
-- ============================================================================
-- SM
SELECT orderUUID, userContext['clientID'] AS clientID
FROM rta_tarunm_checkout_event_sparse_map
WHERE userContext['clientID'] >= 1000
ORDER BY createdAtMs DESC LIMIT 500;
-- FLAT
SELECT orderUUID, userContext_clientID AS clientID
FROM rta_tarunm_checkout_event_flattened
WHERE userContext_clientID >= 1000
ORDER BY createdAtMs DESC LIMIT 500;

-- ============================================================================
-- Q08: Range filter on inverted-indexed LONG key (cityID)
-- ============================================================================
-- SM
SELECT orderUUID, userContext['cityID'] AS cityID, userContext['regionName'] AS regionName
FROM rta_tarunm_checkout_event_sparse_map
WHERE userContext['cityID'] >= 100 AND userContext['cityID'] <= 500
ORDER BY createdAtMs DESC LIMIT 500;
-- FLAT
SELECT orderUUID, userContext_cityID AS cityID, userContext_regionName AS regionName
FROM rta_tarunm_checkout_event_flattened
WHERE userContext_cityID >= 100 AND userContext_cityID <= 500
ORDER BY createdAtMs DESC LIMIT 500;

-- ============================================================================
-- Q09: GROUP BY on inverted-indexed key + COUNT
-- ============================================================================
-- SM
SELECT userContext['deviceOS'] AS deviceOS, COUNT(*) AS cnt
FROM rta_tarunm_checkout_event_sparse_map
GROUP BY userContext['deviceOS']
ORDER BY cnt DESC LIMIT 20;
-- FLAT
SELECT userContext_deviceOS AS deviceOS, COUNT(*) AS cnt
FROM rta_tarunm_checkout_event_flattened
GROUP BY userContext_deviceOS
ORDER BY cnt DESC LIMIT 20;

-- ============================================================================
-- Q10: GROUP BY on sparse key + SUM aggregation
-- ============================================================================
-- SM
SELECT userContext['regionName'] AS regionName, SUM(amount) AS totalAmount
FROM rta_tarunm_checkout_event_sparse_map
GROUP BY userContext['regionName']
ORDER BY totalAmount DESC LIMIT 20;
-- FLAT
SELECT userContext_regionName AS regionName, SUM(amount) AS totalAmount
FROM rta_tarunm_checkout_event_flattened
GROUP BY userContext_regionName
ORDER BY totalAmount DESC LIMIT 20;

-- ============================================================================
-- Q11: GROUP BY cross-map key + AVG aggregation
-- ============================================================================
-- SM
SELECT selectedDefaultPaymentProfile['selectedPaymentMethodType'] AS pmType, AVG(amount) AS avgAmount, COUNT(*) AS cnt
FROM rta_tarunm_checkout_event_sparse_map
GROUP BY selectedDefaultPaymentProfile['selectedPaymentMethodType']
ORDER BY cnt DESC LIMIT 20;
-- FLAT
SELECT selectedDefaultPaymentProfile_selectedPaymentMethodType AS pmType, AVG(amount) AS avgAmount, COUNT(*) AS cnt
FROM rta_tarunm_checkout_event_flattened
GROUP BY selectedDefaultPaymentProfile_selectedPaymentMethodType
ORDER BY cnt DESC LIMIT 20;

-- ============================================================================
-- Q12: Combined filter (EQ on inverted key + EQ on another map's inverted key)
-- ============================================================================
-- SM
SELECT orderUUID, userContext['clientName'] AS clientName, rpcError['errorType'] AS errorType
FROM rta_tarunm_checkout_event_sparse_map
WHERE userContext['deviceOS'] = 'iOS' AND rpcError['errorType'] != ''
ORDER BY createdAtMs DESC LIMIT 500;
-- FLAT
SELECT orderUUID, userContext_clientName AS clientName, rpcError_errorType AS errorType
FROM rta_tarunm_checkout_event_flattened
WHERE userContext_deviceOS = 'iOS' AND rpcError_errorType != ''
ORDER BY createdAtMs DESC LIMIT 500;

-- ============================================================================
-- Q13: Filter on regular column + project sparse key (mixed access)
-- ============================================================================
-- SM
SELECT orderUUID, eventType, userContext['clientName'] AS clientName, userContext['deviceOS'] AS deviceOS, amount
FROM rta_tarunm_checkout_event_sparse_map
WHERE eventType = 'CHECKOUT_COMPLETED' AND amount > 0
ORDER BY createdAtMs DESC LIMIT 500;
-- FLAT
SELECT orderUUID, eventType, userContext_clientName AS clientName, userContext_deviceOS AS deviceOS, amount
FROM rta_tarunm_checkout_event_flattened
WHERE eventType = 'CHECKOUT_COMPLETED' AND amount > 0
ORDER BY createdAtMs DESC LIMIT 500;

-- ============================================================================
-- Q14: Multi-key GROUP BY (two keys from same sparse map)
-- ============================================================================
-- SM
SELECT userContext['deviceOS'] AS deviceOS, userContext['regionName'] AS regionName, COUNT(*) AS cnt
FROM rta_tarunm_checkout_event_sparse_map
GROUP BY userContext['deviceOS'], userContext['regionName']
ORDER BY cnt DESC LIMIT 50;
-- FLAT
SELECT userContext_deviceOS AS deviceOS, userContext_regionName AS regionName, COUNT(*) AS cnt
FROM rta_tarunm_checkout_event_flattened
GROUP BY userContext_deviceOS, userContext_regionName
ORDER BY cnt DESC LIMIT 50;

-- ============================================================================
-- Q15: Heavy projection (6 keys across 3 maps) + filter on regular column
-- ============================================================================
-- SM
SELECT orderUUID,
       userContext['clientName'] AS clientName,
       userContext['deviceOS'] AS deviceOS,
       riskError['code'] AS riskCode,
       riskError['title'] AS riskTitle,
       selectedDefaultPaymentProfile['selectedPaymentMethodType'] AS pmType,
       selectedDefaultPaymentProfile['paymentProfileFundingMethod'] AS fundingMethod,
       amount
FROM rta_tarunm_checkout_event_sparse_map
WHERE eventType = 'CHECKOUT_STARTED'
ORDER BY createdAtMs DESC LIMIT 200;
-- FLAT
SELECT orderUUID,
       userContext_clientName AS clientName,
       userContext_deviceOS AS deviceOS,
       riskError_code AS riskCode,
       riskError_title AS riskTitle,
       selectedDefaultPaymentProfile_selectedPaymentMethodType AS pmType,
       selectedDefaultPaymentProfile_paymentProfileFundingMethod AS fundingMethod,
       amount
FROM rta_tarunm_checkout_event_flattened
WHERE eventType = 'CHECKOUT_STARTED'
ORDER BY createdAtMs DESC LIMIT 200;
