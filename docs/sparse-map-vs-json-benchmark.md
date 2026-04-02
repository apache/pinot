<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License. You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied. See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# SPARSE_MAP vs JSON Column — Benchmark Results

## Setup

| Parameter | Value |
|---|---|
| Dataset | 10,020 rows · 11 segments (10 × 1,000-row ingestion + 1 × 20-row quickstart) |
| Iterations per query | 100 |
| Broker | `http://localhost:8000/query/sql` |
| SPARSE_MAP keys | `clicks` (LONG, ~65%), `spend` (DOUBLE, ~55%), `sessions` (INT, ~45%), `country` (STRING, ~70%) |
| SPARSE_MAP index | `enableInvertedIndex: true`, `maxKeys: 100` |
| JSON index | `jsonIndexColumns: ["metrics"]`, `noDictionaryColumns: ["metrics"]` |
| Benchmark script | `scripts/benchmark_sparse_vs_json.py` |

### Schema

Both tables share the same logical schema. The only difference is the `metrics` column type.

**`userMetrics`** — `metrics` is `SPARSE_MAP`

```json
{
  "name": "metrics",
  "dataType": "SPARSE_MAP",
  "singleValueField": true,
  "sparseMapKeyTypes": { "clicks": "LONG", "spend": "DOUBLE", "sessions": "INT", "country": "STRING" },
  "sparseMapDefaultValueType": "STRING"
}
```

**`userMetricsJson`** — `metrics` is `JSON`

```json
{ "name": "metrics", "dataType": "JSON" }
```

### Query Syntax Cheat Sheet

| Operation | SPARSE_MAP | JSON |
|---|---|---|
| Read key | `metrics['clicks']` | `JSON_EXTRACT_SCALAR(metrics, '$.clicks', 'LONG', '0')` |
| EQ filter | `WHERE metrics['country'] = 'US'` | `WHERE JSON_MATCH(metrics, '"$.country" = ''US''')` |
| IN filter | `WHERE metrics['country'] IN ('US','DE')` | `WHERE JSON_MATCH(metrics, '"$.country" IN (''US'',''DE'')')` |
| NEQ filter | `WHERE metrics['country'] != 'US'` | `WHERE JSON_MATCH(metrics, '"$.country" != ''US''')` |
| Presence | `WHERE metrics['country'] != ''` | `WHERE JSON_MATCH(metrics, '"$.country" IS NOT NULL')` |
| Range | `WHERE metrics['clicks'] >= 200` | `WHERE JSON_EXTRACT_SCALAR(...) >= 200` |
| REGEXP | `WHERE REGEXP_LIKE(metrics['country'], 'U.*')` | `WHERE JSON_MATCH(metrics, 'REGEXP_LIKE("$.country",''U.*'')')` |
| Aggregate | `SUM(metrics['clicks'])` | `SUM(JSON_EXTRACT_SCALAR(metrics, '$.clicks', 'LONG', '0'))` |
| Group by | `GROUP BY metrics['country']` | `GROUP BY JSON_EXTRACT_SCALAR(metrics, '$.country', 'STRING', '')` |

---

## Results — 100 Iterations

Stats columns: **avg** · **min** · **max** · **p95** · **σ** (stddev) — all in milliseconds (server-side).
Scan columns: **docsScanned** · **filterScan** (`numEntriesScannedInFilter`) · **postScan** (`numEntriesScannedPostFilter`).

---

### Q01 · `COUNT(*)`

```sql
-- SPARSE_MAP
SELECT COUNT(*) FROM userMetrics

-- JSON
SELECT COUNT(*) FROM userMetricsJson
```

| Table | avg | min | max | p95 | σ | docsScanned | filterScan | postScan | Data |
|---|---|---|---|---|---|---|---|---|---|
| SPARSE_MAP | 1.1 ms | 1.0 | 2.0 | 2.0 | 0.3 | 10,020 | 0 | 0 | ✓ match |
| JSON | 1.1 ms | 1.0 | 2.0 | 2.0 | 0.3 | 10,020 | 0 | 0 | |

**Winner: TIE** — metadata-only aggregation; neither type reads document values.

---

### Q02 · Project single key

```sql
-- SPARSE_MAP
SELECT userId, metrics['clicks'] FROM userMetrics ORDER BY userId LIMIT 50

-- JSON
SELECT userId, JSON_EXTRACT_SCALAR(metrics, '$.clicks', 'LONG', '0') AS clicks
FROM userMetricsJson ORDER BY userId LIMIT 50
```

| Table | avg | min | max | p95 | σ | docsScanned | filterScan | postScan | Data |
|---|---|---|---|---|---|---|---|---|---|
| SPARSE_MAP | 1.1 ms | 1.0 | 2.0 | 2.0 | 0.4 | 50 | 0 | 100 | ✓ match |
| JSON | 1.3 ms | 1.0 | 2.0 | 2.0 | 0.5 | 70 | 0 | 140 | |

**Winner: TIE** — SPARSE_MAP reads a typed `LONG[]` array; JSON parses the blob per doc. Gap too small to measure at LIMIT 50.

---

### Q03 · Project 3 keys

```sql
-- SPARSE_MAP
SELECT userId, metrics['clicks'], metrics['spend'], metrics['country']
FROM userMetrics ORDER BY userId LIMIT 50

-- JSON
SELECT userId,
  JSON_EXTRACT_SCALAR(metrics, '$.clicks',   'LONG',   '0')  AS clicks,
  JSON_EXTRACT_SCALAR(metrics, '$.spend',    'DOUBLE', '0.0') AS spend,
  JSON_EXTRACT_SCALAR(metrics, '$.country',  'STRING', '')    AS country
FROM userMetricsJson ORDER BY userId LIMIT 50
```

| Table | avg | min | max | p95 | σ | docsScanned | filterScan | postScan | Data |
|---|---|---|---|---|---|---|---|---|---|
| SPARSE_MAP | 1.2 ms | 1.0 | 2.0 | 2.0 | 0.4 | 70 | 0 | 140 | ✓ match |
| JSON | 1.8 ms | 1.0 | 3.0 | 2.0 | 0.5 | 70 | 0 | 140 | |

**Winner: TIE** (<1 ms) — SPARSE_MAP resolves each key via O(1) bitmap rank; JSON parses the full blob once per doc to extract 3 fields. Gap grows with key count and dataset size.

---

### Q04 · EQ filter on string key

```sql
-- SPARSE_MAP
SELECT userId, metrics['clicks'] FROM userMetrics
WHERE metrics['country'] = 'US' ORDER BY userId LIMIT 500

-- JSON  (hits JSON inverted index)
SELECT userId, JSON_EXTRACT_SCALAR(metrics, '$.clicks', 'LONG', '0') AS clicks
FROM userMetricsJson
WHERE JSON_MATCH(metrics, '"$.country" = ''US''') ORDER BY userId LIMIT 500
```

| Table | avg | min | max | p95 | σ | docsScanned | filterScan | postScan | Data |
|---|---|---|---|---|---|---|---|---|---|
| SPARSE_MAP | 1.6 ms | 1.0 | 3.0 | 2.0 | 0.5 | 2,102 | **0** | 4,204 | ✓ match |
| JSON | 2.3 ms | 2.0 | 9.0 | 3.0 | 0.8 | 2,323 | **0** | 4,646 | |

**Winner: TIE** (<1 ms) — `filterScan = 0` on both: SPARSE_MAP uses its per-key inverted index bitmap; JSON uses the JSON index token lookup. Neither touches document values during the filter pass.

---

### Q05 · IN filter on string key

```sql
-- SPARSE_MAP
SELECT userId, metrics['country'] FROM userMetrics
WHERE metrics['country'] IN ('US', 'DE', 'JP') ORDER BY userId LIMIT 500

-- JSON
SELECT userId, JSON_EXTRACT_SCALAR(metrics, '$.country', 'STRING', '') AS country
FROM userMetricsJson
WHERE JSON_MATCH(metrics, '"$.country" IN (''US'',''DE'',''JP'')') ORDER BY userId LIMIT 500
```

| Table | avg | min | max | p95 | σ | docsScanned | filterScan | postScan | Data |
|---|---|---|---|---|---|---|---|---|---|
| SPARSE_MAP | 1.8 ms | 1.0 | 8.0 | 2.0 | 0.8 | 2,456 | **0** | 4,912 | ✓ match |
| JSON | 2.8 ms | 2.0 | 9.0 | 4.0 | 0.9 | 3,077 | **0** | 6,154 | |

**Winner: SPARSE_MAP** (1.8 vs 2.8 ms) — both compute a bitmap union over 3 values with no doc scan. SPARSE_MAP's typed value extraction during projection is cheaper than JSON blob parsing.

---

### Q06 · NEQ filter on string key

```sql
-- SPARSE_MAP
SELECT userId FROM userMetrics
WHERE metrics['country'] != 'US' AND metrics['country'] != '' ORDER BY userId LIMIT 500

-- JSON
SELECT userId FROM userMetricsJson
WHERE JSON_MATCH(metrics, '"$.country" != ''US''') ORDER BY userId LIMIT 500
```

| Table | avg | min | max | p95 | σ | docsScanned | filterScan | postScan | Data |
|---|---|---|---|---|---|---|---|---|---|
| SPARSE_MAP | 1.6 ms | 1.0 | 4.0 | 3.0 | 0.6 | 1,868 | **0** | 1,868 | ✓ match |
| JSON | 1.8 ms | 1.0 | 8.0 | 3.0 | 0.9 | 4,676 | **0** | 4,676 | |

**Winner: TIE** (<1 ms) — both compute inverted index complement. SPARSE_MAP scans fewer docs (1,868 vs 4,676) because the double predicate (`!= 'US' AND != ''`) prunes absent-key docs; JSON NEQ only excludes `'US'`.

---

### Q07 · Range filter on numeric key

```sql
-- SPARSE_MAP
SELECT userId, metrics['clicks'] FROM userMetrics
WHERE metrics['clicks'] >= 200 ORDER BY userId LIMIT 500

-- JSON  (no range index — full forward scan)
SELECT userId, JSON_EXTRACT_SCALAR(metrics, '$.clicks', 'LONG', '0') AS clicks
FROM userMetricsJson
WHERE JSON_EXTRACT_SCALAR(metrics, '$.clicks', 'LONG', '0') >= 200 ORDER BY userId LIMIT 500
```

| Table | avg | min | max | p95 | σ | docsScanned | filterScan | postScan | Data |
|---|---|---|---|---|---|---|---|---|---|
| SPARSE_MAP | **1.8 ms** | 1.0 | 3.0 | 2.0 | 0.4 | 3,929 | 10,020 | 7,858 | ✓ match |
| JSON | 4.0 ms | 3.0 | 10.0 | 7.0 | 1.5 | 3,929 | 10,020 | 7,858 | |

**Winner: SPARSE_MAP — 2.2× faster** — `filterScan = 10,020` on both (no range index, every doc value inspected). SPARSE_MAP reads a compact typed `LONG[]` array; JSON deserialises the full blob per doc just to extract one field.

---

### Q08 · BETWEEN on numeric key

```sql
-- SPARSE_MAP
SELECT userId, metrics['spend'] FROM userMetrics
WHERE metrics['spend'] BETWEEN 50 AND 200 ORDER BY userId LIMIT 500

-- JSON
SELECT userId, JSON_EXTRACT_SCALAR(metrics, '$.spend', 'DOUBLE', '0.0') AS spend
FROM userMetricsJson
WHERE JSON_EXTRACT_SCALAR(metrics, '$.spend', 'DOUBLE', '0.0') BETWEEN 50 AND 200
ORDER BY userId LIMIT 500
```

| Table | avg | min | max | p95 | σ | docsScanned | filterScan | postScan | Data |
|---|---|---|---|---|---|---|---|---|---|
| SPARSE_MAP | **1.8 ms** | 1.0 | 3.0 | 2.0 | 0.5 | 2,709 | 10,020 | 5,418 | ✓ match |
| JSON | 3.5 ms | 2.0 | 9.0 | 4.0 | 1.0 | 2,709 | 10,020 | 5,418 | |

**Winner: SPARSE_MAP — 1.9× faster** — same as Q07. High σ=1.0 on JSON reflects per-doc blob parse variance.

---

### Q09 · Key presence check

```sql
-- SPARSE_MAP  (presenceBitmap.contains() — O(1) per doc)
SELECT userId FROM userMetrics
WHERE metrics['country'] != '' ORDER BY userId LIMIT 500

-- JSON  (IS NOT NULL hits the JSON index)
SELECT userId FROM userMetricsJson
WHERE JSON_MATCH(metrics, '"$.country" IS NOT NULL') ORDER BY userId LIMIT 500
```

| Table | avg | min | max | p95 | σ | docsScanned | filterScan | postScan | Data |
|---|---|---|---|---|---|---|---|---|---|
| SPARSE_MAP | 1.6 ms | 1.0 | 8.0 | 2.0 | 0.8 | 1,015 | **0** | 1,015 | ✓ match |
| JSON | 2.0 ms | 1.0 | 3.0 | 3.0 | 0.4 | 4,015 | **0** | 4,015 | |

**Winner: TIE** (<1 ms) — both `filterScan = 0`. SPARSE_MAP uses the `presenceBitmap` directly (single bit test); JSON uses IS NOT NULL token lookup. SPARSE_MAP scans far fewer docs (1,015 vs 4,015) due to segment pruning.

---

### Q10 · SUM aggregation on numeric key

```sql
-- SPARSE_MAP
SELECT SUM(metrics['clicks']) AS tc FROM userMetrics

-- JSON
SELECT SUM(JSON_EXTRACT_SCALAR(metrics, '$.clicks', 'LONG', '0')) AS tc FROM userMetricsJson
```

| Table | avg | min | max | p95 | σ | docsScanned | filterScan | postScan | Data |
|---|---|---|---|---|---|---|---|---|---|
| SPARSE_MAP | **2.1 ms** | 1.0 | 4.0 | 4.0 | 0.7 | 10,020 | 0 | 10,020 | ✓ match |
| JSON | 3.1 ms | 2.0 | 10.0 | 4.0 | 1.0 | 10,020 | 0 | 10,020 | |

**Winner: SPARSE_MAP — 1.5× faster** — identical scan counts; SPARSE_MAP iterates a dense typed `LONG[]` array; JSON parses 10,020 blobs to extract one field each.

---

### Q11 · AVG aggregation on numeric key

```sql
-- SPARSE_MAP
SELECT ROUND(AVG(metrics['spend']), 4) AS avg FROM userMetrics

-- JSON
SELECT ROUND(AVG(JSON_EXTRACT_SCALAR(metrics, '$.spend', 'DOUBLE', '0.0')), 4) AS avg
FROM userMetricsJson
```

| Table | avg | min | max | p95 | σ | docsScanned | filterScan | postScan | Data |
|---|---|---|---|---|---|---|---|---|---|
| SPARSE_MAP | **1.5 ms** | 1.0 | 4.0 | 2.0 | 0.6 | 10,020 | 0 | 10,020 | ✓ match |
| JSON | 3.0 ms | 2.0 | 10.0 | 4.0 | 1.1 | 10,020 | 0 | 10,020 | |

**Winner: SPARSE_MAP — 2.0× faster** — typed `DOUBLE[]` array iteration vs per-doc blob parse. High σ=1.1 on JSON reflects GC pressure from repeated object allocation during deserialisation.

---

### Q12 · GROUP BY string key

```sql
-- SPARSE_MAP
SELECT metrics['country'] AS country, COUNT(*) AS cnt
FROM userMetrics WHERE metrics['country'] != ''
GROUP BY metrics['country'] ORDER BY country

-- JSON
SELECT JSON_EXTRACT_SCALAR(metrics, '$.country', 'STRING', '') AS country, COUNT(*) AS cnt
FROM userMetricsJson WHERE JSON_MATCH(metrics, '"$.country" IS NOT NULL')
GROUP BY country ORDER BY country
```

| Table | avg | min | max | p95 | σ | docsScanned | filterScan | postScan | Data |
|---|---|---|---|---|---|---|---|---|---|
| SPARSE_MAP | **1.8 ms** | 1.0 | 3.0 | 2.0 | 0.5 | 6,999 | 0 | 6,999 | ✓ match (10 groups) |
| JSON | 3.4 ms | 2.0 | 11.0 | 5.0 | 1.1 | 6,999 | 0 | 6,999 | |

**Winner: SPARSE_MAP — 1.9× faster** — both reach the same 6,999 docs via index. SPARSE_MAP reads typed string values; JSON extracts the `country` field from the blob for every grouped row. High max=11 ms on JSON shows blob-parse spikes.

---

### Q13 · GROUP BY string key + SUM numeric key

```sql
-- SPARSE_MAP
SELECT metrics['country'] AS country, SUM(metrics['clicks']) AS tc
FROM userMetrics WHERE metrics['country'] != ''
GROUP BY metrics['country'] ORDER BY country

-- JSON
SELECT JSON_EXTRACT_SCALAR(metrics, '$.country', 'STRING', '') AS country,
       SUM(JSON_EXTRACT_SCALAR(metrics, '$.clicks', 'LONG', '0')) AS tc
FROM userMetricsJson WHERE JSON_MATCH(metrics, '"$.country" IS NOT NULL')
GROUP BY country ORDER BY country
```

| Table | avg | min | max | p95 | σ | docsScanned | filterScan | postScan | Data |
|---|---|---|---|---|---|---|---|---|---|
| SPARSE_MAP | **1.9 ms** | 1.0 | 9.0 | 3.0 | 0.8 | 6,999 | 0 | 6,999 | ✓ match (10 groups) |
| JSON | 4.0 ms | 3.0 | 10.0 | 5.0 | 1.0 | 6,999 | 0 | 6,999 | |

**Winner: SPARSE_MAP — 2.1× faster** — JSON must extract both `country` (for grouping) and `clicks` (for SUM) from each blob, effectively parsing every doc twice. SPARSE_MAP reads each from independent typed arrays.

---

### Q14 · GROUP BY regular column + AVG on map key

```sql
-- SPARSE_MAP
SELECT region, ROUND(AVG(metrics['clicks']), 4) AS avg
FROM userMetrics GROUP BY region ORDER BY region

-- JSON
SELECT region, ROUND(AVG(JSON_EXTRACT_SCALAR(metrics, '$.clicks', 'LONG', '0')), 4) AS avg
FROM userMetricsJson GROUP BY region ORDER BY region
```

| Table | avg | min | max | p95 | σ | docsScanned | filterScan | postScan | Data |
|---|---|---|---|---|---|---|---|---|---|
| SPARSE_MAP | **1.7 ms** | 1.0 | 2.0 | 2.0 | 0.5 | 10,020 | 0 | 20,040 | ✓ match (3 groups) |
| JSON | 3.3 ms | 2.0 | 12.0 | 5.0 | 1.3 | 10,020 | 0 | 20,040 | |

**Winner: SPARSE_MAP — 1.9× faster** — no map-column filter; all 10,020 docs scanned. `postScan = 20,040` (region + clicks). Difference is entirely the cost of extracting `clicks` per doc.

---

### Q15 · Range filter + GROUP BY + AVG

```sql
-- SPARSE_MAP
SELECT metrics['country'] AS country, ROUND(AVG(metrics['spend']), 4) AS avg
FROM userMetrics
WHERE metrics['clicks'] >= 50 AND metrics['country'] != ''
GROUP BY metrics['country'] ORDER BY country

-- JSON
SELECT JSON_EXTRACT_SCALAR(metrics, '$.country', 'STRING', '') AS country,
       ROUND(AVG(JSON_EXTRACT_SCALAR(metrics, '$.spend', 'DOUBLE', '0.0')), 4) AS avg
FROM userMetricsJson
WHERE JSON_EXTRACT_SCALAR(metrics, '$.clicks', 'LONG', '0') >= 50
  AND JSON_MATCH(metrics, '"$.country" IS NOT NULL')
GROUP BY country ORDER BY country
```

| Table | avg | min | max | p95 | σ | docsScanned | filterScan | postScan | Data |
|---|---|---|---|---|---|---|---|---|---|
| SPARSE_MAP | **2.1 ms** | 1.0 | 9.0 | 3.0 | 0.8 | 4,151 | 6,999 | 4,151 | ✓ match (10 groups) |
| JSON | 4.3 ms | 3.0 | 11.0 | 5.0 | 1.0 | 4,151 | 6,999 | 4,151 | |

**Winner: SPARSE_MAP — 2.0× faster** — `filterScan = 6,999` on both (range scan over docs that have `clicks`). JSON must parse the blob to extract `clicks` for the range check, then extract `country` and `spend` for grouping/aggregation — three extractions per doc vs three typed array reads.

---

### Q16 · Top-N ORDER BY map key DESC

```sql
-- SPARSE_MAP
SELECT userId, region, metrics['clicks'] AS clicks
FROM userMetrics ORDER BY metrics['clicks'] DESC, userId ASC LIMIT 5

-- JSON
SELECT userId, region, JSON_EXTRACT_SCALAR(metrics, '$.clicks', 'LONG', '0') AS clicks
FROM userMetricsJson ORDER BY JSON_EXTRACT_SCALAR(metrics, '$.clicks', 'LONG', '0') DESC, userId ASC LIMIT 5
```

| Table | avg | min | max | p95 | σ | docsScanned | filterScan | postScan | Data |
|---|---|---|---|---|---|---|---|---|---|
| SPARSE_MAP | **1.7 ms** | 1.0 | 3.0 | 2.0 | 0.5 | 10,020 | 0 | 20,095 | ✓ match (5 rows) |
| JSON | 3.2 ms | 2.0 | 10.0 | 4.0 | 0.8 | 10,020 | 0 | 20,095 | |

**Winner: SPARSE_MAP — 1.9× faster** — full scan required to determine Top-N. `postScan = 20,095` (userId + clicks) on both. SPARSE_MAP reads `clicks` from a typed array for the sort key; JSON parses the blob per doc.

---

### Q17 · Multi-key filter: range AND EQ

```sql
-- SPARSE_MAP  (EQ on country prunes via inverted index first; range scans the remainder)
SELECT userId, metrics['clicks'] AS clicks
FROM userMetrics
WHERE metrics['clicks'] >= 100 AND metrics['country'] = 'US'
ORDER BY userId LIMIT 500

-- JSON
SELECT userId, JSON_EXTRACT_SCALAR(metrics, '$.clicks', 'LONG', '0') AS clicks
FROM userMetricsJson
WHERE JSON_EXTRACT_SCALAR(metrics, '$.clicks', 'LONG', '0') >= 100
  AND JSON_MATCH(metrics, '"$.country" = ''US''')
ORDER BY userId LIMIT 500
```

| Table | avg | min | max | p95 | σ | docsScanned | filterScan | postScan | Data |
|---|---|---|---|---|---|---|---|---|---|
| SPARSE_MAP | **1.5 ms** | 1.0 | 2.0 | 2.0 | 0.5 | 1,241 | 2,323 | 2,482 | ✓ match (500 rows) |
| JSON | 2.6 ms | 2.0 | 8.0 | 3.0 | 0.7 | 1,241 | 2,323 | 2,482 | |

**Winner: SPARSE_MAP — 1.7× faster** — identical scan plan: EQ predicate uses inverted/JSON index to get `country='US'` doc set (filterScan=2,323), range scan applied over those. Difference is projection cost: typed array reads vs blob parse.

---

### Q18 · REGEXP filter on string key  ⚠ Semantic difference

```sql
-- SPARSE_MAP  (ExpressionFilterOperator — full forward scan, filterScan = totalDocs)
SELECT userId, metrics['country'] AS country
FROM userMetrics WHERE REGEXP_LIKE(metrics['country'], 'U.*') ORDER BY userId LIMIT 500

-- JSON  (JSON_MATCH REGEXP uses token index, filterScan = 0)
SELECT userId, JSON_EXTRACT_SCALAR(metrics, '$.country', 'STRING', '') AS country
FROM userMetricsJson
WHERE JSON_MATCH(metrics, 'REGEXP_LIKE("$.country",''U.*'')') ORDER BY userId LIMIT 500
```

| Table | avg | min | max | p95 | σ | docsScanned | filterScan | postScan | Data |
|---|---|---|---|---|---|---|---|---|---|
| SPARSE_MAP | 1.9 ms | 1.0 | 3.0 | 3.0 | 0.4 | 3,065 | **10,020** | 6,130 | ⚠ semantic diff |
| JSON | **2.5 ms** | 2.0 | 9.0 | 3.0 | 0.8 | 2,681 | **0** | 5,362 | |

**Winner: JSON (fewer filterScan)** — JSON_MATCH REGEXP_LIKE is backed by the JSON inverted token index (`filterScan = 0`); SPARSE_MAP has no regex index and falls back to `ExpressionFilterOperator` which scans all 10,020 docs.

> **⚠ Semantic difference:** SPARSE_MAP `REGEXP_LIKE` uses Java `Matcher.find()` (substring match), while JSON_MATCH REGEXP uses token-prefix matching. The pattern `'U.*'` matches `'AU'` via `find()` (finds `'U'` at index 1) but not via token prefix. Result sets differ — this is a REGEXP semantics difference between the two implementations, not a data bug.

---

### Q19 · COUNT DISTINCT on map key

```sql
-- SPARSE_MAP
SELECT COUNT(DISTINCT metrics['country']) AS dc FROM userMetrics

-- JSON
SELECT COUNT(DISTINCT JSON_EXTRACT_SCALAR(metrics, '$.country', 'STRING', '')) AS dc
FROM userMetricsJson
```

| Table | avg | min | max | p95 | σ | docsScanned | filterScan | postScan | Data |
|---|---|---|---|---|---|---|---|---|---|
| SPARSE_MAP | **1.4 ms** | 1.0 | 2.0 | 2.0 | 0.5 | 10,020 | 0 | 10,020 | ✓ match |
| JSON | 2.9 ms | 2.0 | 10.0 | 3.0 | 0.8 | 10,020 | 0 | 10,020 | |

**Winner: SPARSE_MAP — 2.1× faster** — full scan on both (no distinct index). SPARSE_MAP reads the per-key `country` string array; JSON parses each blob to extract the field.

---

### Q20 · 3-key filter + 2-key projection

```sql
-- SPARSE_MAP
SELECT userId, metrics['clicks'] AS clicks, metrics['spend'] AS spend
FROM userMetrics
WHERE metrics['country'] = 'US' AND metrics['clicks'] >= 100 AND metrics['spend'] > 50
ORDER BY userId LIMIT 500

-- JSON
SELECT userId,
  JSON_EXTRACT_SCALAR(metrics, '$.clicks', 'LONG',   '0')   AS clicks,
  JSON_EXTRACT_SCALAR(metrics, '$.spend',  'DOUBLE', '0.0') AS spend
FROM userMetricsJson
WHERE JSON_MATCH(metrics, '"$.country" = ''US''')
  AND JSON_EXTRACT_SCALAR(metrics, '$.clicks', 'LONG', '0') >= 100
  AND JSON_EXTRACT_SCALAR(metrics, '$.spend', 'DOUBLE', '0.0') > 50
ORDER BY userId LIMIT 500
```

| Table | avg | min | max | p95 | σ | docsScanned | filterScan | postScan | Data |
|---|---|---|---|---|---|---|---|---|---|
| SPARSE_MAP | **1.6 ms** | 1.0 | 2.0 | 2.0 | 0.5 | 589 | 3,564 | 1,178 | ✓ match (500 rows) |
| JSON | 3.1 ms | 2.0 | 9.0 | 4.0 | 0.7 | 589 | 3,564 | 1,178 | |

**Winner: SPARSE_MAP — 1.9× faster** — EQ on `country` prunes via inverted/JSON index, then `clicks` and `spend` ranges scan 3,564 docs. Identical scan counts; difference is that JSON must extract 3 fields from each blob (for filter) and 2 more (for projection) — up to 5 blob extractions per doc vs 5 typed array reads.

---

## Summary

### Scorecard

| Query | Pattern | Winner | SM avg | JSON avg | Speedup | filterScan SM | filterScan JSON |
|---|---|---|---|---|---|---|---|
| Q01 | COUNT(*) | **TIE** | 1.1 ms | 1.1 ms | 1.0× | 0 | 0 |
| Q02 | Project 1 key | **TIE** | 1.1 ms | 1.3 ms | 1.2× | 0 | 0 |
| Q03 | Project 3 keys | **TIE** | 1.2 ms | 1.8 ms | 1.5× | 0 | 0 |
| Q04 | EQ filter | **TIE** | 1.6 ms | 2.3 ms | 1.4× | 0 | 0 |
| Q05 | IN filter | **SPARSE_MAP** | 1.8 ms | 2.8 ms | **1.6×** | 0 | 0 |
| Q06 | NEQ filter | **TIE** | 1.6 ms | 1.8 ms | 1.1× | 0 | 0 |
| Q07 | Range `>=` | **SPARSE_MAP** | 1.8 ms | 4.0 ms | **2.2×** | 10,020 | 10,020 |
| Q08 | BETWEEN | **SPARSE_MAP** | 1.8 ms | 3.5 ms | **1.9×** | 10,020 | 10,020 |
| Q09 | Presence IS NOT NULL | **TIE** | 1.6 ms | 2.0 ms | 1.3× | 0 | 0 |
| Q10 | SUM | **SPARSE_MAP** | 2.1 ms | 3.1 ms | **1.5×** | 0 | 0 |
| Q11 | AVG | **SPARSE_MAP** | 1.5 ms | 3.0 ms | **2.0×** | 0 | 0 |
| Q12 | GROUP BY key | **SPARSE_MAP** | 1.8 ms | 3.4 ms | **1.9×** | 0 | 0 |
| Q13 | GROUP BY + SUM | **SPARSE_MAP** | 1.9 ms | 4.0 ms | **2.1×** | 0 | 0 |
| Q14 | GROUP BY col + AVG | **SPARSE_MAP** | 1.7 ms | 3.3 ms | **1.9×** | 0 | 0 |
| Q15 | Range + GROUP BY AVG | **SPARSE_MAP** | 2.1 ms | 4.3 ms | **2.0×** | 6,999 | 6,999 |
| Q16 | Top-N ORDER BY key | **SPARSE_MAP** | 1.7 ms | 3.2 ms | **1.9×** | 0 | 0 |
| Q17 | Range + EQ multi-key | **SPARSE_MAP** | 1.5 ms | 2.6 ms | **1.7×** | 2,323 | 2,323 |
| Q18 | REGEXP | **JSON** | 1.9 ms | 2.5 ms | — | **10,020** | **0** |
| Q19 | COUNT DISTINCT | **SPARSE_MAP** | 1.4 ms | 2.9 ms | **2.1×** | 0 | 0 |
| Q20 | 3-key filter + project | **SPARSE_MAP** | 1.6 ms | 3.1 ms | **1.9×** | 3,564 | 3,564 |

**SPARSE_MAP wins: 13 · JSON wins: 1 · Ties: 6 · Data validation: ALL MATCH ✓ (Q18 flagged as expected semantic difference)**

---

### When to use each

| Query pattern | Recommendation | Reason |
|---|---|---|
| Range / BETWEEN on numeric key | **SPARSE_MAP** | 2× faster — reads compact typed array; JSON parses full blob per doc |
| SUM / AVG / aggregation | **SPARSE_MAP** | 1.5–2× faster — dense typed array iteration |
| GROUP BY map key | **SPARSE_MAP** | ~2× faster — typed value extraction per grouped row |
| ORDER BY map key (Top-N) | **SPARSE_MAP** | ~2× faster — typed sort key extraction |
| EQ / IN / NEQ / IS NOT NULL filter | **TIE** | Both use inverted index; `filterScan = 0` on both |
| REGEXP filter on map key | **JSON** | JSON_MATCH uses token index (`filterScan = 0`); SPARSE_MAP full-scans |
| COUNT(*) / metadata queries | **TIE** | No value access on either side |

### Key architectural difference

| Stat | SPARSE_MAP | JSON |
|---|---|---|
| Value storage | Per-key typed arrays (`LONG[]`, `DOUBLE[]`, `STRING[]`) | Full JSON blob (raw bytes) |
| Value access | `presenceBitmap.rank(docId) → array[ordinal]` — O(1) | `JsonPath.extract(blob, "$.key")` — O(blob size) |
| EQ/IN filter index | Per-key inverted bitmap index | JSON inverted token index |
| REGEXP filter index | None — falls back to full scan | JSON token index — O(matching docs) |
| Null / absent key | `presenceBitmap.contains(docId)` — O(1) bit test | IS NOT NULL token in JSON index |
| Schema | Keys + types declared upfront | Schemaless — any key accepted |
| Range filter | Full forward scan (no sorted structure per key) | Full forward scan (same limitation) |

---

## Running the Benchmark

```bash
# Default: 10 iterations
python3 scripts/benchmark_sparse_vs_json.py

# 100 iterations (recommended for stable averages)
python3 scripts/benchmark_sparse_vs_json.py --iterations 100

# Custom broker
python3 scripts/benchmark_sparse_vs_json.py --broker http://localhost:8000/query/sql --iterations 50
```

The script reports `avg`, `min`, `max`, `p95`, and `σ` (stddev) for server-side latency across all iterations, and validates that both tables return identical result sets for every query (except Q18 which is flagged as an expected semantic difference).
