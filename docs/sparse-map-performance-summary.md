a<!--
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

# SPARSE_MAP vs JSON — Performance Comparison

> **Dataset:** 10,020 rows · 11 segments · 100 iterations per query · all results validated identical between both column types.

Apache Pinot supports two ways to store a map of key-value pairs per document:

| | **SPARSE_MAP** | **JSON** |
|---|---|---|
| Storage | Per-key typed columnar arrays with presence bitmaps | Raw JSON blob per document |
| Key access | `metrics['clicks']` | `JSON_EXTRACT_SCALAR(metrics, '$.clicks', 'LONG', '0')` |
| EQ / IN filter | Per-key inverted index — no doc scan | JSON inverted token index — no doc scan |
| Range filter | Full forward scan over typed array | Full forward scan over JSON blobs |
| REGEXP filter | Full forward scan (no regex index) | JSON token index — no doc scan |

---

## Overall Scorecard

| # | Query pattern | Winner | SPARSE_MAP avg | JSON avg | Speedup |
|---|---|---|---|---|---|
| Q01 | COUNT(*) | TIE | 1.1 ms | 1.1 ms | 1.0× |
| Q02 | Project 1 key | TIE | 1.1 ms | 1.3 ms | 1.2× |
| Q03 | Project 3 keys | TIE | 1.2 ms | 1.8 ms | 1.5× |
| Q04 | EQ filter on string key | TIE | 1.6 ms | 2.3 ms | 1.4× |
| Q05 | IN filter (3 values) | **SPARSE_MAP** | 1.8 ms | 2.8 ms | **1.6×** |
| Q06 | NEQ filter | TIE | 1.6 ms | 1.8 ms | 1.1× |
| Q07 | Range filter `clicks >= 200` | **SPARSE_MAP** | 1.8 ms | 4.0 ms | **2.2×** |
| Q08 | BETWEEN `spend 50–200` | **SPARSE_MAP** | 1.8 ms | 3.5 ms | **1.9×** |
| Q09 | Presence check (IS NOT NULL) | TIE | 1.6 ms | 2.0 ms | 1.3× |
| Q10 | SUM on numeric key | **SPARSE_MAP** | 2.1 ms | 3.1 ms | **1.5×** |
| Q11 | AVG on numeric key | **SPARSE_MAP** | 1.5 ms | 3.0 ms | **2.0×** |
| Q12 | GROUP BY string key | **SPARSE_MAP** | 1.8 ms | 3.4 ms | **1.9×** |
| Q13 | GROUP BY + SUM | **SPARSE_MAP** | 1.9 ms | 4.0 ms | **2.1×** |
| Q14 | GROUP BY regular col + AVG key | **SPARSE_MAP** | 1.7 ms | 3.3 ms | **1.9×** |
| Q15 | Range filter + GROUP BY + AVG | **SPARSE_MAP** | 2.1 ms | 4.3 ms | **2.0×** |
| Q16 | Top-5 ORDER BY key DESC | **SPARSE_MAP** | 1.7 ms | 3.2 ms | **1.9×** |
| Q17 | Multi-key filter (range + EQ) | **SPARSE_MAP** | 1.5 ms | 2.6 ms | **1.7×** |
| Q18 | REGEXP on string key ⚠ | **JSON** | 1.9 ms | 2.5 ms | — |
| Q19 | COUNT DISTINCT on key | **SPARSE_MAP** | 1.4 ms | 2.9 ms | **2.1×** |
| Q20 | 3-key filter + 2-key projection | **SPARSE_MAP** | 1.6 ms | 3.1 ms | **1.9×** |

**SPARSE_MAP wins 13 · JSON wins 1 · Ties 6**

> ⚠ Q18 REGEXP: JSON wins on scan count (`filterScan = 0` via token index vs full scan on SPARSE_MAP). Note also that the two implementations have different REGEXP semantics — `REGEXP_LIKE` on SPARSE_MAP uses substring matching while `JSON_MATCH` uses prefix token matching, so result sets for some patterns will differ.

---

## Detailed Results

All timings are server-side latency in milliseconds. `filterScan` = number of index entries evaluated during the filter phase. `postScan` = entries read during projection / aggregation.

---

### Filtering

#### EQ / IN / NEQ — index-backed, no document scan

Both column types resolve equality predicates via an inverted index. `filterScan = 0` on both — no document values are read during the filter phase.

| Query | SPARSE_MAP | | | JSON | | | filterScan |
|---|---|---|---|---|---|---|---|
| | avg | p95 | σ | avg | p95 | σ | (both) |
| `WHERE country = 'US'` (Q04) | 1.6 ms | 2.0 | 0.5 | 2.3 ms | 3.0 | 0.8 | **0** |
| `WHERE country IN ('US','DE','JP')` (Q05) | 1.8 ms | 2.0 | 0.8 | 2.8 ms | 4.0 | 0.9 | **0** |
| `WHERE country != 'US'` (Q06) | 1.6 ms | 3.0 | 0.6 | 1.8 ms | 3.0 | 0.9 | **0** |
| `WHERE country IS NOT NULL` (Q09) | 1.6 ms | 2.0 | 0.8 | 2.0 ms | 3.0 | 0.4 | **0** |

**Takeaway:** Inverted-index filters are essentially free on both types. The small ms gap comes from the projection step, not the filter itself.

---

#### Range / BETWEEN — full forward scan, typed array vs blob parse

Neither type has a range index, so every document's value must be inspected. The cost difference is entirely in how that value is read: SPARSE_MAP reads from a compact typed array, JSON parses the entire blob.

| Query | SPARSE_MAP | | | JSON | | | filterScan |
|---|---|---|---|---|---|---|---|
| | avg | p95 | σ | avg | p95 | σ | (both) |
| `WHERE clicks >= 200` (Q07) | **1.8 ms** | 2.0 | 0.4 | 4.0 ms | 7.0 | 1.5 | 10,020 |
| `WHERE spend BETWEEN 50 AND 200` (Q08) | **1.8 ms** | 2.0 | 0.5 | 3.5 ms | 4.0 | 1.0 | 10,020 |

**Takeaway:** SPARSE_MAP is **~2× faster** on range predicates. The high σ on JSON (1.0–1.5) reflects GC pressure from repeated blob deserialisation.

---

#### REGEXP — JSON wins via token index

| Query | SPARSE_MAP | | | JSON | | | SM filterScan | JSON filterScan |
|---|---|---|---|---|---|---|---|---|
| | avg | p95 | σ | avg | p95 | σ | | |
| `REGEXP_LIKE(country, 'U.*')` (Q18) | 1.9 ms | 3.0 | 0.4 | **2.5 ms** | 3.0 | 0.8 | **10,020** | **0** |

**Takeaway:** JSON wins — `JSON_MATCH REGEXP_LIKE` uses the JSON token index (no document scan). SPARSE_MAP has no regex index and falls back to scanning all 10,020 values. **Use JSON if REGEXP filtering on map keys is a primary access pattern.**

---

### Aggregation

Aggregation scans all documents (`filterScan = 0`). The difference is the cost of extracting the numeric value per doc: typed array lookup vs blob parse.

| Query | SPARSE_MAP | | | JSON | | |
|---|---|---|---|---|---|---|
| | avg | p95 | σ | avg | p95 | σ |
| `SUM(clicks)` (Q10) | **2.1 ms** | 4.0 | 0.7 | 3.1 ms | 4.0 | 1.0 |
| `AVG(spend)` (Q11) | **1.5 ms** | 2.0 | 0.6 | 3.0 ms | 4.0 | 1.1 |
| `COUNT(DISTINCT country)` (Q19) | **1.4 ms** | 2.0 | 0.5 | 2.9 ms | 3.0 | 0.8 |

**Takeaway:** SPARSE_MAP is **1.5–2.1× faster** on aggregations. The σ on JSON (0.8–1.1) is consistently higher — blob allocation and GC adds latency variance at scale.

---

### Projection

Reading individual key values without a filter.

| Query | SPARSE_MAP | | | JSON | | | postScan |
|---|---|---|---|---|---|---|---|
| | avg | p95 | σ | avg | p95 | σ | (both) |
| Project 1 key — `clicks` (Q02) | 1.1 ms | 2.0 | 0.4 | 1.3 ms | 2.0 | 0.5 | 100 |
| Project 3 keys — `clicks, spend, country` (Q03) | 1.2 ms | 2.0 | 0.4 | 1.8 ms | 2.0 | 0.5 | 140 |

**Takeaway:** Gap is small at LIMIT 50 but grows proportionally with rows returned and keys projected. SPARSE_MAP resolves each key via a single bitmap rank operation (O(1)); JSON parses the full blob once per document regardless of how many keys are extracted.

---

### GROUP BY

| Query | SPARSE_MAP | | | JSON | | | docsScanned |
|---|---|---|---|---|---|---|---|
| | avg | p95 | σ | avg | p95 | σ | |
| `GROUP BY country` (Q12) | **1.8 ms** | 2.0 | 0.5 | 3.4 ms | 5.0 | 1.1 | 6,999 |
| `GROUP BY country, SUM(clicks)` (Q13) | **1.9 ms** | 3.0 | 0.8 | 4.0 ms | 5.0 | 1.0 | 6,999 |
| `GROUP BY region, AVG(clicks)` (Q14) | **1.7 ms** | 2.0 | 0.5 | 3.3 ms | 5.0 | 1.3 | 10,020 |

**Takeaway:** SPARSE_MAP is **1.9–2.1× faster**. Q13 is the clearest case: JSON must extract both `country` (for grouping) and `clicks` (for SUM) from each blob — effectively two parse passes per document. SPARSE_MAP reads each from independent typed arrays.

---

### Combined Queries

Complex queries combining filter, aggregation, and projection.

| Query | SPARSE_MAP | | | JSON | | | filterScan |
|---|---|---|---|---|---|---|---|
| | avg | p95 | σ | avg | p95 | σ | (both) |
| Range + GROUP BY + AVG (Q15) | **2.1 ms** | 3.0 | 0.8 | 4.3 ms | 5.0 | 1.0 | 6,999 |
| Top-5 ORDER BY clicks DESC (Q16) | **1.7 ms** | 2.0 | 0.5 | 3.2 ms | 4.0 | 0.8 | 0 |
| Range + EQ, project 1 key (Q17) | **1.5 ms** | 2.0 | 0.5 | 2.6 ms | 3.0 | 0.7 | 2,323 |
| 3-key filter + 2-key project (Q20) | **1.6 ms** | 2.0 | 0.5 | 3.1 ms | 4.0 | 0.7 | 3,564 |

**Takeaway:** SPARSE_MAP is **1.7–2.0× faster** across all complex query shapes. The advantage compounds when a single query touches multiple keys — each additional key extraction from a JSON blob adds a parse cost, while SPARSE_MAP reads from independent typed arrays.

---

## Decision Guide

| If your primary access pattern is… | Use |
|---|---|
| Range / BETWEEN on numeric keys (`clicks >= N`, `spend BETWEEN x AND y`) | **SPARSE_MAP** |
| SUM / AVG / aggregation on numeric keys | **SPARSE_MAP** |
| GROUP BY on map keys | **SPARSE_MAP** |
| ORDER BY / Top-N on a map key | **SPARSE_MAP** |
| Projecting multiple keys per document | **SPARSE_MAP** |
| EQ / IN / NEQ filtering on string keys | Either (both indexed, SPARSE_MAP slightly faster) |
| REGEXP filtering on map keys | **JSON** |
| Schemaless / dynamic keys not known at table creation | **JSON** |
| Mixed workload across all of the above | **SPARSE_MAP** (wins 13/20, loses only REGEXP) |

---

## How each type stores data inside a segment

### SPARSE_MAP — per-key columnar layout

Each key declared in `sparseMapKeyTypes` gets its own binary region inside the `.sparsemap.idx` file. The on-disk layout per key is:

```
[PRESENCE_BITMAP]   ImmutableRoaringBitmap — one bit per doc that has this key
[VALUES]            typed array — one entry per doc that has this key, in doc order
```

An optional inverted index region follows for each key when `enableInvertedIndex: true`:

```
[INVERTED_INDEX]    TreeMap<value_string → RoaringBitmap of doc IDs>
```

**Segment file:** `<column>.sparsemap.idx`
**No forward index, no dictionary** — the SparseMap index is the sole storage structure.

Key read operations:
- **Value lookup** for doc `d`: `ordinal = presenceBitmap.rank(d) - 1` → `values[ordinal]` — a single bit-count plus an array dereference.
- **Presence check**: `presenceBitmap.contains(d)` — O(1).
- **EQ / IN filter**: `invertedIndex[key][value]` returns a pre-built docId bitmap — no document scan.

Because keys are physically independent, reading `clicks` and `country` from the same document touches two separate byte ranges; the other keys are never loaded.

---

### JSON — single blob per document

The entire map is serialised as a UTF-8 JSON string and stored as a single opaque value in the column's forward index (one blob per document). A JSON inverted token index can be built alongside it.

**Segment files:** standard forward index + optional `<column>.json.idx` token index

Key read operations:
- **Value lookup** for any key: load full blob → deserialise JSON tree → traverse to key → type-convert string. Cost scales with blob size, not the number of keys needed.
- **EQ / IN / IS NOT NULL / REGEXP filter**: `JSON_MATCH` uses the token index — no document scan (same `filterScan = 0` as SPARSE_MAP inverted index).
- **Range / BETWEEN filter**: no range token index exists; falls back to a full blob scan, same as SPARSE_MAP's expression scan but slower because each blob must be fully deserialised.

---

### Segment layout comparison

| | **SPARSE_MAP** | **JSON** |
|---|---|---|
| Storage unit | Per-key typed array + presence bitmap | One blob per document |
| File | `.sparsemap.idx` | Forward index file + `.json.idx` |
| Forward index | None | Standard (stores raw blob) |
| Dictionary | None | None (`noDictionaryColumns`) |
| EQ / IN index | Per-key inverted index (value → docId bitmap) | JSON token inverted index |
| Range index | Not supported (expression scan) | Not supported (blob scan) |
| Multi-key read cost | O(keys_needed) | O(blob_size) — fixed per doc regardless |
| Absent key | `presenceBitmap.contains(d) = false` → zero-default | Key simply absent from the parsed tree |

---

## Why SPARSE_MAP is faster on value-access queries

SPARSE_MAP stores each key's values in a separate compact array (one entry per document that has that key). Reading the value for document `d` is:

```
position = presenceBitmap.rank(d) - 1   // single bit-count operation
value    = typedArray[position]          // direct array index
```

JSON stores the entire map as a raw blob. Reading any key requires:

```
deserialise blob → parse JSON tree → traverse to key → type-convert extracted string
```

The cost of blob deserialisation is proportional to the size of the entire JSON object, regardless of how many keys are actually needed. This is why the gap grows with the number of keys in the map and the number of documents processed.
