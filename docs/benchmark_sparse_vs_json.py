#!/usr/bin/env python3
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

"""
Benchmark: SPARSE_MAP vs JSON column type
Runs each query ITERATIONS times, reports avg/min/max/p95/stddev,
and validates that both tables return identical result sets.
"""

import json, urllib.request, time, statistics, sys, argparse

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BROKER     = "http://localhost:8000/query/sql"
ITERATIONS = 10

# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------
def jlong(k, d="0"):   return f"JSON_EXTRACT_SCALAR(metrics,'$.{k}','LONG','{d}')"
def jdbl(k,  d="0.0"): return f"JSON_EXTRACT_SCALAR(metrics,'$.{k}','DOUBLE','{d}')"
def jstr(k,  d=""):    return f"JSON_EXTRACT_SCALAR(metrics,'$.{k}','STRING','{d}')"


# ---------------------------------------------------------------------------
# Query pairs  (label, sparse_map_sql, json_sql)
# ---------------------------------------------------------------------------
QUERIES = [
    (
        "Q01 COUNT(*)",
        "SELECT COUNT(*) FROM userMetrics",
        "SELECT COUNT(*) FROM userMetricsJson",
    ),
    (
        "Q02 Project single key (clicks)",
        "SELECT userId, metrics['clicks'] FROM userMetrics ORDER BY userId LIMIT 50",
        f"SELECT userId, {jlong('clicks')} AS clicks FROM userMetricsJson ORDER BY userId LIMIT 50",
    ),
    (
        "Q03 Project 3 keys",
        "SELECT userId, metrics['clicks'], metrics['spend'], metrics['country'] FROM userMetrics ORDER BY userId LIMIT 50",
        f"SELECT userId, {jlong('clicks')} AS clicks, {jdbl('spend')} AS spend, {jstr('country')} AS country FROM userMetricsJson ORDER BY userId LIMIT 50",
    ),
    (
        "Q04 EQ filter: country='US'",
        "SELECT userId, metrics['clicks'] FROM userMetrics WHERE metrics['country'] = 'US' ORDER BY userId LIMIT 500",
        f"SELECT userId, {jlong('clicks')} AS clicks FROM userMetricsJson WHERE JSON_MATCH(metrics,'\"$.country\" = ''US''') ORDER BY userId LIMIT 500",
    ),
    (
        "Q05 IN filter: country IN (US,DE,JP)",
        "SELECT userId, metrics['country'] FROM userMetrics WHERE metrics['country'] IN ('US','DE','JP') ORDER BY userId LIMIT 500",
        f"SELECT userId, {jstr('country')} AS country FROM userMetricsJson WHERE JSON_MATCH(metrics,'\"$.country\" IN (''US'',''DE'',''JP'')') ORDER BY userId LIMIT 500",
    ),
    (
        "Q06 NEQ filter: country != 'US'",
        "SELECT userId FROM userMetrics WHERE metrics['country'] != 'US' AND metrics['country'] != '' ORDER BY userId LIMIT 500",
        f"SELECT userId FROM userMetricsJson WHERE JSON_MATCH(metrics,'\"$.country\" != ''US''') ORDER BY userId LIMIT 500",
    ),
    (
        "Q07 Range: clicks >= 200",
        "SELECT userId, metrics['clicks'] FROM userMetrics WHERE metrics['clicks'] >= 200 ORDER BY userId LIMIT 500",
        f"SELECT userId, {jlong('clicks')} AS clicks FROM userMetricsJson WHERE {jlong('clicks')} >= 200 ORDER BY userId LIMIT 500",
    ),
    (
        "Q08 BETWEEN: spend 50-200",
        "SELECT userId, metrics['spend'] FROM userMetrics WHERE metrics['spend'] BETWEEN 50 AND 200 ORDER BY userId LIMIT 500",
        f"SELECT userId, {jdbl('spend')} AS spend FROM userMetricsJson WHERE {jdbl('spend')} BETWEEN 50 AND 200 ORDER BY userId LIMIT 500",
    ),
    (
        "Q09 Presence: country IS NOT NULL",
        "SELECT userId FROM userMetrics WHERE metrics['country'] != '' ORDER BY userId LIMIT 500",
        f"SELECT userId FROM userMetricsJson WHERE JSON_MATCH(metrics,'\"$.country\" IS NOT NULL') ORDER BY userId LIMIT 500",
    ),
    (
        "Q10 SUM(clicks)",
        "SELECT SUM(metrics['clicks']) AS tc FROM userMetrics",
        f"SELECT SUM({jlong('clicks')}) AS tc FROM userMetricsJson",
    ),
    (
        "Q11 AVG(spend)",
        "SELECT ROUND(AVG(metrics['spend']),4) AS avg FROM userMetrics",
        f"SELECT ROUND(AVG({jdbl('spend')}),4) AS avg FROM userMetricsJson",
    ),
    (
        "Q12 GROUP BY country",
        "SELECT metrics['country'] AS country, COUNT(*) AS cnt FROM userMetrics WHERE metrics['country'] != '' GROUP BY metrics['country'] ORDER BY country",
        f"SELECT {jstr('country')} AS country, COUNT(*) AS cnt FROM userMetricsJson WHERE JSON_MATCH(metrics,'\"$.country\" IS NOT NULL') GROUP BY country ORDER BY country",
    ),
    (
        "Q13 GROUP BY country + SUM(clicks)",
        "SELECT metrics['country'] AS country, SUM(metrics['clicks']) AS tc FROM userMetrics WHERE metrics['country'] != '' GROUP BY metrics['country'] ORDER BY country",
        f"SELECT {jstr('country')} AS country, SUM({jlong('clicks')}) AS tc FROM userMetricsJson WHERE JSON_MATCH(metrics,'\"$.country\" IS NOT NULL') GROUP BY country ORDER BY country",
    ),
    (
        "Q14 GROUP BY region + AVG(clicks)",
        "SELECT region, ROUND(AVG(metrics['clicks']),4) AS avg FROM userMetrics GROUP BY region ORDER BY region",
        f"SELECT region, ROUND(AVG({jlong('clicks')}),4) AS avg FROM userMetricsJson GROUP BY region ORDER BY region",
    ),
    (
        "Q15 Range+EQ GROUP BY AVG(spend)",
        "SELECT metrics['country'] AS country, ROUND(AVG(metrics['spend']),4) AS avg FROM userMetrics WHERE metrics['clicks'] >= 50 AND metrics['country'] != '' GROUP BY metrics['country'] ORDER BY country",
        f"SELECT {jstr('country')} AS country, ROUND(AVG({jdbl('spend')}),4) AS avg FROM userMetricsJson WHERE {jlong('clicks')} >= 50 AND JSON_MATCH(metrics,'\"$.country\" IS NOT NULL') GROUP BY country ORDER BY country",
    ),
    (
        "Q16 Top-5 ORDER BY clicks DESC",
        "SELECT userId, region, metrics['clicks'] AS clicks FROM userMetrics ORDER BY metrics['clicks'] DESC, userId ASC LIMIT 5",
        f"SELECT userId, region, {jlong('clicks')} AS clicks FROM userMetricsJson ORDER BY {jlong('clicks')} DESC, userId ASC LIMIT 5",
    ),
    (
        "Q17 Multi-key: clicks>=100 AND country='US'",
        "SELECT userId, metrics['clicks'] AS clicks FROM userMetrics WHERE metrics['clicks'] >= 100 AND metrics['country'] = 'US' ORDER BY userId LIMIT 500",
        f"SELECT userId, {jlong('clicks')} AS clicks FROM userMetricsJson WHERE {jlong('clicks')} >= 100 AND JSON_MATCH(metrics,'\"$.country\" = ''US''') ORDER BY userId LIMIT 500",
    ),
    (
        "Q18 REGEXP: country ~ 'U.*'  [semantic-diff]",
        "SELECT userId, metrics['country'] AS country FROM userMetrics WHERE REGEXP_LIKE(metrics['country'], 'U.*') ORDER BY userId LIMIT 500",
        f"SELECT userId, {jstr('country')} AS country FROM userMetricsJson WHERE JSON_MATCH(metrics,'REGEXP_LIKE(\"$.country\",''U.*'')') ORDER BY userId LIMIT 500",
    ),
    (
        "Q19 COUNT DISTINCT country",
        "SELECT COUNT(DISTINCT metrics['country']) AS dc FROM userMetrics",
        f"SELECT COUNT(DISTINCT {jstr('country')}) AS dc FROM userMetricsJson",
    ),
    (
        "Q20 3-key filter, 2-key project",
        "SELECT userId, metrics['clicks'] AS clicks, metrics['spend'] AS spend FROM userMetrics WHERE metrics['country'] = 'US' AND metrics['clicks'] >= 100 AND metrics['spend'] > 50 ORDER BY userId LIMIT 500",
        f"SELECT userId, {jlong('clicks')} AS clicks, {jdbl('spend')} AS spend FROM userMetricsJson WHERE JSON_MATCH(metrics,'\"$.country\" = ''US''') AND {jlong('clicks')} >= 100 AND {jdbl('spend')} > 50 ORDER BY userId LIMIT 500",
    ),
]


# ---------------------------------------------------------------------------
# Execute one query, return (rows, stats_dict, error_str)
# ---------------------------------------------------------------------------
def run_query(sql):
    data = json.dumps({"sql": sql}).encode()
    req  = urllib.request.Request(BROKER, data=data,
                                  headers={"Content-Type": "application/json"})
    t0 = time.perf_counter()
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
    wall_ms = (time.perf_counter() - t0) * 1000

    r   = json.loads(raw)
    exc = r.get("exceptions", [])
    if exc:
        return None, {}, exc[0].get("message", "?")[:120]

    rows = r.get("resultTable", {}).get("rows", [])
    stats = {
        "wall_ms": wall_ms,
        "srv_ms":  r.get("timeUsedMs", 0),
        "docs":    r.get("numDocsScanned", 0),
        "filter":  r.get("numEntriesScannedInFilter", 0),
        "post":    r.get("numEntriesScannedPostFilter", 0),
        "total":   r.get("totalDocs", 0),
        "segs":    r.get("numSegmentsQueried", 0),
    }
    return rows, stats, None


# ---------------------------------------------------------------------------
# Run N iterations, collect timing + final stats snapshot
# ---------------------------------------------------------------------------
def bench(sql, n=ITERATIONS):
    wall_times, srv_times = [], []
    last_rows, last_stats, last_err = None, {}, None

    for _ in range(n):
        rows, stats, err = run_query(sql)
        if err:
            last_err = err
            break
        wall_times.append(stats["wall_ms"])
        srv_times.append(stats["srv_ms"])
        last_rows  = rows
        last_stats = stats

    if last_err:
        return None, {}, last_err

    def agg(lst):
        return {
            "avg":   statistics.mean(lst),
            "min":   min(lst),
            "max":   max(lst),
            "p95":   sorted(lst)[int(len(lst) * 0.95)],
            "stdev": statistics.stdev(lst) if len(lst) > 1 else 0,
        }

    summary = {
        "wall": agg(wall_times),
        "srv":  agg(srv_times),
        "docs":   last_stats.get("docs", 0),
        "filter": last_stats.get("filter", 0),
        "post":   last_stats.get("post", 0),
        "total":  last_stats.get("total", 0),
        "segs":   last_stats.get("segs", 0),
        "n":      n,
    }
    return last_rows, summary, None


# ---------------------------------------------------------------------------
# Data validation helpers
# ---------------------------------------------------------------------------
def normalise_row(row):
    """Round floats to 2dp so DOUBLE precision differences don't cause mismatches."""
    out = []
    for v in row:
        if isinstance(v, float):
            out.append(round(v, 2))
        else:
            out.append(v)
    return tuple(out)

def validate(sm_rows, js_rows, label):
    if sm_rows is None or js_rows is None:
        return "SKIP (error)"
    if "[semantic-diff]" in label:
        return f"SKIP — semantic difference expected (SM={len(sm_rows)} rows, JSON={len(js_rows)} rows)"
    sm_set = sorted([normalise_row(r) for r in sm_rows])
    js_set = sorted([normalise_row(r) for r in js_rows])
    if sm_set == js_set:
        return f"OK ({len(sm_set)} rows match)"
    # compute diff
    sm_only = [r for r in sm_set if r not in js_set]
    js_only = [r for r in js_set if r not in sm_set]
    detail = f"MISMATCH — SM_only:{len(sm_only)} JSON_only:{len(js_only)}"
    if sm_only:
        detail += f" | SM sample: {sm_only[:2]}"
    if js_only:
        detail += f" | JSON sample: {js_only[:2]}"
    return detail


# ---------------------------------------------------------------------------
# Printing helpers
# ---------------------------------------------------------------------------
SEP  = "=" * 148
DASH = "-" * 148

def hdr(s): print(f"\n{SEP}\n{s}\n{SEP}")

def row_line(label, table, n, srv, docs, filt, post, total, validation=""):
    avg, mn, mx, p95, sd = srv["avg"], srv["min"], srv["max"], srv["p95"], srv["stdev"]
    print(f"  {label:<36} {table:<12} {n:>3}x │ "
          f"avg={avg:6.1f}ms  min={mn:5.1f}  max={mx:5.1f}  p95={p95:5.1f}  σ={sd:4.1f} │ "
          f"docs={docs:>6}  filter={filt:>6}  post={post:>6}  total={total:>6} │ {validation}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    global BROKER, ITERATIONS
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=ITERATIONS)
    parser.add_argument("--broker", default=BROKER)
    args = parser.parse_args()

    BROKER     = args.broker
    ITERATIONS = args.iterations

    print(f"\nBenchmark: SPARSE_MAP vs JSON  |  {ITERATIONS} iterations per query  |  broker={BROKER}")

    sm_wins = json_wins = ties = errors = 0
    all_valid = True
    summary_lines = []

    for label, sm_sql, json_sql in QUERIES:
        hdr(label)
        print(f"  {'Table':<50} {'itr':>3}x │ {'avg':>8}  {'min':>7}  {'max':>7}  {'p95':>7}  {'σ':>6} │ "
              f"{'docs':>10}  {'filter':>8}  {'post':>8}  {'total':>8} │ validation")
        print(f"  {'-'*144}")

        sm_rows,  sm_s,  sm_err  = bench(sm_sql,   ITERATIONS)
        js_rows,  js_s,  js_err  = bench(json_sql,  ITERATIONS)

        validation = validate(sm_rows, js_rows, label)
        if "MISMATCH" in validation:
            all_valid = False
        if sm_err:
            print(f"  {'SPARSE_MAP':<50} ERROR: {sm_err}")
            errors += 1
        else:
            row_line(label, "SPARSE_MAP", sm_s["n"], sm_s["srv"],
                     sm_s["docs"], sm_s["filter"], sm_s["post"], sm_s["total"],
                     validation)

        if js_err:
            print(f"  {'JSON':<50} ERROR: {js_err}")
            errors += 1
        else:
            row_line("", "JSON", js_s["n"], js_s["srv"],
                     js_s["docs"], js_s["filter"], js_s["post"], js_s["total"])

        # determine winner
        if sm_err or js_err:
            winner = "ERROR"
            errors += 1
        else:
            sm_filter = sm_s["filter"]
            js_filter = js_s["filter"]
            sm_avg    = sm_s["srv"]["avg"]
            js_avg    = js_s["srv"]["avg"]

            if sm_filter < js_filter:
                winner = "SPARSE_MAP (fewer filterScan)"
                sm_wins += 1
            elif js_filter < sm_filter:
                winner = "JSON       (fewer filterScan)"
                json_wins += 1
            elif abs(sm_avg - js_avg) < 1.0:
                winner = "TIE        (< 1 ms diff)"
                ties += 1
            elif sm_avg < js_avg:
                winner = f"SPARSE_MAP (avg {sm_avg:.1f} vs {js_avg:.1f} ms)"
                sm_wins += 1
            else:
                winner = f"JSON       (avg {js_avg:.1f} vs {sm_avg:.1f} ms)"
                json_wins += 1

        print(f"\n  ► Winner: {winner}   |   Data: {validation}")
        summary_lines.append((label, winner, validation,
                               sm_s.get("srv", {}), js_s.get("srv", {}),
                               sm_s.get("filter", "-"), js_s.get("filter", "-")))

    # -----------------------------------------------------------------------
    # Final summary table
    # -----------------------------------------------------------------------
    print(f"\n\n{'#'*148}")
    print("FINAL SUMMARY")
    print(f"{'#'*148}")
    print(f"  {'Query':<40} {'Winner':<40} {'SM avg':>8}  {'JS avg':>8}  {'SM filter':>9}  {'JS filter':>9}  {'Data'}")
    print(f"  {'-'*144}")
    for label, winner, validation, sm_srv, js_srv, sm_f, js_f in summary_lines:
        sm_avg = f"{sm_srv.get('avg',0):.1f}" if sm_srv else "-"
        js_avg = f"{js_srv.get('avg',0):.1f}" if js_srv else "-"
        print(f"  {label:<40} {winner:<40} {sm_avg:>8}  {js_avg:>8}  {str(sm_f):>9}  {str(js_f):>9}  {validation}")

    print(f"\n  {'='*60}")
    print(f"  SPARSE_MAP wins : {sm_wins}")
    print(f"  JSON wins       : {json_wins}")
    print(f"  Ties            : {ties}")
    print(f"  Errors          : {errors}")
    print(f"  Data validation : {'ALL MATCH ✓' if all_valid else 'MISMATCHES FOUND ✗'}")
    print(f"  {'='*60}\n")

    sys.exit(0 if all_valid and errors == 0 else 1)


if __name__ == "__main__":
    main()
