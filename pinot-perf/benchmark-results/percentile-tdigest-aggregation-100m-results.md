<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# TDigest 3.2 to 3.3 Upgrade: Compatibility and Performance

- Date: 2026-07-23
- Follow-up baseline: `706b772334e`, t-digest 3.2
- Follow-up candidate: PR #19015 rebased onto the baseline, plus the path-aware accumulator optimization below
- Host: Apple M4 Pro (14 cores, 24 GiB), macOS 26.5.2 aarch64
- Runtime: OpenJDK 25, JMH 1.37

## Decision

Upgrade Pinot to t-digest 3.3 and keep Pinot's existing TDigest API and serialized state. Do not replace it with the
Apache DataSketches quantiles implementation as part of this dependency upgrade. A DataSketches replacement would
change the user-visible aggregation type and wire format and therefore needs a separate format, migration, and
mixed-version design.

Pinot explicitly selects `ScaleFunction.K_1`. T-digest 3.3 changed its default scale function, and retaining K1 avoids
the middle-quantile accuracy regression measured with the new default. Pinot also centralizes construction and
serialization, repairs legacy weighted boundary centroids, and uses a primitive accumulator for serialized and
reducer paths.

The follow-up removes the query-path regressions measured in the original PR. Raw query aggregation is 1.3% faster.
With dependency-native stored digests, the StarTree query kernels are 35.5-43.4% faster. The group-by improvement also
reduces allocation by 34.8%. Raw and direct serialized aggregation now merge incrementally at the configured public
compression when it is at least 50, avoiding a redundant final recompression. Buffered reducer inputs and compression
below 50 retain the two-level working compression for accuracy.

This does not establish that every synthetic operation is always faster. With byte-for-byte fixed verbose inputs, the
non-grouped StarTree kernel is 6.4% faster, while the 1,000-group kernel is 1.3% slower and allocates 8.6% less. That
small residual control-case regression is reported rather than hidden; every measured production-shaped `NATIVE`
query path is faster.

## User and rollout guide

For finite values produced by Pinot, the upgrade requires no SQL, schema, table-config, or segment rebuild change.
The compatibility serializer emits compact or verbose `MergingDigest` state readable by both t-digest 3.2 and 3.3,
so finite intermediate results can flow in either direction during a rolling upgrade. Existing stored TDigest values
continue to be readable. Exact bytes, centroid counts, and approximate percentile answers can change across versions.

Compression values below 10 continue to run, but t-digest 3.3 normalizes their effective compression to 10. This is
upstream 3.3 behavior. Compression 10 and above retains the configured value.

Repeated positive or negative infinity values need special care during a mixed-version rollout. Pinot's 3.3 path
preserves them, but the generic t-digest 3.2 reader can return `NaN` or incorrect middle quantiles for a structurally
valid digest containing infinity centroids. Sanitize or filter non-finite values while any 3.2 reader can consume new
partial results, or complete the reader upgrade before relying on non-finite percentile semantics. `NaN` input remains
rejected.

### Sample queries

```sql
-- Approximate P75 with the default TDigest compression of 100.
SELECT PERCENTILETDIGEST(latencyMs, 75) AS p75Approx
FROM myTable;

-- Approximate P75 with an explicit compression factor.
SELECT PERCENTILETDIGEST(latencyMs, 75, 200) AS p75Approx
FROM myTable;

-- Approximate P75 per region; a matching StarTree can serve the stored TDigest metric.
SELECT region, PERCENTILETDIGEST(latencyMs, 75) AS p75Approx
FROM myTable
GROUP BY region;
```

### Sample StarTree table config

The compression in the query must match the compression used by the StarTree function-column pair. This example uses
the default compression of 100.

```json
{
  "tableIndexConfig": {
    "starTreeIndexConfigs": [
      {
        "dimensionsSplitOrder": ["region", "deviceType"],
        "skipStarNodeCreationForDimensions": [],
        "functionColumnPairs": ["percentileTDigest__latencyMs"],
        "maxLeafRecords": 10000
      }
    ]
  }
}
```

Choose the split order and leaf size for the table's query patterns and cardinalities. The benchmark below isolates
stored-metric aggregation after traversal; it does not measure planning, StarTree traversal, forward-index I/O, or
network transfer.

## Methodology

Each follow-up comparison used independently built runtime packages containing exactly one t-digest JAR. The baseline
and candidate used matched benchmark bytecode, deterministic input, parameters, JVM, and JMH settings. T-digest 3.3
and the 3.2 control both use K1 in the benchmark helper. Reducer sources are immutable so one invocation cannot mutate
the next invocation's input.

The affected query paths were run in `3.2, 3.3, 3.3, 3.2` order to reduce machine-drift bias. Each run used one fork,
two one-second warmup iterations, five one-second measurement iterations, one thread, an 8 GiB maximum heap, and the
GC profiler. Each displayed score is therefore the mean of two runs and ten measured iterations. Dataset and
exact-oracle creation occurs outside the timed methods.

The raw aggregation and construction workloads process 100 million deterministic values. The StarTree query workload
merges 10,000 stored digests representing 100 million source rows. Its group-by case distributes them across 1,000
groups. `NATIVE` includes each dependency version's production serialization and centroid layout. `FIXED_VERBOSE`
uses the same deterministic bytes, including singleton endpoint centroids, with both versions to isolate query-time
merging from source-layout differences. The leaf-to-parent construction benchmark merges ten serialized leaves into
each parent. The reducer control uses 32 fixed verbose inputs at compression 100. The accuracy sweep uses 128 native
immutable inputs and 32 independent deterministic source orders for randomized-order cases.

The segment-construction and reducer-throughput tables are the original PR characterization against `fae8080bc7`.
They are retained as context and were not used to decide whether the follow-up fixed the affected query paths.

## Results

Positive deltas are regressions; negative deltas are improvements. Allocation is normalized bytes per benchmark
operation.

### Raw and StarTree query aggregation

| Workload | Source layout | 3.2 (ms/op) | 3.3 (ms/op) | Latency delta | 3.2 allocation | 3.3 allocation | Allocation delta |
|---|---|---:|---:|---:|---:|---:|---:|
| Raw aggregation plus P75, 100M rows | N/A | 3,377.903 | 3,334.669 | -1.3% | 15,954 | 16,018 | +0.4% |
| StarTree merge plus P75, 10K stored digests | Native | 10.763 | 6.947 | -35.5% | 10,524 | 11,573 | +10.0% |
| StarTree group-by, 1,000 groups | Native | 15.395 | 8.711 | -43.4% | 13,168,809 | 8,590,700 | -34.8% |
| StarTree merge plus P75, 10K stored digests | Fixed verbose | 11.200 | 10.479 | -6.4% | 10,528 | 11,488 | +9.1% |
| StarTree group-by, 1,000 groups | Fixed verbose | 14.674 | 14.860 | +1.3% | 13,124,164 | 11,990,374 | -8.6% |

The native rows represent the end-to-end production layout and improve materially because 3.3 emits fewer source
centroids and the accumulator merges them without a redundant public recompression. The fixed rows are an attribution
control. They show a query-kernel improvement without group-by and a near-parity group-by latency result with lower
allocation.

### Earlier segment-construction characterization

| Workload | 3.2 (ms/op) | 3.3 (ms/op) | Latency delta | 3.2 allocation | 3.3 allocation | Allocation delta |
|---|---:|---:|---:|---:|---:|---:|
| Aggregate and serialize raw values | 5,999.450 ± 539.794 | 5,528.188 ± 346.021 | -7.9% | 1,569,923,324 | 1,581,851,411 | +0.8% |
| Merge and serialize pre-aggregated leaves | 1,256.659 ± 380.261 | 466.930 ± 58.190 | -62.8% | 1,926,851,492 | 1,795,782,790 | -6.8% |

Both workloads use 100 million source rows and 1,000 rows per leaf group. The pre-aggregated case merges ten serialized
leaves into each parent and validates total size and a finite median. Accuracy is measured separately below.

### Earlier server-local and distributed-reduction characterization

This table compares the 3.2 production-style pairwise merge with the 3.3 serialized accumulator used by the updated
reducer. All inputs use the same fixed verbose centroid layout.

| Workload | 3.2 pairwise (ms/op) | 3.3 accumulator (ms/op) | Latency delta | 3.2 allocation | 3.3 allocation | Allocation delta |
|---|---:|---:|---:|---:|---:|---:|
| Merge kernel | 0.2444 ± 0.0039 | 0.0385 ± 0.0013 | -84.3% | 77,930 | 36,552 | -53.1% |
| `IndexedTable` combine | 0.2452 ± 0.0036 | 0.0394 ± 0.0010 | -83.9% | 81,626 | 40,248 | -50.7% |
| Combine plus final percentile extraction | 0.2602 ± 0.0045 | 0.0412 ± 0.0016 | -84.2% | 81,722 | 40,345 | -50.6% |

The accumulator decodes sorted serialized centroids and linearly merges them rather than repeatedly materializing and
sorting a `MergingDigest`. The benchmark also retains pairwise and accumulator controls under both dependency versions
to distinguish library changes from the production implementation change.

### Reducer accuracy

The accuracy counters report mean absolute quantile-value error in parts per billion against fully sorted raw-value
oracles. The randomized-order sweep now uses 32 independent deterministic source orders instead of repeating one
order, which exposes the merge-order sensitivity hidden by the earlier table. JMH event counters are normalized over
measurement iterations and forks. Duplicate-heavy input is exact or within floating-point noise. The largest
candidate P99 error observed across the 32 source orders was 1,870,494 ppb (`0.001870494` absolute value error for
values in `[0, 1]`).

| Distribution | Implementation | P75 error (ppb) | P95 error (ppb) | P99 error (ppb) | Mean centroids |
|---|---|---:|---:|---:|---:|
| Uniform | 3.2 pairwise | 84,669 | 190,823 | 152,434 | 125 |
| Uniform | 3.3 accumulator | 192,884 | 255,414 | 210,463 | 65 |
| Skewed | 3.2 pairwise | 459,832 | 591,766 | 979,104 | 127 |
| Skewed | 3.3 accumulator | 1,095,175 | 277,086 | 1,262,052 | 64 |
| Bimodal | 3.2 pairwise | 23,796 | 18,924 | 61,153 | 124 |
| Bimodal | 3.3 accumulator | 27,069 | 84,881 | 170,251 | 66 |
| Duplicate-heavy | 3.2 pairwise | 0 | 0 | 0 | 141 |
| Duplicate-heavy | 3.3 accumulator | 0 | 117 | 0 | 71 |

Every measured result retained exact total weight, positive centroid weights, sorted finite centroid means, and
monotonic P0/P50/P75/P95/P99/P100. Unit tests separately cover standard wire round-trips, compression 20, 100, and
1,000, compact and verbose encodings, fractional compression, large double-precision weights, weighted legacy
boundaries, and repeated infinities.

## Reproduce

Build the benchmark package from the repository root:

```bash
./mvnw -pl pinot-perf -am clean package -DskipTests
```

Run raw aggregation:

```bash
java -cp 'pinot-perf/target/pinot-perf-pkg/lib/*' \
  org.openjdk.jmh.Main \
  'org.apache.pinot.perf.aggregation.BenchmarkPercentileTDigestAggregation.aggregatePercentileTDigest75' \
  -wi 2 -i 5 -f 2 -w 1s -r 1s -t 1 -to 30m -gc true -foe true -prof gc
```

Run the stored StarTree query paths:

```bash
java -cp 'pinot-perf/target/pinot-perf-pkg/lib/*' \
  org.openjdk.jmh.Main \
  'org.apache.pinot.perf.aggregation.BenchmarkPercentileTDigestStarTreeAggregation.*' \
  -p _numGroups=1000 -p _sourceLayout=NATIVE,FIXED_VERBOSE \
  -wi 2 -i 5 -f 2 -w 1s -r 1s -t 1 -to 30m -gc true -foe true -prof gc
```

Run raw and pre-aggregated construction:

```bash
java -cp 'pinot-perf/target/pinot-perf-pkg/lib/*' \
  org.openjdk.jmh.Main \
  'org.apache.pinot.perf.aggregation.BenchmarkPercentileTDigestValueAggregator.*' \
  -p _numRows=100000000 -p _rowsPerGroup=1000 \
  -wi 2 -i 5 -f 2 -w 1s -r 1s -t 1 -to 30m -gc true -foe true -prof gc
```

Run the controlled reducer comparison:

```bash
java -cp 'pinot-perf/target/pinot-perf-pkg/lib/*' \
  org.openjdk.jmh.Main \
  'org.apache.pinot.perf.aggregation.BenchmarkPercentileTDigestCombine.(mergeKernel|combineIndexedTable|combineIndexedTableAndExtract)' \
  -p _numGroups=1 -p _numMetrics=1 -p _fanIn=32 -p _compression=100 \
  -p _distribution=UNIFORM -p _mergeOrder=ORIGINAL \
  -p _sourceLayout=FIXED_VERBOSE -p _sourceReuse=SHARED \
  -p _implementation=PAIRWISE,ACCUMULATOR_WIRE \
  -wi 2 -i 5 -f 2 -w 1s -r 1s -t 1 -to 30m -gc true -foe true -prof gc
```

Run reducer accuracy over skewed and duplicate-heavy inputs:

```bash
java -cp 'pinot-perf/target/pinot-perf-pkg/lib/*' \
  org.openjdk.jmh.Main \
  'org.apache.pinot.perf.aggregation.BenchmarkPercentileTDigestCombine.mergeKernel' \
  -p _numGroups=1 -p _numMetrics=1 -p _fanIn=128 -p _compression=100 \
  -p _distribution=SKEWED,DUPLICATE_HEAVY -p _mergeOrder=RANDOMIZED \
  -p _sourceLayout=NATIVE -p _sourceReuse=SHARED \
  -p _implementation=PAIRWISE,ACCUMULATOR_WIRE \
  -wi 2 -i 5 -f 2 -w 1s -r 1s -t 1 -to 30m -gc true -foe true -prof gc
```

For a strict dependency comparison, build the baseline and candidate into separate package directories and verify that
each `lib` directory contains only its intended `t-digest-3.2.jar` or `t-digest-3.3.jar`. Reusing one package directory
can leave both versions on the classpath and invalidate the result.
