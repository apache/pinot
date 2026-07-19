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

# TDigest Percentile Aggregation: 100-Million-Row Benchmark

- Date: 2026-07-15
- Baseline: `068b458883d6`
- Reducer baseline: `633aab66c6`
- Host: Apple M4 Pro (14 cores, 24 GiB), macOS 26.5.2 aarch64, OpenJDK 21.0.10

These benchmarks measure single-threaded TDigest aggregation-function performance over deterministic pseudo-random
`DOUBLE` values. The raw-value workload exercises the production block aggregation API over 100 million rows. The
star-tree query workload merges 10,000 stored TDigest entries that represent 100 million source rows, and the construction
workload isolates the raw-value aggregator used to build those entries. These numbers describe aggregation kernels on
one machine, not distributed end-to-end query latency or complete segment-generation time.

## User guide

The optimizations are automatic and require no query changes. `PERCENTILETDIGEST` maintains bounded, mergeable
aggregation state, and a star-tree can pre-aggregate it.

| Function | Result | Aggregation state |
|---|---|---|
| `PERCENTILETDIGEST(metric, 75)` | Approximate P75, default compression `100` | Bounded TDigest |
| `PERCENTILETDIGEST(metric, 75, 200)` | Approximate P75 with higher compression | Larger TDigest with potentially better accuracy |

### Sample queries

```sql
-- Approximate P75 with the default TDigest compression of 100.
SELECT PERCENTILETDIGEST(latencyMs, 75) AS p75Approx
FROM myTable;

-- Approximate P75 with an explicit compression factor.
SELECT PERCENTILETDIGEST(latencyMs, 75, 200) AS p75Approx
FROM myTable;

-- Approximate P75 per region; a matching star-tree can serve the stored TDigest metric.
SELECT region, PERCENTILETDIGEST(latencyMs, 75) AS p75Approx
FROM myTable
GROUP BY region;
```

### Sample star-tree table config

Add the TDigest function-column pair to a star-tree index when common queries group or filter on a stable set of
dimensions. This sample uses the default compression `100` and matches the two-argument `PERCENTILETDIGEST` query
shown above; a query with an explicit compression must match the compression configured for the index.

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
the stored-metric aggregation path after star-tree traversal; it does not measure planner, traversal, or forward-index
I/O. The optimized serialized merge applies to both non-grouped and group-by aggregation. Each serialized group keeps
its first digest pending and allocates primitive merge buffers only if another stored digest reaches that group; its
raw-value buffer remains lazy unless a raw value is added.

## Results

JMH 1.37 ran two forks, five one-second measurement iterations, one thread, an 8 GiB maximum heap, and the GC profiler
for each matched comparison. The raw-value and construction comparisons used two one-second warmup iterations; the
serialized star-tree query comparison used three. Each reported comparison contains ten measured iterations.

### TDigest percentile

| Benchmark | Baseline (ms/op) | Optimized (ms/op) | Latency reduction | Speedup |
|---|---:|---:|---:|---:|
| Aggregation plus TDigest P75 | 5,677.240 ± 149.058 | 2,555.127 ± 49.673 | 54.994% | 2.222x |

TDigest throughput improved from 17.614 million to 39.137 million rows per second. The optimized result was
`0.7499280847`, compared with the exact result `0.7499315464`, for an absolute error of `0.0000034617`. Neither TDigest
run triggered garbage collection; normalized allocation changed from 20,520 to 31,872 bytes per operation.

### Star-tree TDigest query path

| Benchmark | Baseline (ms/op) | Optimized (ms/op) | Latency reduction | Speedup |
|---|---:|---:|---:|---:|
| Merge 10,000 stored digests representing 100 million rows, then P75 | 85.658 ± 1.211 | 7.100 ± 0.186 | 91.711% | 12.065x |

Normalized allocation fell from 198,223,964 bytes to 26,361 bytes per operation, and measured GC events fell from ten
to zero. The optimized digest retained an exact size of 100 million and produced P75 `0.7498860479`, versus the fully
sorted oracle `0.7499315464` (absolute error `0.0000454985`). Profiling showed the baseline repeatedly materializing,
shuffling, and sorting centroids as each stored digest was added. The optimized path decodes already-sorted serialized
centroids and linearly merges them into the accumulator.

### Star-tree TDigest group-by query path

This incremental comparison uses commit `4a950bde9455`, the preceding version of this change with the optimized
non-grouped path but the original group-by implementation, as its baseline. Every workload aggregates 10,000 stored
digests representing 100 million source rows. Entries are distributed round-robin, and the timed operation extracts
and validates every group result.

| Groups | Stored digests per group | Baseline (ms/op) | Optimized (ms/op) | Latency reduction | Speedup | Baseline allocation (B/op) | Optimized allocation (B/op) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | 100 | 119.244 ± 66.230 | 9.275 ± 0.161 | 92.222% | 12.856x | 197,636,620 | 2,894,952 |
| 1,000 | 10 | 96.039 ± 11.979 | 11.371 ± 0.257 | 88.159% | 8.446x | 192,278,646 | 28,821,662 |
| 10,000 | 1 | 12.107 ± 6.227 | 9.078 ± 0.412 | Not distinguishable | Parity | 136,520,147 | 137,480,126 |

The 100- and 1,000-group workloads exercise centroid merging and reduce normalized allocation by 98.535% and 85.010%,
respectively. With 10,000 groups, each group receives exactly one stored digest, so there is no source digest merge to
optimize; extraction dominates and allocation is effectively unchanged (+0.703%). Its baseline and optimized latency
confidence intervals overlap, so the nominal difference between their means is not claimed as a speedup. That
high-cardinality case keeps the first
serialized digest pending, constructs only the final result digest, and does not allocate the accumulator's raw-value
buffer. The baseline had isolated latency outliers, reflected in its wider confidence intervals.

### Server-local and distributed TDigest reduction

The reducer benchmark runs on the same host with OpenJDK 25. It prebuilds segment digests and exact raw-value oracles
outside the timed region, creates fresh targets per invocation, and measures both the merge kernel and the complete
`ConcurrentIndexedTable` update path. `PROMOTED_LOCAL` models raw group-by segment results that arrive as ordinary
`MergingDigest` objects. `ACCUMULATOR_LOCAL` models materialized accumulator results from non-grouped and StarTree
segment execution. `ACCUMULATOR_WIRE` models lazy broker-side deserialization of standard TDigest bytes.

The representative comparison uses 32 segment results, compression 100, uniform input, two forks, and five measured
iterations per fork.

| Benchmark | Pairwise 3.2 (ms/op) | Promoted local | Accumulator local | Serialized/wire |
|---|---:|---:|---:|---:|
| Merge kernel | 0.2573 | 0.0325 (7.92x) | 0.0186 (13.82x) | 0.0220 (11.68x) |
| Full `IndexedTable` combine | 0.2683 | 0.0340 (7.90x) | 0.0201 (13.33x) | 0.0233 (11.52x) |
| Full combine plus final percentile extraction | 0.2983 | 0.0353 (8.46x) | 0.0201 (14.83x) | 0.0239 (12.47x) |

Normalized kernel allocation fell from 205,162 B/op to 24,000 B/op for promoted local state (-88.3%), 11,936 B/op
for accumulator-local state (-94.2%), and 15,328 B/op for wire state (-92.5%). Direct percentile extraction reads the
primitive centroid buffers and does not construct a temporary source digest.

The production-shaped workload uses 1,440 groups, seven TDigest metrics, fan-in 32, and distinct prebuilt source
objects for every group and metric. This removes the cache-locality advantage of the smaller shared-corpus kernel.

| Path | Full combine (ms/op) | Speedup | Allocation (B/op) | Allocation reduction |
|---|---:|---:|---:|---:|
| TDigest 3.2 pairwise | 4,574.358 ± 309.333 | 1.00x | 2,074,920,845 | - |
| Promoted raw group-by state | 856.308 ± 176.240 | 5.34x | 248,823,888 | 88.01% |
| Materialized accumulator / StarTree-local | 222.986 ± 2.308 | 20.51x | 127,215,599 | 93.87% |
| Lazy distributed wire state | 283.015 ± 6.168 | 16.16x | 161,407,415 | 92.22% |

A separate non-forked `/usr/bin/time -l` diagnostic over the same unique-source shape measured maximum RSS of
5,701 MB for pairwise, 5,773 MB for promoted local (+1.26%), 3,042 MB for accumulator-local (-46.65%), and 3,017 MB
for wire state (-47.09%). It is a peak-memory check only; the two-fork JMH results above are the latency measurements.

The improvement holds across configured compression factors:

| Compression | Promoted local | Accumulator local | Serialized/wire |
|---:|---:|---:|---:|
| 50 | 7.17x / -84.8% allocation | 12.14x / -90.5% | 10.98x / -88.8% |
| 100 | 7.67x / -86.7% allocation | 13.63x / -92.5% | 11.80x / -90.9% |
| 200 | 8.62x / -87.9% allocation | 15.27x / -93.6% | 12.75x / -92.1% |

A 36-shape fan-in/distribution/order sweep covered fan-in 8/32/128; uniform, skewed, bimodal, and duplicate-heavy
inputs; and original, reversed, and seeded-random order across all four implementations (144 implementation/shape
cases). Every result retained exact total weight, finite positive centroids, and monotonic p0/p50/p75/p95/p99/p100.
Mean P75 error improved from 0.000175 for pairwise 3.2 to 0.000144 for every primitive-accumulator mode; the maximum
sample error was 0.001353 for pairwise and 0.000709 for the candidates. The focused correctness suite additionally
covers compression 20, 100, and 1,000 for the full parameter cross-product and round-trips every reduced result
through standard TDigest 3.2 bytes.

#### TDigest 3.3 dependency-only experiment

A separate build changed only the TDigest dependency from 3.2 to 3.3. The control harness supplies the same 128
verbose centroids to both versions. Across 36 workload shapes, the geometric mean speedup was 4.47x in the kernel and
4.03x through `IndexedTable`, with 70.7% and 65.6% lower allocation. It is not used by this change: mean P75 error
increased 18.9x, the maximum sampled P75 error reached 0.01138, and mean resulting centroids changed from 123.1 to
42.2. A high-compression small-encoding mixed-version boundary also failed. In a separate 3.3 representative run,
`add(List.of(source))` was 2.27x slower than pairwise and bounded batches were 1.31-1.52x slower while allocating more.
The accepted reducer therefore stays on TDigest 3.2 and preserves standard TDigest wire bytes.

### Star-tree TDigest construction kernel

| Workload | Baseline (ms/op) | Optimized (ms/op) | Latency reduction | Speedup |
|---|---:|---:|---:|---:|
| 1 million rows, 1,000 rows per dimension group | 1,840.391 ± 47.275 | 53.770 ± 1.470 | 97.078% | 34.227x |

The previous size-accounting call compressed the TDigest after every input value, defeating the digest's batching.
The optimized path maintains a safe serialized-size bound without compressing and defers compression until
serialization. A separate optimized 100-million-row run with 1,000 rows per group completed in 5,455.170 ms, or
18.331 million rows per second. The 100-million-row number is an actual optimized measurement; no unmeasured baseline
is extrapolated for it.
This construction benchmark includes one serialization per group but excludes dimension sorting and forward-index
I/O.

## Reproduce

Build the benchmark package from the repository root:

```bash
./mvnw -pl pinot-perf -am clean package -DskipTests
```

Run the TDigest benchmark:

```bash
java -Xms4g -Xmx8g \
  -cp 'pinot-perf/target/pinot-perf-pkg/lib/*' \
  org.openjdk.jmh.Main \
  'org.apache.pinot.perf.aggregation.BenchmarkPercentileTDigestAggregation.*' \
  -wi 2 -i 5 -f 2 -w 1s -r 1s -t 1 -to 30m -gc true -prof gc
```

Run the stored star-tree TDigest query benchmark. Its 10,000 input entries represent 100 million source rows:

```bash
java -Xms4g -Xmx8g \
  -cp 'pinot-perf/target/pinot-perf-pkg/lib/*' \
  org.openjdk.jmh.Main \
  'org.apache.pinot.perf.aggregation.BenchmarkPercentileTDigestStarTreeAggregation.*' \
  -wi 3 -i 5 -f 2 -w 1s -r 1s -t 1 -to 30m -gc true -prof gc
```

Run the star-tree construction kernel over 100 million raw rows with 1,000 rows per dimension group:

```bash
java -Xms4g -Xmx8g \
  -cp 'pinot-perf/target/pinot-perf-pkg/lib/*' \
  org.openjdk.jmh.Main \
  'org.apache.pinot.perf.aggregation.BenchmarkPercentileTDigestValueAggregator.*' \
  -p _numRows=100000000 -p _rowsPerGroup=1000 \
  -wi 1 -i 3 -f 1 -w 1s -r 1s -t 1 -to 30m -gc true -prof gc
```

Run the representative reducer comparison:

```bash
java -Xms4g -Xmx8g \
  -cp 'pinot-perf/target/pinot-perf-pkg/lib/*' \
  org.openjdk.jmh.Main \
  'org.apache.pinot.perf.aggregation.BenchmarkPercentileTDigestCombine.*' \
  -p _numGroups=1 -p _numMetrics=1 -p _fanIn=32 -p _compression=100 \
  -p _distribution=UNIFORM -p _mergeOrder=ORIGINAL \
  -p _implementation=PAIRWISE,PROMOTED_LOCAL,ACCUMULATOR_LOCAL,ACCUMULATOR_WIRE \
  -p _sourceLayout=NATIVE -p _sourceReuse=SHARED \
  -wi 2 -i 5 -f 2 -w 1s -r 1s -t 1 -prof gc
```

Run the production-shaped combine with unique prebuilt source state:

```bash
java -Xms4g -Xmx8g \
  -cp 'pinot-perf/target/pinot-perf-pkg/lib/*' \
  org.openjdk.jmh.Main \
  'org.apache.pinot.perf.aggregation.BenchmarkPercentileTDigestCombine.combineIndexedTable$' \
  -p _numGroups=1440 -p _numMetrics=7 -p _fanIn=32 -p _compression=100 \
  -p _distribution=UNIFORM -p _mergeOrder=ORIGINAL \
  -p _implementation=PAIRWISE,PROMOTED_LOCAL,ACCUMULATOR_LOCAL,ACCUMULATOR_WIRE \
  -p _sourceLayout=NATIVE -p _sourceReuse=UNIQUE \
  -wi 2 -i 5 -f 2 -w 1s -r 1s -t 1 -prof gc
```

For the dependency-only experiment, change only `<t-digest.version>` in the root `pom.xml` from `3.2` to `3.3`,
rebuild `pinot-core` and `pinot-perf`, and run the same command for each build with `_implementation=PAIRWISE` and
`_sourceLayout=FIXED_VERBOSE`. Write each result with `-rf json -rff <result-file>` and construct the runtime
classpath with exactly one `t-digest` JAR; a package directory reused across builds can otherwise retain both JARs.

The raw-value benchmark generates the same 100 million values from `SplittableRandom(42)` during trial setup. The
timed methods include result extraction and validation. Dataset generation and exact-oracle construction are outside
the measured region. The star-tree query benchmark similarly generates its stored digests during trial setup. The
construction benchmark generates a reusable deterministic value block before measurement.
