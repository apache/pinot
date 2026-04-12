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

Pinot Perf Package
===
Pinot perf package contains a set of performance benchmark for Pinot components.

Note: this package will pull `org.openjdk.jmh:jmh-core`, which is based on `GPL 2 license`.

# Steps for running benchmark

1. Build the source
```bash
$ cd <root_source_code>
$ ./mvnw -f pinot-perf/pom.xml -am package -DskipTests
```
2. The above command will generate `pinot-perf/target/pinot-perf-pkg`

3. Run benchmark using generated scripts
```bash
$ cd target/pinot-perf-pkg/bin
$ ./pinot-BenchmarkDictionary.sh
```

# Vector benchmark suite

`org.apache.pinot.perf.BenchmarkVectorIndex` is the canonical entry point for Pinot's vector
benchmark suite. The suite is workload-based instead of phase-based. Each mode maps to a concrete
question Pinot needs to answer about its vector stack:

- `frontier`: ANN quality, latency, build cost, and index-size frontiers for `HNSW`, `IVF_FLAT`,
  and `IVF_PQ` against exact scan baselines.
- `sanity`: small smoke run for fast validation before running the heavier suites.
- `filters`: filtered ANN and approximate-radius workloads, including `IVF_ON_DISK`.
- `features`: quantized IVF comparisons, HNSW runtime controls, mutable HNSW ingestion, and
  mixed immutable/mutable search.
- `suite`: runs `frontier`, `sanity`, `filters`, and `features` in sequence.

Run from the repository root:

```bash
./mvnw -f pinot-perf/pom.xml -am exec:java \
  -Dexec.mainClass=org.apache.pinot.perf.BenchmarkVectorIndex \
  -Dpinot.perf.vector.mode=frontier
```

On JDK 21+, use the same `MAVEN_OPTS` required by Pinot's vector readers:

```bash
MAVEN_OPTS='--add-opens=java.base/java.net=ALL-UNNAMED --enable-native-access=ALL-UNNAMED --add-modules=jdk.incubator.vector'
```

## Benchmark design

The rewritten suite follows the same high-level structure used by mature vector database
benchmarks, but tuned for Pinot-specific execution surfaces:

- Frontier workloads measure build time, build throughput, peak heap growth, on-disk size,
  recall, latency, and QPS across representative distance functions and dimensions.
- Filter workloads separate exact filtered top-k, filter-aware ANN, and approximate radius so the
  trade-offs are explicit instead of hidden in a single score.
- Feature workloads isolate Pinot-specific runtime knobs such as quantization choices, HNSW query
  controls, and mutable versus immutable segment behavior.

## Frontier

`frontier` is the default mode. It benchmarks:

- Euclidean Gaussian vectors
- 768-d cosine-normalized vectors
- 1536-d cosine-normalized vectors
- 768-d inner-product vectors with magnitude skew

What it reports:

- Build time
- Build throughput in docs/sec
- Peak heap growth during build
- On-disk index size
- Recall@10 and Recall@100
- Query latency p50/p95/p99
- QPS

Useful properties:

```bash
-Dpinot.perf.vector.queries=200
-Dpinot.perf.vector.warmupQueries=50
-Dpinot.perf.vector.frontier.l2.dimension=128
-Dpinot.perf.vector.frontier.l2.size=10000
-Dpinot.perf.vector.frontier.cosine768.size=10000
-Dpinot.perf.vector.frontier.cosine1536.size=5000
-Dpinot.perf.vector.frontier.dot768.size=10000
-Dpinot.perf.vector.frontier.nlist=32,64,128
-Dpinot.perf.vector.frontier.nprobe=1,2,4,8,16
-Dpinot.perf.vector.frontier.pqM=16,32
-Dpinot.perf.vector.frontier.pqNbits=8
-Dpinot.perf.vector.memoryPollMs=10
```

Small smoke run:

```bash
./mvnw -f pinot-perf/pom.xml -am exec:java \
  -Dexec.mainClass=org.apache.pinot.perf.BenchmarkVectorIndex \
  -Dpinot.perf.vector.mode=frontier \
  -Dpinot.perf.vector.queries=10 \
  -Dpinot.perf.vector.warmupQueries=3 \
  -Dpinot.perf.vector.frontier.l2.dimension=32 \
  -Dpinot.perf.vector.frontier.l2.size=1000 \
  -Dpinot.perf.vector.frontier.cosine768.size=1000 \
  -Dpinot.perf.vector.frontier.cosine1536.size=500 \
  -Dpinot.perf.vector.frontier.dot768.size=1000 \
  -Dpinot.perf.vector.frontier.nlist=8,16 \
  -Dpinot.perf.vector.frontier.nprobe=1,2 \
  -Dpinot.perf.vector.frontier.pqM=8 \
  -Dpinot.perf.vector.frontier.pqNbits=4
```

## Sanity

`sanity` is the fast validation mode. It checks exact scan, `IVF_FLAT`, and `IVF_PQ` when
available.

Useful properties:

```bash
-Dpinot.perf.vector.sanity.size=5000
-Dpinot.perf.vector.sanity.dimension=128
-Dpinot.perf.vector.sanity.queries=100
-Dpinot.perf.vector.sanity.topK=10
-Dpinot.perf.vector.sanity.nlist=64
```

Run it:

```bash
./mvnw -f pinot-perf/pom.xml -am exec:java \
  -Dexec.mainClass=org.apache.pinot.perf.BenchmarkVectorIndex \
  -Dpinot.perf.vector.mode=sanity
```

## Filters

`filters` benchmarks exact filtered baselines, filter-aware ANN, and approximate-radius behavior.

What it reports:

- Exact top-k and filtered top-k baselines
- `IVF_FLAT` and `IVF_ON_DISK` filter-aware recall/latency across selectivities
- Approximate-radius recall/latency for Euclidean and cosine workloads

Useful properties:

```bash
-Dpinot.perf.vector.filters.size=12000
-Dpinot.perf.vector.filters.dimension=768
-Dpinot.perf.vector.filters.queries=80
-Dpinot.perf.vector.filters.warmupQueries=20
-Dpinot.perf.vector.filters.topK=10
-Dpinot.perf.vector.filters.nlist=96
-Dpinot.perf.vector.filters.nprobe=8
-Dpinot.perf.vector.filters.quantizer=SQ8
-Dpinot.perf.vector.filters.radiusTargetMatches=24
-Dpinot.perf.vector.filters.radiusMaxCandidates=512
```

Smoke run:

```bash
./mvnw -f pinot-perf/pom.xml -am exec:java \
  -Dexec.mainClass=org.apache.pinot.perf.BenchmarkVectorIndex \
  -Dpinot.perf.vector.mode=filters \
  -Dpinot.perf.vector.filters.size=2000 \
  -Dpinot.perf.vector.filters.dimension=32 \
  -Dpinot.perf.vector.filters.queries=20 \
  -Dpinot.perf.vector.filters.warmupQueries=5 \
  -Dpinot.perf.vector.filters.nlist=16 \
  -Dpinot.perf.vector.filters.nprobe=4 \
  -Dpinot.perf.vector.filters.topK=5 \
  -Dpinot.perf.vector.filters.radiusTargetMatches=10 \
  -Dpinot.perf.vector.filters.radiusMaxCandidates=64
```

## Features

`features` benchmarks Pinot-specific vector capabilities that are not captured by a broad ANN
frontier.

What it reports:

- `IVF_FLAT` quantizer variants `FLAT`, `SQ8`, `SQ4`
- `IVF_PQ` build/search trade-offs
- HNSW runtime controls on immutable and mutable readers
- Mutable HNSW ingestion cost
- Mixed immutable plus mutable HNSW search behavior

Useful properties:

```bash
-Dpinot.perf.vector.features.size=12000
-Dpinot.perf.vector.features.dimension=768
-Dpinot.perf.vector.features.queries=80
-Dpinot.perf.vector.features.warmupQueries=20
-Dpinot.perf.vector.features.topK=10
-Dpinot.perf.vector.features.nlist=96
-Dpinot.perf.vector.features.nprobe=8
-Dpinot.perf.vector.features.mutablePercent=10
-Dpinot.perf.vector.features.pqM=16
-Dpinot.perf.vector.features.pqNbits=8
-Dpinot.perf.vector.features.hnswEfSearch=64
```

Smoke run:

```bash
./mvnw -f pinot-perf/pom.xml -am exec:java \
  -Dexec.mainClass=org.apache.pinot.perf.BenchmarkVectorIndex \
  -Dpinot.perf.vector.mode=features \
  -Dpinot.perf.vector.features.size=2000 \
  -Dpinot.perf.vector.features.dimension=32 \
  -Dpinot.perf.vector.features.queries=20 \
  -Dpinot.perf.vector.features.warmupQueries=5 \
  -Dpinot.perf.vector.features.nlist=16 \
  -Dpinot.perf.vector.features.nprobe=4 \
  -Dpinot.perf.vector.features.topK=5 \
  -Dpinot.perf.vector.features.mutablePercent=20 \
  -Dpinot.perf.vector.features.pqM=8
```

## Full suite

Run everything in one pass:

```bash
./mvnw -f pinot-perf/pom.xml -am exec:java \
  -Dexec.mainClass=org.apache.pinot.perf.BenchmarkVectorIndex \
  -Dpinot.perf.vector.mode=suite
```
