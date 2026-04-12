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
```
$ cd <root_source_code>
$ ./mvnw -f pinot-perf/pom.xml -am package -DskipTests
```
2. The above cmd will generate `pinot-perf/target/pinot-perf-pkg`

3. Run benchmark using generated scripts
```
$ cd target/pinot-perf-pkg/bin
$ ./pinot-BenchmarkDictionary.sh
```

# Vector benchmark suite

`org.apache.pinot.perf.BenchmarkVectorIndex` is the canonical entry point for Pinot's vector
benchmark suite. Run it from the repository root with
`./mvnw -f pinot-perf/pom.xml -am ...`, then use `-Dpinot.perf.vector.mode=...` to select the
scenario:

- `matrix` (default): broad backend matrix comparing Exact Scan, HNSW, `IVF_FLAT`, and `IVF_PQ`
- `quick`: small validation run to sanity-check recall and latency before a larger benchmark
- `phase3`: filtered exact scan and threshold-search semantics benchmark
- `closeout`: final vector close-out benchmark for quantizers, HNSW runtime controls,
  `IVF_ON_DISK` filter-aware ANN, approximate radius, and mixed mutable/immutable HNSW
- `all`: run every mode in sequence

The older entry-point classes remain for compatibility, but new usage should go through
`BenchmarkVectorIndex`.

## Matrix mode

This is the default mode and compares Exact Scan, HNSW, `IVF_FLAT`, and `IVF_PQ` on synthetic
datasets.

What it reports:
- Build time
- Build throughput in docs/sec
- Peak heap growth during build
- On-disk index size
- Recall@10 and Recall@100
- Query latency p50/p95/p99

Datasets:
- Euclidean Gaussian
- Cosine normalized
- Inner product with magnitude skew

Run it with Maven:

```bash
./mvnw -f pinot-perf/pom.xml -am exec:java \
  -Dexec.mainClass=org.apache.pinot.perf.BenchmarkVectorIndex
```

Useful tuning properties:

```bash
-Dpinot.perf.vector.dimension=128
-Dpinot.perf.vector.datasetSizes=1000,10000,100000
-Dpinot.perf.vector.queries=200
-Dpinot.perf.vector.warmupQueries=50
-Dpinot.perf.vector.nlist=16,32,64,128,256
-Dpinot.perf.vector.nprobe=1,2,4,8,16,32
-Dpinot.perf.vector.pqM=8,16
-Dpinot.perf.vector.pqNbits=8
-Dpinot.perf.vector.memoryPollMs=10
-Dpinot.perf.vector.sweepSize=10000
-Dpinot.perf.vector.sweepDimension=128
-Dpinot.perf.vector.skipSweep=true
```

Small smoke run:

```bash
./mvnw -f pinot-perf/pom.xml -am exec:java \
  -Dexec.mainClass=org.apache.pinot.perf.BenchmarkVectorIndex \
  -Dpinot.perf.vector.dimension=16 \
  -Dpinot.perf.vector.datasetSizes=100 \
  -Dpinot.perf.vector.queries=10 \
  -Dpinot.perf.vector.warmupQueries=3 \
  -Dpinot.perf.vector.nlist=4,8 \
  -Dpinot.perf.vector.nprobe=1,2 \
  -Dpinot.perf.vector.pqM=4,8 \
  -Dpinot.perf.vector.pqNbits=4 \
  -Dpinot.perf.vector.skipSweep=true
```

## Closeout mode

This mode targets the final vector-search feature gaps that were closed after the broad backend
matrix benchmark was added.

What it reports:
- IVF_FLAT real-path quantizer comparisons for `FLAT`, `SQ8`, `SQ4`, plus `IVF_PQ`
- HNSW runtime-control sweeps for `vectorEfSearch`, relative-distance pruning, and bounded-queue mode
- `IVF_ON_DISK` `FILTER_THEN_ANN` versus post-filter behavior at different filter selectivities
- Backend-native approximate radius behavior for Euclidean and cosine workloads
- Mixed immutable plus mutable HNSW candidate recall and latency

Run it from the repository root with the module-scoped Maven invocation below. On JDK 21+, use
`MAVEN_OPTS` to open the Pinot plugin-loader access that the HNSW reader path needs:

```bash
MAVEN_OPTS='--add-opens=java.base/java.net=ALL-UNNAMED --enable-native-access=ALL-UNNAMED --add-modules=jdk.incubator.vector' \
./mvnw -f pinot-perf/pom.xml -am -Ppinot-fastdev exec:java \
  -Dexec.mainClass=org.apache.pinot.perf.BenchmarkVectorIndex \
  -Dpinot.perf.vector.mode=closeout
```

Useful tuning properties:

```bash
-Dpinot.perf.vector.closeout.size=10000
-Dpinot.perf.vector.closeout.dimension=128
-Dpinot.perf.vector.closeout.queries=80
-Dpinot.perf.vector.closeout.warmupQueries=20
-Dpinot.perf.vector.closeout.nlist=128
-Dpinot.perf.vector.closeout.nprobe=8
-Dpinot.perf.vector.closeout.topK=10
-Dpinot.perf.vector.closeout.radiusTargetMatches=20
-Dpinot.perf.vector.closeout.radiusMaxCandidates=256
-Dpinot.perf.vector.closeout.mutablePercent=10
-Dpinot.perf.vector.closeout.hnswEfSearch=64
```

Small smoke run:

```bash
MAVEN_OPTS='--add-opens=java.base/java.net=ALL-UNNAMED --enable-native-access=ALL-UNNAMED --add-modules=jdk.incubator.vector' \
./mvnw -f pinot-perf/pom.xml -am -Ppinot-fastdev exec:java \
  -Dexec.mainClass=org.apache.pinot.perf.BenchmarkVectorIndex \
  -Dpinot.perf.vector.mode=closeout \
  -Dpinot.perf.vector.closeout.size=2000 \
  -Dpinot.perf.vector.closeout.dimension=32 \
  -Dpinot.perf.vector.closeout.queries=20 \
  -Dpinot.perf.vector.closeout.warmupQueries=5 \
  -Dpinot.perf.vector.closeout.nlist=32 \
  -Dpinot.perf.vector.closeout.nprobe=4 \
  -Dpinot.perf.vector.closeout.topK=5 \
  -Dpinot.perf.vector.closeout.radiusTargetMatches=10 \
  -Dpinot.perf.vector.closeout.radiusMaxCandidates=64
```

## Quick and phase3 compatibility modes

These older specialized benchmarks are also reachable through the canonical entry point:

```bash
./mvnw -f pinot-perf/pom.xml -am -Ppinot-fastdev exec:java \
  -Dexec.mainClass=org.apache.pinot.perf.BenchmarkVectorIndex \
  -Dpinot.perf.vector.mode=quick

./mvnw -f pinot-perf/pom.xml -am -Ppinot-fastdev exec:java \
  -Dexec.mainClass=org.apache.pinot.perf.BenchmarkVectorIndex \
  -Dpinot.perf.vector.mode=phase3
```
