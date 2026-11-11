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

# Vector Benchmark Rewrite Smoke Results

Date: 2026-04-12  
Host: macOS aarch64, JDK 21.0.8

This file captures the first smoke-scale runs for the rewritten workload-based vector benchmark
suite in `pinot-perf`. These are validation runs for benchmark shape and completion, not
publication-grade performance claims.

## Commands

### Sanity

```bash
MAVEN_OPTS='--add-opens=java.base/java.net=ALL-UNNAMED --enable-native-access=ALL-UNNAMED --add-modules=jdk.incubator.vector' \
./mvnw -pl pinot-perf -q exec:java \
  -Dexec.mainClass=org.apache.pinot.perf.BenchmarkVectorIndex \
  -Dpinot.perf.vector.mode=sanity \
  -Dpinot.perf.vector.sanity.size=1000 \
  -Dpinot.perf.vector.sanity.dimension=32 \
  -Dpinot.perf.vector.sanity.queries=20 \
  -Dpinot.perf.vector.sanity.topK=5 \
  -Dpinot.perf.vector.sanity.nlist=16 \
  -Dpinot.perf.vector.sanity.nprobe=1,2,4,8
```

Key output:

- Exact scan recall@10: `1.0000`
- `IVF_FLAT`, `nprobe=8`: recall@10 `0.9100`, p50 `30.2us`
- `IVF_PQ`, `nprobe=8`: recall@10 `0.8300`, p50 `87.6us`

### Frontier

```bash
MAVEN_OPTS='--add-opens=java.base/java.net=ALL-UNNAMED --enable-native-access=ALL-UNNAMED --add-modules=jdk.incubator.vector' \
./mvnw -pl pinot-perf -q exec:java \
  -Dexec.mainClass=org.apache.pinot.perf.BenchmarkVectorIndex \
  -Dpinot.perf.vector.mode=frontier \
  -Dpinot.perf.vector.queries=10 \
  -Dpinot.perf.vector.warmupQueries=3 \
  -Dpinot.perf.vector.frontier.l2.dimension=32 \
  -Dpinot.perf.vector.frontier.l2.size=500 \
  -Dpinot.perf.vector.frontier.cosine768.size=300 \
  -Dpinot.perf.vector.frontier.cosine1536.size=150 \
  -Dpinot.perf.vector.frontier.dot768.size=300 \
  -Dpinot.perf.vector.frontier.nlist=8 \
  -Dpinot.perf.vector.frontier.nprobe=1,2 \
  -Dpinot.perf.vector.frontier.pqM=8 \
  -Dpinot.perf.vector.frontier.pqNbits=4
```

Representative output:

- `l2-gaussian-32d-500`
  - `Exact Scan`: p50 `234.3us`, qps `3424.3`
  - `HNSW`: recall@10 `0.8900`, p50 `136.6us`, qps `7178.1`
  - Best `IVF_FLAT` config in this run: `SQ4,nprobe=2`, recall@10 `0.5100`, p50 `26.6us`
- `cosine-normalized-768d-300`
  - `HNSW`: recall@10 `0.7000`, p50 `133.6us`
  - Best `IVF_FLAT` config in this run: `SQ8,nprobe=2`, recall@10 `0.4700`, p50 `108.4us`
- `cosine-normalized-1536d-150`
  - `HNSW`: recall@10 `0.8600`, p50 `90.3us`
- `inner-product-skew-768d-300`
  - `HNSW`: recall@10 `0.9000`, p50 `67.5us`

### Filters

```bash
MAVEN_OPTS='--add-opens=java.base/java.net=ALL-UNNAMED --enable-native-access=ALL-UNNAMED --add-modules=jdk.incubator.vector' \
./mvnw -pl pinot-perf -q exec:java \
  -Dexec.mainClass=org.apache.pinot.perf.BenchmarkVectorIndex \
  -Dpinot.perf.vector.mode=filters \
  -Dpinot.perf.vector.filters.size=2000 \
  -Dpinot.perf.vector.filters.dimension=32 \
  -Dpinot.perf.vector.filters.queries=20 \
  -Dpinot.perf.vector.filters.warmupQueries=5 \
  -Dpinot.perf.vector.filters.topK=5 \
  -Dpinot.perf.vector.filters.nlist=16 \
  -Dpinot.perf.vector.filters.nprobe=4 \
  -Dpinot.perf.vector.filters.radiusTargetMatches=10 \
  -Dpinot.perf.vector.filters.radiusMaxCandidates=64
```

Representative output:

- Filter-aware ANN, 100% selectivity:
  - `IVF_FLAT`: recall `0.7000`, p50 `188.2us`
  - `IVF_ON_DISK`: recall `0.7000`, p50 `145.3us`
- Filter-aware ANN, 10% selectivity:
  - `IVF_FLAT`: recall `0.5100`, p50 `64.6us`
  - `IVF_ON_DISK`: recall `0.5100`, p50 `66.8us`
- Approximate radius:
  - `IVF_FLAT`, Euclidean, `approx_radius`: recall `0.6300`, p50 `39.2us`
  - `IVF_ON_DISK`, Cosine, `approx_radius`: recall `0.5750`, p50 `61.7us`

### Features

```bash
MAVEN_OPTS='--add-opens=java.base/java.net=ALL-UNNAMED --enable-native-access=ALL-UNNAMED --add-modules=jdk.incubator.vector' \
./mvnw -pl pinot-perf -q exec:java \
  -Dexec.mainClass=org.apache.pinot.perf.BenchmarkVectorIndex \
  -Dpinot.perf.vector.mode=features \
  -Dpinot.perf.vector.features.size=20 \
  -Dpinot.perf.vector.features.dimension=8 \
  -Dpinot.perf.vector.features.queries=2 \
  -Dpinot.perf.vector.features.warmupQueries=1 \
  -Dpinot.perf.vector.features.topK=2 \
  -Dpinot.perf.vector.features.nlist=4 \
  -Dpinot.perf.vector.features.nprobe=1 \
  -Dpinot.perf.vector.features.mutablePercent=10 \
  -Dpinot.perf.vector.features.pqM=2 \
  -Dpinot.perf.vector.features.pqNbits=4 \
  -Dpinot.perf.vector.features.hnswEfSearch=8
```

Representative output:

- Quantized IVF:
  - `IVF_FLAT FLAT`: recall@10 `0.6667`, p50 `11.6us`
  - `IVF_FLAT SQ8`: recall@10 `0.6667`, p50 `6.7us`
  - `IVF_FLAT SQ4`: recall@10 `0.6000`, p50 `6.8us`
  - `IVF_PQ`: recall@10 `0.2500`, p50 `8.8us`
- Mutable HNSW ingestion:
  - `mutable_hnsw_add`: build `935.3ms`, docs/sec `21.4`
- HNSW runtime controls:
  - Immutable default: recall@10 `1.0000`, p50 `86.0us`
  - Mutable default: recall@10 `1.0000`, p50 `1145.3us`
- Mixed immutable/mutable:
  - Default: recall@10 `1.0000`, p50 `1054.3us`

## Notes

- `features` is materially heavier than `sanity`, `frontier`, and `filters` because it exercises
  mutable HNSW ingestion plus mixed immutable/mutable query execution.
- Larger `features` smoke settings were intentionally not recorded here because they are suitable
  for workstation benchmarking but are too slow for a quick merge-validation pass.
