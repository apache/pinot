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
$ cd <root_source_code>/pinot-perf
$ ./mvnw package -DskipTests
```
2. The above cmd will generate `target/pinot-perf-pkg`

3. Run benchmark using generated scripts
```
$ cd target/pinot-perf-pkg/bin
$ ./pinot-BenchmarkDictionary.sh
```

# Vector index benchmark

`org.apache.pinot.perf.BenchmarkVectorIndex` compares Exact Scan, HNSW, IVF_FLAT, and IVF_PQ on synthetic datasets.

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
cd pinot-perf
../mvnw exec:java \
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
cd pinot-perf
../mvnw exec:java \
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
