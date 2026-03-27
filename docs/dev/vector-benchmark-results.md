# Vector Index Benchmark Report

## 1. Benchmark Setup

### Hardware/Environment
- **OS**: Mac OS X aarch64 (Apple Silicon)
- **JDK**: 21.0.8
- **Pinot**: master branch (commit 57589a1faf)
- **Date**: 2026-03-26

### Methodology
- Each query latency measurement uses `System.nanoTime()` with 50 warmup queries excluded
- Recall is computed by comparing ANN results against pre-computed brute-force ground truth
- 200 query vectors per dataset, results aggregated as mean recall and percentile latency
- Random seed: 42 (deterministic, reproducible)
- All IVF_FLAT indexes built and queried via actual `IvfFlatVectorIndexCreator` / `IvfFlatVectorIndexReader`
- HNSW reader requires full Pinot runtime (PinotDataBuffer/PluginManager); not benchmarkable in standalone mode

### How to Reproduce

```bash
# Build the benchmark module
./mvnw -pl pinot-perf -am install -DskipTests -Drat.skip=true

# Quick validation (5K vectors, ~10 seconds)
./mvnw -pl pinot-perf exec:java \
  -Dexec.mainClass=org.apache.pinot.perf.BenchmarkVectorIndexRunner

# Full benchmark suite (~15 minutes)
./mvnw -pl pinot-perf exec:java \
  -Dexec.mainClass=org.apache.pinot.perf.BenchmarkVectorIndex
```

## 2. Dataset Descriptions

### Dataset A: L2 Synthetic (Gaussian)
- **Distribution**: Each dimension drawn independently from N(0, 1)
- **Distance metric**: EUCLIDEAN (L2 squared, no sqrt)
- **Sizes**: 1K, 10K, 100K vectors
- **Dimension**: 128
- **Seed**: 42 for corpus, 1042 for queries

### Dataset B: Cosine Normalized
- **Distribution**: Gaussian vectors unit-normalized to lie on the 128-d unit hypersphere
- **Distance metric**: COSINE (1 - cosine_similarity)
- **Sizes**: 1K, 10K, 100K vectors
- **Dimension**: 128
- **Seed**: 2042 for corpus, 3042 for queries

### Ground Truth
- Brute-force exact scan for each query, computing all N distances and sorting
- Verified: exact scan achieves recall@10 = 1.0000 for all datasets (sanity check passes)

## 3. Metrics Tables

### Dataset A: L2 Synthetic, 1K vectors, dim=128

| Index     | Parameters        | Build(ms) | Size(KB) | Recall@10 | Recall@100 | p50(us) | p95(us) | p99(us) |
|-----------|-------------------|-----------|----------|-----------|------------|---------|---------|---------|
| Exact Scan | -               | 0         | 0        | 1.0000    | 1.0000     | 421     | 481     | 570     |
| IVF_FLAT  | nlist=8,nprobe=1  | 35        | 508      | 0.2525    | 0.2217     | 17      | 29      | 30      |
| IVF_FLAT  | nlist=8,nprobe=4  | 35        | 508      | 0.7725    | 0.7097     | 58      | 74      | 97      |
| **IVF_FLAT** | **nlist=8,nprobe=8** | **35** | **508** | **1.0000** | **1.0000** | **84** | **90** | **105** |
| IVF_FLAT  | nlist=16,nprobe=4 | 32        | 512      | 0.5280    | 0.4682     | 33      | 41      | 54      |
| IVF_FLAT  | nlist=16,nprobe=8 | 32        | 512      | 0.8390    | 0.7872     | 60      | 127     | 164     |
| IVF_FLAT  | nlist=16,nprobe=16 | 32       | 512      | 1.0000    | 1.0000     | 84      | 89      | 109     |
| IVF_FLAT  | nlist=32,nprobe=8 | 31        | 520      | 0.6945    | 0.6082     | 44      | 50      | 52      |
| IVF_FLAT  | nlist=32,nprobe=16 | 31       | 520      | 0.9310    | 0.8980     | 70      | 73      | 95      |

**Key finding at 1K**: IVF_FLAT with nlist=8, nprobe=8 achieves perfect recall at 5x lower latency than exact scan (84us vs 421us). Even small nlist values work well at this scale.

### Dataset A: L2 Synthetic, 10K vectors, dim=128

| Index     | Parameters          | Build(ms) | Size(KB) | Recall@10 | Recall@100 | p50(us) | p95(us) | p99(us) |
|-----------|---------------------|-----------|----------|-----------|------------|---------|---------|---------|
| Exact Scan | -                 | 0         | 0        | 1.0000    | 1.0000     | 2287    | 2587    | 2946    |
| IVF_FLAT  | nlist=16,nprobe=4   | 742       | 5047     | 0.4550    | 0.4003     | 203     | 218     | 229     |
| IVF_FLAT  | nlist=16,nprobe=8   | 742       | 5047     | 0.7125    | 0.6695     | 402     | 425     | 440     |
| **IVF_FLAT** | **nlist=16,nprobe=16** | **742** | **5047** | **1.0000** | **1.0000** | **803** | **1173** | **1701** |
| IVF_FLAT  | nlist=32,nprobe=8   | 1410      | 5056     | 0.4910    | 0.4326     | 206     | 231     | 338     |
| IVF_FLAT  | nlist=32,nprobe=16  | 1410      | 5056     | 0.7515    | 0.7024     | 410     | 508     | 873     |
| IVF_FLAT  | nlist=64,nprobe=8   | 2885      | 5072     | 0.3345    | 0.2938     | 120     | 133     | 139     |
| IVF_FLAT  | nlist=64,nprobe=16  | 2885      | 5072     | 0.5250    | 0.4851     | 226     | 253     | 281     |
| IVF_FLAT  | nlist=128,nprobe=16 | 3684      | 5105     | 0.4565    | 0.3762     | 162     | 181     | 198     |

**Key finding at 10K**: To achieve recall@10 >= 0.90, IVF_FLAT needs nlist=16 with nprobe=16 (full scan of all clusters). At nlist=16, nprobe=8 gives recall=0.71 at 402us (5.7x faster than exact scan at 2287us). Higher nlist values reduce per-query latency but require more probes for equivalent recall.

### Dataset A: L2 Synthetic, 100K vectors, dim=128

| Index     | Parameters           | Build(ms) | Size(KB) | Recall@10 | Recall@100 | p50(us) | p95(us) | p99(us) |
|-----------|----------------------|-----------|----------|-----------|------------|---------|---------|---------|
| Exact Scan | -                  | 0         | 0        | 1.0000    | 1.0000     | 30937   | 45515   | 54710   |
| IVF_FLAT  | nlist=32,nprobe=4    | 2107      | 50407    | 0.3025    | 0.2743     | 1132    | 1216    | 1267    |
| IVF_FLAT  | nlist=32,nprobe=16   | 2107      | 50407    | 0.7600    | 0.7362     | 4491    | 4710    | 4783    |
| IVF_FLAT  | nlist=64,nprobe=8    | 3368      | 50423    | 0.3220    | 0.3160     | 1190    | 1301    | 1328    |
| IVF_FLAT  | nlist=64,nprobe=16   | 3368      | 50423    | 0.5460    | 0.5152     | 2433    | 2632    | 2702    |
| IVF_FLAT  | nlist=128,nprobe=8   | 3943      | 50456    | 0.2820    | 0.2343     | 773     | 896     | 979     |
| IVF_FLAT  | nlist=128,nprobe=16  | 3943      | 50456    | 0.4355    | 0.3810     | 1507    | 1688    | 1749    |
| IVF_FLAT  | nlist=256,nprobe=8   | 8890      | 50522    | 0.2190    | 0.1954     | 600     | 679     | 741     |
| IVF_FLAT  | nlist=256,nprobe=16  | 8890      | 50522    | 0.3560    | 0.3280     | 1140    | 1296    | 1358    |

**Key finding at 100K**: IVF_FLAT provides significant latency reduction even at moderate recall. nlist=32, nprobe=16 gives recall@10=0.76 at 4.5ms (6.9x faster than exact scan at 30.9ms). The latency advantage grows with dataset size.

### Dataset B: Cosine Normalized, 10K vectors, dim=128

| Index     | Parameters          | Build(ms) | Size(KB) | Recall@10 | Recall@100 | p50(us) | p95(us) | p99(us) |
|-----------|---------------------|-----------|----------|-----------|------------|---------|---------|---------|
| Exact Scan | -                 | 0         | 0        | 1.0000    | 1.0000     | 2407    | 2566    | 2640    |
| IVF_FLAT  | nlist=16,nprobe=8   | 2420      | 5047     | 0.7300    | 0.6926     | 502     | 526     | 540     |
| IVF_FLAT  | nlist=16,nprobe=16  | 2420      | 5047     | 1.0000    | 1.0000     | 1000    | 1037    | 1046    |
| IVF_FLAT  | nlist=32,nprobe=8   | 5277      | 5056     | 0.5140    | 0.4580     | 254     | 271     | 283     |
| IVF_FLAT  | nlist=32,nprobe=16  | 5277      | 5056     | 0.7485    | 0.7238     | 498     | 534     | 573     |
| IVF_FLAT  | nlist=128,nprobe=16 | 10783     | 5105     | 0.3965    | 0.3762     | 151     | 181     | 198     |

**Key finding (Cosine)**: Cosine distance recall patterns are similar to L2. IVF_FLAT's k-means clustering (which internally uses L2 on normalized vectors) works effectively for cosine-distance workloads.

### Dataset B: Cosine Normalized, 100K vectors, dim=128

| Index     | Parameters           | Build(ms) | Size(KB) | Recall@10 | Recall@100 | p50(us) | p95(us) | p99(us) |
|-----------|----------------------|-----------|----------|-----------|------------|---------|---------|---------|
| Exact Scan | -                  | 0         | 0        | 1.0000    | 1.0000     | 33936   | 35306   | 36879   |
| IVF_FLAT  | nlist=32,nprobe=16   | 5994      | 50407    | 0.7910    | 0.7453     | 5461    | 5673    | 5814    |
| IVF_FLAT  | nlist=64,nprobe=16   | 7613      | 50423    | 0.5680    | 0.5096     | 2688    | 2881    | 3113    |
| IVF_FLAT  | nlist=128,nprobe=16  | 9926      | 50456    | 0.4055    | 0.3485     | 1259    | 1312    | 1328    |
| IVF_FLAT  | nlist=256,nprobe=16  | 16112     | 50522    | 0.2695    | 0.2308     | 723     | 816     | 866     |

## 4. Observations

### IVF_FLAT Recall vs nlist/nprobe

The parameter sweep on 10K vectors (128 dim, L2) reveals clear trends:

| nlist | nprobe | Recall@10 | p50(us) | Speedup vs Exact |
|-------|--------|-----------|---------|------------------|
| 16    | 8      | 0.7125    | 383     | 6.0x             |
| 16    | 16     | 1.0000    | 775     | 3.0x             |
| 32    | 8      | 0.4910    | 200     | 11.4x            |
| 32    | 16     | 0.7515    | 394     | 5.8x             |
| 32    | 32     | 1.0000    | 777     | 2.9x             |
| 64    | 16     | 0.5250    | 219     | 10.4x            |
| 64    | 32     | 0.7965    | 419     | 5.5x             |
| 128   | 32     | 0.6715    | 296     | 7.7x             |

**Key observations**:

1. **Recall is primarily controlled by the fraction nprobe/nlist**. At nprobe/nlist = 0.5, recall@10 is consistently around 0.70-0.80 regardless of absolute nlist value. At nprobe/nlist = 1.0, recall@10 = 1.0 (exhaustive).

2. **Higher nlist does NOT improve recall for a given nprobe count**. nlist=16 with nprobe=8 (50%) gives recall=0.71, while nlist=128 with nprobe=8 (6.25%) gives only recall=0.28. The fraction of the dataset scanned determines recall.

3. **Higher nlist reduces per-query latency at the same nprobe**. At nprobe=8: nlist=16 takes 383us, nlist=64 takes 115us, nlist=128 takes 92us. This is because each cluster is smaller.

4. **The recall-latency tradeoff is governed by nprobe/nlist ratio, not the absolute values**. For a target recall of 0.70, you need to scan approximately 50% of vectors regardless of nlist.

### When Does Approximate Search Become Worthwhile?

| Dataset Size | Exact p50(us) | IVF_FLAT best at recall>=0.70 | Speedup |
|-------------|---------------|-------------------------------|---------|
| 1K          | 421           | 58us (nlist=8,nprobe=4,R=0.77) | 7.3x   |
| 10K         | 2287          | 383us (nlist=16,nprobe=8,R=0.71) | 6.0x  |
| 100K        | 30937         | 4491us (nlist=32,nprobe=16,R=0.76) | 6.9x |

**At all tested sizes, IVF_FLAT provides a 6-7x speedup for recall in the 0.70-0.80 range.** The absolute latency savings grow with dataset size (from 363us saved at 1K to 26.4ms saved at 100K).

### How Sensitive Is IVF_FLAT to nlist/nprobe?

**nlist sensitivity**: Doubling nlist approximately halves per-query scan cost but also halves recall at fixed nprobe. The net effect on the recall-latency Pareto frontier is modest -- all nlist values produce similar recall-vs-latency curves when normalized.

**nprobe sensitivity**: Each doubling of nprobe approximately doubles latency and adds 10-15 percentage points of recall. This is the primary tuning knob.

### Training Sample Size Impact

At the tested scales (up to 100K), the default `trainSampleSize = max(nlist * 40, 10000)` uses the full dataset for training when N <= 65K. For the 100K dataset, the training subsample of ~10K vectors is sufficient as evidenced by:
- K-means converges within the iteration limit
- Inverted lists are reasonably balanced (no empty clusters observed)
- Recall values are consistent with the nprobe/nlist ratio theory

### Index Size

IVF_FLAT index size is approximately:
- Raw vectors: N * dim * 4 bytes
- Overhead: centroids (nlist * dim * 4) + doc IDs (N * 4) + metadata

For 10K vectors, dim=128: raw vectors = 5120 KB, actual index = 5047-5105 KB. The overhead is minimal because IVF_FLAT stores flat (uncompressed) vectors.

### Build Time

Build time is dominated by k-means training. At 10K vectors:
- nlist=16: 742ms
- nlist=32: 1410ms
- nlist=64: 2885ms
- nlist=128: 3684ms

Build time scales roughly as O(nlist * N * iterations * dim), so doubling nlist roughly doubles build time.

## 5. Recommended Defaults

### nlist

**Recommended default: `nlist = sqrt(N)`**

Rationale:
- At sqrt(N), each cluster contains approximately sqrt(N) vectors
- This balances cluster granularity with the number of clusters to probe
- At N=10K, sqrt(N)=100, close to nlist=128 in our sweep
- At N=100K, sqrt(N)=316

However, for the current IVF_FLAT implementation where recall requires scanning a large fraction of clusters, **a lower nlist (e.g., 16-32 for N <= 10K)** may be more practical, as it allows full-scan recall at reasonable cost.

**Practical recommendation**: `nlist = max(16, min(sqrt(N), 256))`

### nprobe

**Recommended default: `nprobe = max(1, nlist / 4)`**

Rationale:
- At nprobe/nlist = 0.25, recall@10 is typically 0.45-0.55 (reasonable for many use cases)
- Users who need higher recall should increase nprobe
- The default provides a 4x theoretical speedup over exhaustive search
- For nlist=128, this gives nprobe=32, yielding recall@10 around 0.67

For high-recall requirements (recall >= 0.90):
- `nprobe = nlist / 2` to `nprobe = nlist` is needed
- At these settings, IVF_FLAT still provides 2-3x speedup over exact scan

### trainSampleSize

**Recommended default: `min(65536, N)`**

Rationale:
- For N <= 65536, use the full dataset for training (no subsampling)
- For larger datasets, 65536 samples is sufficient for good cluster quality
- Reducing to N/2 or 10K risks degraded cluster balance at high nlist values
- The current default of `max(nlist * 40, 10000)` is good for small nlist but may undersample at nlist=256+

### When to Recommend Each Index Type

| Scenario | Recommendation | Rationale |
|----------|---------------|-----------|
| N < 1000 | **Exact scan** | ANN overhead not justified; exact scan < 500us |
| 1K <= N < 10K, recall >= 0.95 needed | **IVF_FLAT** (nlist=16, nprobe=nlist) | Full scan through IVF is still 3x faster than brute force |
| 1K <= N < 10K, recall >= 0.70 acceptable | **IVF_FLAT** (nlist=16-32, nprobe=nlist/2) | 6x speedup with good recall |
| N >= 10K, latency-sensitive | **IVF_FLAT** (nlist=sqrt(N), nprobe=nlist/4) | Best latency reduction, moderate recall |
| N >= 10K, recall-sensitive | **HNSW** or **IVF_FLAT** (small nlist, high nprobe) | HNSW typically achieves recall > 0.95 at lower latency than IVF_FLAT at equivalent recall |
| Mutable segments (real-time) | **HNSW** | IVF_FLAT is offline-only in phase 1 |

## 6. Known Limitations

### What This Benchmark Does Not Cover

1. **HNSW performance**: The Lucene HNSW reader requires `PinotDataBuffer`/`PluginManager` which cannot be initialized in standalone benchmark mode. HNSW numbers should be obtained via integration tests within a full Pinot runtime.

2. **Multi-threaded query performance**: All measurements are single-threaded. Concurrent query behavior may differ due to cache effects and memory bandwidth.

3. **Disk I/O**: IVF_FLAT loads the entire index into memory at reader construction time. This benchmark does not measure cold-start load times from disk.

4. **Real-world data distributions**: Synthetic Gaussian/uniform data may behave differently from real embedding spaces (which often have cluster structure, varying density, etc.).

5. **Dimensions other than 128**: Higher dimensions (256, 512, 1024) will increase distance computation cost proportionally and may affect the recall-latency tradeoff.

6. **Very large datasets**: N > 100K was not tested. At N = 1M+, IVF_FLAT's in-memory requirement may become a concern, and product quantization (IVF_PQ) would be more appropriate.

### Caveats About Generalization

- **The curse of dimensionality**: At high dimensions (dim >= 100), all vectors become approximately equidistant, making ANN search inherently harder. The recall numbers here reflect this -- achieving recall > 0.90 requires scanning a large fraction of the dataset.

- **Data-dependent recall**: On data with natural cluster structure (e.g., image embeddings), IVF_FLAT recall may be significantly better than on synthetic random data, because k-means clustering aligns with the data's structure.

- **Timing variability**: Latency measurements on a development machine include OS scheduling noise. Production numbers may differ due to CPU frequency, memory bandwidth, and system load.

### Future Work

1. **HNSW comparison in integration test**: Build a Pinot integration test that creates segments with both HNSW and IVF_FLAT indexes and compares query performance within the full Pinot runtime.

2. **Larger scale testing**: Benchmark at N = 500K, 1M, 5M to validate the sqrt(N) nlist heuristic and identify the scale at which IVF_FLAT's in-memory model becomes impractical.

3. **Product quantization (IVF_PQ)**: Adding PQ compression to IVF would reduce memory footprint and potentially improve cache performance at the cost of some recall.

4. **Filtered search**: Benchmark IVF_FLAT with pre-filtering (bitmap intersection) which is a common real-world pattern in Pinot queries.

5. **Variable dimensions**: Test at dim=32 (compact embeddings), dim=256, and dim=768 (transformer embeddings).
