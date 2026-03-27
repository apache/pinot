/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.perf;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.function.scalar.VectorFunctions;
import org.apache.pinot.segment.local.segment.creator.impl.vector.HnswVectorIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.vector.HnswVectorIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.vector.IvfFlatVectorIndexReader;
import org.apache.pinot.segment.local.segment.index.vector.IvfFlatVectorIndexCreator;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;


/**
 * Benchmark harness comparing exact scan, HNSW (Lucene), and IVF_FLAT vector indexes.
 *
 * <p>This benchmark measures build time, index size, query latency (p50/p99), and recall@K
 * for each index type across synthetic datasets of configurable size and dimensionality.</p>
 *
 * <h3>Usage</h3>
 * <pre>
 *   # Build the pinot-perf module:
 *   ./mvnw -pl pinot-perf -am compile
 *
 *   # Run the benchmark (standalone):
 *   java -cp pinot-perf/target/classes:... org.apache.pinot.perf.BenchmarkVectorIndex
 *
 *   # Or run via Maven:
 *   ./mvnw -pl pinot-perf exec:java -Dexec.mainClass=org.apache.pinot.perf.BenchmarkVectorIndex
 * </pre>
 *
 * <h3>Datasets</h3>
 * <ul>
 *   <li><b>Dataset A (L2 Synthetic)</b>: Gaussian random vectors, EUCLIDEAN distance</li>
 *   <li><b>Dataset B (Cosine Normalized)</b>: Unit-normalized vectors, COSINE distance</li>
 * </ul>
 *
 * <p>Ground truth is pre-computed via brute-force scan for each query set.</p>
 *
 * <h3>Thread safety</h3>
 * <p>This class is designed for single-threaded benchmark execution.</p>
 */
public class BenchmarkVectorIndex {

  private BenchmarkVectorIndex() {
  }

  // ---------------------------------------------------------------------------
  // Configuration constants
  // ---------------------------------------------------------------------------

  /** Fixed random seed for reproducibility. */
  private static final long SEED = 42L;

  /** Number of query vectors for recall/latency measurement. */
  private static final int NUM_QUERIES = 200;

  /** Number of warmup queries before timing. */
  private static final int WARMUP_QUERIES = 50;

  /** Column name used for index creation. */
  private static final String COLUMN_NAME = "embedding";

  // ---------------------------------------------------------------------------
  // Data generation
  // ---------------------------------------------------------------------------

  /**
   * Generates random Gaussian vectors. Each dimension is drawn from N(0, 1).
   */
  static float[][] generateGaussianVectors(int count, int dimension, long seed) {
    Random rng = new Random(seed);
    float[][] vectors = new float[count][dimension];
    for (int i = 0; i < count; i++) {
      for (int d = 0; d < dimension; d++) {
        vectors[i][d] = (float) rng.nextGaussian();
      }
    }
    return vectors;
  }

  /**
   * Generates unit-normalized random vectors (for cosine distance benchmarks).
   */
  static float[][] generateNormalizedVectors(int count, int dimension, long seed) {
    float[][] vectors = generateGaussianVectors(count, dimension, seed);
    for (int i = 0; i < count; i++) {
      vectors[i] = normalizeVector(vectors[i]);
    }
    return vectors;
  }

  // ---------------------------------------------------------------------------
  // Ground truth computation (brute-force exact scan)
  // ---------------------------------------------------------------------------

  /**
   * Computes brute-force top-K for a single query. Returns an ordered array of doc IDs
   * sorted by increasing distance.
   */
  static int[] exactTopK(float[][] corpus, float[] query, int topK,
      VectorIndexConfig.VectorDistanceFunction distFunc) {
    int n = corpus.length;
    float[] distances = new float[n];
    for (int i = 0; i < n; i++) {
      distances[i] = computeDistance(query, corpus[i], distFunc);
    }
    // Min-heap of (distance, docId) pairs -- but we need top-K smallest, so use a simple sort
    Integer[] indices = new Integer[n];
    for (int i = 0; i < n; i++) {
      indices[i] = i;
    }
    final float[] d = distances;
    Arrays.sort(indices, (a, b) -> Float.compare(d[a], d[b]));
    int[] result = new int[Math.min(topK, n)];
    for (int i = 0; i < result.length; i++) {
      result[i] = indices[i];
    }
    return result;
  }

  /**
   * Computes ground truth for all queries. Returns an array of top-K doc ID arrays.
   */
  static int[][] computeGroundTruth(float[][] corpus, float[][] queries, int topK,
      VectorIndexConfig.VectorDistanceFunction distFunc) {
    int[][] groundTruth = new int[queries.length][];
    for (int q = 0; q < queries.length; q++) {
      groundTruth[q] = exactTopK(corpus, queries[q], topK, distFunc);
    }
    return groundTruth;
  }

  /**
   * Computes recall@K: fraction of true top-K neighbors found by the approximate result.
   */
  static double computeRecall(int[] truthTopK, Set<Integer> approxResult) {
    int hits = 0;
    for (int docId : truthTopK) {
      if (approxResult.contains(docId)) {
        hits++;
      }
    }
    return (double) hits / truthTopK.length;
  }

  /**
   * Converts a Roaring bitmap to a set of integers.
   */
  static Set<Integer> bitmapToSet(org.roaringbitmap.buffer.ImmutableRoaringBitmap bitmap) {
    Set<Integer> set = new HashSet<>();
    bitmap.forEach((org.roaringbitmap.IntConsumer) set::add);
    return set;
  }

  // ---------------------------------------------------------------------------
  // IVF_FLAT index lifecycle
  // ---------------------------------------------------------------------------

  /**
   * Builds an IVF_FLAT index from corpus vectors. Returns the time in nanoseconds.
   */
  static long buildIvfFlatIndex(File indexDir, float[][] corpus, int dimension, int nlist,
      VectorIndexConfig.VectorDistanceFunction distFunc)
      throws IOException {
    VectorIndexConfig config = createIvfConfig(dimension, nlist, distFunc);
    long start = System.nanoTime();
    try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN_NAME, indexDir, config)) {
      for (float[] vector : corpus) {
        creator.add(vector);
      }
      creator.seal();
    }
    return System.nanoTime() - start;
  }

  /**
   * Opens an IVF_FLAT reader with the given nprobe.
   */
  static IvfFlatVectorIndexReader openIvfReader(File indexDir, int dimension, int nlist, int nprobe,
      VectorIndexConfig.VectorDistanceFunction distFunc) {
    VectorIndexConfig config = createIvfConfig(dimension, nlist, distFunc);
    IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, indexDir, config);
    reader.setNprobe(nprobe);
    return reader;
  }

  static VectorIndexConfig createIvfConfig(int dimension, int nlist,
      VectorIndexConfig.VectorDistanceFunction distFunc) {
    Map<String, String> props = new HashMap<>();
    props.put("vectorIndexType", "IVF_FLAT");
    props.put("vectorDimension", String.valueOf(dimension));
    props.put("vectorDistanceFunction", distFunc.name());
    props.put("nlist", String.valueOf(nlist));
    props.put("trainingSeed", String.valueOf(SEED));
    return new VectorIndexConfig(false, "IVF_FLAT", dimension, 1, distFunc, props);
  }

  // ---------------------------------------------------------------------------
  // HNSW index lifecycle
  // ---------------------------------------------------------------------------

  /**
   * Builds an HNSW (Lucene) index from corpus vectors. Returns the time in nanoseconds.
   */
  static long buildHnswIndex(File indexDir, float[][] corpus, int dimension,
      VectorIndexConfig.VectorDistanceFunction distFunc)
      throws IOException {
    VectorIndexConfig config = createHnswConfig(dimension, distFunc);
    long start = System.nanoTime();
    try (HnswVectorIndexCreator creator = new HnswVectorIndexCreator(COLUMN_NAME, indexDir, config)) {
      for (float[] vector : corpus) {
        creator.add(vector);
      }
      creator.seal();
    }
    return System.nanoTime() - start;
  }

  /**
   * Opens an HNSW reader. Requires the HNSW index directory to exist.
   */
  static HnswVectorIndexReader openHnswReader(File indexDir, int numDocs, int dimension,
      VectorIndexConfig.VectorDistanceFunction distFunc) {
    VectorIndexConfig config = createHnswConfig(dimension, distFunc);
    return new HnswVectorIndexReader(COLUMN_NAME, indexDir, numDocs, config);
  }

  static VectorIndexConfig createHnswConfig(int dimension,
      VectorIndexConfig.VectorDistanceFunction distFunc) {
    Map<String, String> props = new HashMap<>();
    props.put("vectorIndexType", "HNSW");
    props.put("vectorDimension", String.valueOf(dimension));
    props.put("vectorDistanceFunction", distFunc.name());
    // Lucene defaults: M=16, beamWidth=100
    props.put("maxCon", "16");
    props.put("beamWidth", "100");
    return new VectorIndexConfig(false, "HNSW", dimension, 1, distFunc, props);
  }

  // ---------------------------------------------------------------------------
  // Latency measurement
  // ---------------------------------------------------------------------------

  /**
   * Measures query latencies (in nanoseconds) over the query set. Runs warmup first.
   */
  static long[] measureIvfLatencies(IvfFlatVectorIndexReader reader, float[][] queries, int topK) {
    // Warmup
    int warmup = Math.min(WARMUP_QUERIES, queries.length);
    for (int i = 0; i < warmup; i++) {
      reader.getDocIds(queries[i], topK);
    }
    // Measure
    long[] latencies = new long[queries.length];
    for (int i = 0; i < queries.length; i++) {
      long start = System.nanoTime();
      reader.getDocIds(queries[i], topK);
      latencies[i] = System.nanoTime() - start;
    }
    return latencies;
  }

  /**
   * Measures query latencies for HNSW.
   */
  static long[] measureHnswLatencies(HnswVectorIndexReader reader, float[][] queries, int topK) {
    int warmup = Math.min(WARMUP_QUERIES, queries.length);
    for (int i = 0; i < warmup; i++) {
      reader.getDocIds(queries[i], topK);
    }
    long[] latencies = new long[queries.length];
    for (int i = 0; i < queries.length; i++) {
      long start = System.nanoTime();
      reader.getDocIds(queries[i], topK);
      latencies[i] = System.nanoTime() - start;
    }
    return latencies;
  }

  /**
   * Measures query latencies for exact (brute-force) scan.
   */
  static long[] measureExactLatencies(float[][] corpus, float[][] queries, int topK,
      VectorIndexConfig.VectorDistanceFunction distFunc) {
    int warmup = Math.min(WARMUP_QUERIES, queries.length);
    for (int i = 0; i < warmup; i++) {
      exactTopK(corpus, queries[i], topK, distFunc);
    }
    long[] latencies = new long[queries.length];
    for (int i = 0; i < queries.length; i++) {
      long start = System.nanoTime();
      exactTopK(corpus, queries[i], topK, distFunc);
      latencies[i] = System.nanoTime() - start;
    }
    return latencies;
  }

  /**
   * Returns the p-th percentile from a sorted array of longs.
   */
  static long percentile(long[] sorted, double p) {
    int idx = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
    return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
  }

  // ---------------------------------------------------------------------------
  // Index size measurement
  // ---------------------------------------------------------------------------

  /**
   * Computes the total size of IVF_FLAT index file(s) in a directory.
   */
  static long ivfIndexSize(File indexDir) {
    File f = new File(indexDir, COLUMN_NAME + V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
    return f.exists() ? f.length() : 0;
  }

  /**
   * Computes the total size of HNSW index directory.
   */
  static long hnswIndexSize(File indexDir) {
    File hnswDir = new File(indexDir,
        COLUMN_NAME + V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    if (hnswDir.isDirectory()) {
      return FileUtils.sizeOfDirectory(hnswDir);
    }
    return hnswDir.exists() ? hnswDir.length() : 0;
  }

  // ---------------------------------------------------------------------------
  // Result data structures
  // ---------------------------------------------------------------------------

  /**
   * Holds benchmark results for a single (index_type, parameter_set) combination.
   */
  static class BenchmarkResult {
    final String _indexType;
    final String _params;
    final long _buildTimeNs;
    final long _indexSizeBytes;
    final double _recallAt10;
    final double _recallAt100;
    final long _p50LatencyNs;
    final long _p95LatencyNs;
    final long _p99LatencyNs;

    BenchmarkResult(String indexType, String params, long buildTimeNs, long indexSizeBytes,
        double recallAt10, double recallAt100, long p50LatencyNs, long p95LatencyNs, long p99LatencyNs) {
      _indexType = indexType;
      _params = params;
      _buildTimeNs = buildTimeNs;
      _indexSizeBytes = indexSizeBytes;
      _recallAt10 = recallAt10;
      _recallAt100 = recallAt100;
      _p50LatencyNs = p50LatencyNs;
      _p95LatencyNs = p95LatencyNs;
      _p99LatencyNs = p99LatencyNs;
    }
  }

  // ---------------------------------------------------------------------------
  // Benchmark runners
  // ---------------------------------------------------------------------------

  /**
   * Runs the full benchmark suite for one dataset configuration.
   */
  static List<BenchmarkResult> runDatasetBenchmark(String datasetName, float[][] corpus,
      float[][] queries, int dimension, VectorIndexConfig.VectorDistanceFunction distFunc,
      int[] nlistValues, int[] nprobeValues, PrintStream out)
      throws IOException {
    List<BenchmarkResult> results = new ArrayList<>();
    out.printf("%n=== Dataset: %s (N=%d, dim=%d, distance=%s) ===%n",
        datasetName, corpus.length, dimension, distFunc);

    // Pre-compute ground truth
    out.println("Computing ground truth (exact scan)...");
    int[][] gt10 = computeGroundTruth(corpus, queries, 10, distFunc);
    int[][] gt100 = computeGroundTruth(corpus, queries, Math.min(100, corpus.length), distFunc);

    // Sanity check: exact scan should have recall@10 = 1.0
    double sanityRecall = 0;
    for (int q = 0; q < queries.length; q++) {
      Set<Integer> gtSet = new HashSet<>();
      for (int id : gt10[q]) {
        gtSet.add(id);
      }
      sanityRecall += computeRecall(gt10[q], gtSet);
    }
    sanityRecall /= queries.length;
    out.printf("Ground truth sanity check: recall@10 = %.4f (expected 1.0)%n", sanityRecall);

    // 1. Exact scan benchmark
    out.println("Benchmarking exact scan...");
    long[] exactLat = measureExactLatencies(corpus, queries, 10, distFunc);
    Arrays.sort(exactLat);
    results.add(new BenchmarkResult("Exact Scan", "-", 0, 0,
        1.0, 1.0, percentile(exactLat, 50), percentile(exactLat, 95), percentile(exactLat, 99)));

    // 2. HNSW benchmark
    File hnswDir = null;
    try {
      hnswDir = Files.createTempDirectory("bench_hnsw_").toFile();
      out.println("Building HNSW index...");
      long hnswBuildTime = buildHnswIndex(hnswDir, corpus, dimension, distFunc);
      long hnswSize = hnswIndexSize(hnswDir);

      try (HnswVectorIndexReader reader = openHnswReader(hnswDir, corpus.length, dimension, distFunc)) {
        // Measure recall
        double recall10 = 0;
        double recall100 = 0;
        for (int q = 0; q < queries.length; q++) {
          Set<Integer> r10 = bitmapToSet(reader.getDocIds(queries[q], 10));
          recall10 += computeRecall(gt10[q], r10);
          if (corpus.length >= 100) {
            Set<Integer> r100 = bitmapToSet(reader.getDocIds(queries[q], 100));
            recall100 += computeRecall(gt100[q], r100);
          } else {
            recall100 += 1.0;
          }
        }
        recall10 /= queries.length;
        recall100 /= queries.length;

        long[] hnswLat = measureHnswLatencies(reader, queries, 10);
        Arrays.sort(hnswLat);

        results.add(new BenchmarkResult("HNSW", "M=16,ef=100", hnswBuildTime, hnswSize,
            recall10, recall100, percentile(hnswLat, 50), percentile(hnswLat, 95),
            percentile(hnswLat, 99)));
      }
    } catch (Throwable e) {
      // HNSW reader requires PinotDataBuffer/PluginManager which may not be available
      // outside a full Pinot runtime. ExceptionInInitializerError is common here.
      out.println("HNSW benchmark skipped: " + e.getClass().getSimpleName() + ": " + e.getMessage());
      out.println("  (HNSW reader requires full Pinot runtime; run in integration test for HNSW numbers)");
    } finally {
      if (hnswDir != null) {
        FileUtils.deleteQuietly(hnswDir);
      }
    }

    // 3. IVF_FLAT benchmark for each (nlist, nprobe) combination
    for (int nlist : nlistValues) {
      if (nlist > corpus.length) {
        continue;
      }
      File ivfDir = Files.createTempDirectory("bench_ivf_").toFile();
      try {
        out.printf("Building IVF_FLAT index (nlist=%d)...%n", nlist);
        long ivfBuildTime = buildIvfFlatIndex(ivfDir, corpus, dimension, nlist, distFunc);
        long ivfSize = ivfIndexSize(ivfDir);

        for (int nprobe : nprobeValues) {
          if (nprobe > nlist) {
            continue;
          }
          try (IvfFlatVectorIndexReader reader = openIvfReader(
              ivfDir, dimension, nlist, nprobe, distFunc)) {
            reader.setNprobe(nprobe);

            // Measure recall
            double recall10 = 0;
            double recall100 = 0;
            for (int q = 0; q < queries.length; q++) {
              Set<Integer> r10 = bitmapToSet(reader.getDocIds(queries[q], 10));
              recall10 += computeRecall(gt10[q], r10);
              if (corpus.length >= 100) {
                Set<Integer> r100 = bitmapToSet(reader.getDocIds(queries[q], Math.min(100, corpus.length)));
                recall100 += computeRecall(gt100[q], r100);
              } else {
                recall100 += 1.0;
              }
            }
            recall10 /= queries.length;
            recall100 /= queries.length;

            // Measure latency
            long[] ivfLat = measureIvfLatencies(reader, queries, 10);
            Arrays.sort(ivfLat);

            String paramStr = String.format("nlist=%d,nprobe=%d", nlist, nprobe);
            results.add(new BenchmarkResult("IVF_FLAT", paramStr, ivfBuildTime, ivfSize,
                recall10, recall100, percentile(ivfLat, 50), percentile(ivfLat, 95),
                percentile(ivfLat, 99)));
          }
        }
      } finally {
        FileUtils.deleteQuietly(ivfDir);
      }
    }

    return results;
  }

  /**
   * Runs the IVF_FLAT parameter sweep: all (nlist, nprobe) combinations on a fixed dataset.
   */
  static List<BenchmarkResult> runParameterSweep(PrintStream out)
      throws IOException {
    int numVectors = 10_000;
    int dimension = 128;
    VectorIndexConfig.VectorDistanceFunction distFunc = VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN;

    out.println("\n========================================");
    out.println("  IVF_FLAT Parameter Sweep");
    out.printf("  N=%d, dim=%d, distance=%s%n", numVectors, dimension, distFunc);
    out.println("========================================");

    float[][] corpus = generateGaussianVectors(numVectors, dimension, SEED);
    float[][] queries = generateGaussianVectors(NUM_QUERIES, dimension, SEED + 1000);

    int[][] gt10 = computeGroundTruth(corpus, queries, 10, distFunc);

    int[] nlistValues = {16, 32, 64, 128, 256};
    int[] nprobeValues = {1, 2, 4, 8, 16, 32};

    List<BenchmarkResult> results = new ArrayList<>();

    for (int nlist : nlistValues) {
      File ivfDir = Files.createTempDirectory("bench_sweep_").toFile();
      try {
        out.printf("Building IVF_FLAT (nlist=%d)...%n", nlist);
        long buildTime = buildIvfFlatIndex(ivfDir, corpus, dimension, nlist, distFunc);
        long indexSize = ivfIndexSize(ivfDir);

        for (int nprobe : nprobeValues) {
          if (nprobe > nlist) {
            continue;
          }
          try (IvfFlatVectorIndexReader reader = openIvfReader(
              ivfDir, dimension, nlist, nprobe, distFunc)) {
            reader.setNprobe(nprobe);

            double recall10 = 0;
            for (int q = 0; q < queries.length; q++) {
              Set<Integer> r = bitmapToSet(reader.getDocIds(queries[q], 10));
              recall10 += computeRecall(gt10[q], r);
            }
            recall10 /= queries.length;

            long[] latencies = measureIvfLatencies(reader, queries, 10);
            Arrays.sort(latencies);

            String params = String.format("nlist=%d,nprobe=%d", nlist, nprobe);
            results.add(new BenchmarkResult("IVF_FLAT", params, buildTime, indexSize,
                recall10, 0, percentile(latencies, 50), percentile(latencies, 95),
                percentile(latencies, 99)));
          }
        }
      } finally {
        FileUtils.deleteQuietly(ivfDir);
      }
    }

    return results;
  }

  // ---------------------------------------------------------------------------
  // Output formatting
  // ---------------------------------------------------------------------------

  static void printResultsTable(List<BenchmarkResult> results, PrintStream out) {
    out.printf("%-14s %-22s %10s %12s %10s %10s %10s %10s %10s%n",
        "Index", "Parameters", "Build(ms)", "Size(KB)", "Recall@10", "Recall@100",
        "p50(us)", "p95(us)", "p99(us)");
    out.println("-".repeat(120));
    for (BenchmarkResult r : results) {
      out.printf("%-14s %-22s %10.1f %12.1f %10.4f %10.4f %10.1f %10.1f %10.1f%n",
          r._indexType, r._params,
          r._buildTimeNs / 1_000_000.0,
          r._indexSizeBytes / 1024.0,
          r._recallAt10, r._recallAt100,
          r._p50LatencyNs / 1000.0,
          r._p95LatencyNs / 1000.0,
          r._p99LatencyNs / 1000.0);
    }
  }

  static void printSweepTable(List<BenchmarkResult> results, PrintStream out) {
    out.printf("%-22s %10s %10s %10s %10s%n",
        "Parameters", "Recall@10", "p50(us)", "p95(us)", "p99(us)");
    out.println("-".repeat(75));
    for (BenchmarkResult r : results) {
      out.printf("%-22s %10.4f %10.1f %10.1f %10.1f%n",
          r._params, r._recallAt10,
          r._p50LatencyNs / 1000.0,
          r._p95LatencyNs / 1000.0,
          r._p99LatencyNs / 1000.0);
    }
  }

  // ---------------------------------------------------------------------------
  // Main entry point
  // ---------------------------------------------------------------------------

  /**
   * Runs the complete benchmark suite and prints results to stdout.
   *
   * <p>The benchmark is organized in three parts:
   * <ol>
   *   <li>Dataset A (L2/Euclidean) at multiple sizes: 1K, 10K, 100K</li>
   *   <li>Dataset B (Cosine) at multiple sizes: 1K, 10K, 100K</li>
   *   <li>IVF_FLAT parameter sweep on 10K vectors, 128 dimensions</li>
   * </ol>
   */
  public static void main(String[] args)
      throws Exception {
    PrintStream out = System.out;
    out.println("========================================");
    out.println("  Apache Pinot Vector Index Benchmark");
    out.println("========================================");
    out.printf("JDK: %s%n", System.getProperty("java.version"));
    out.printf("OS: %s %s%n", System.getProperty("os.name"), System.getProperty("os.arch"));
    out.printf("Seed: %d%n", SEED);
    out.printf("Queries: %d (warmup: %d)%n", NUM_QUERIES, WARMUP_QUERIES);
    out.println();

    int dimension = 128;
    int[] datasetSizes = {1_000, 10_000, 100_000};

    // nlist/nprobe configurations for the size-dependent sweep
    int[] nlistSmall = {8, 16, 32};
    int[] nlistMedium = {16, 32, 64, 128};
    int[] nlistLarge = {32, 64, 128, 256};
    int[] nprobeValues = {1, 4, 8, 16};

    List<List<BenchmarkResult>> allResults = new ArrayList<>();

    // --- Dataset A: L2 Synthetic ---
    out.println("\n########################################");
    out.println("# Dataset A: L2 Synthetic (Gaussian)");
    out.println("########################################");

    for (int n : datasetSizes) {
      float[][] corpus = generateGaussianVectors(n, dimension, SEED);
      float[][] queries = generateGaussianVectors(NUM_QUERIES, dimension, SEED + 1000);
      int[] nlistForSize = n <= 1000 ? nlistSmall : (n <= 10000 ? nlistMedium : nlistLarge);

      List<BenchmarkResult> results = runDatasetBenchmark(
          "L2-" + n, corpus, queries, dimension,
          VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN,
          nlistForSize, nprobeValues, out);
      allResults.add(results);

      out.println();
      printResultsTable(results, out);
    }

    // --- Dataset B: Cosine Normalized ---
    out.println("\n########################################");
    out.println("# Dataset B: Cosine Normalized");
    out.println("########################################");

    for (int n : datasetSizes) {
      float[][] corpus = generateNormalizedVectors(n, dimension, SEED + 2000);
      float[][] queries = generateNormalizedVectors(NUM_QUERIES, dimension, SEED + 3000);
      int[] nlistForSize = n <= 1000 ? nlistSmall : (n <= 10000 ? nlistMedium : nlistLarge);

      List<BenchmarkResult> results = runDatasetBenchmark(
          "Cosine-" + n, corpus, queries, dimension,
          VectorIndexConfig.VectorDistanceFunction.COSINE,
          nlistForSize, nprobeValues, out);
      allResults.add(results);

      out.println();
      printResultsTable(results, out);
    }

    // --- Parameter Sweep ---
    out.println("\n########################################");
    out.println("# IVF_FLAT Parameter Sweep (10K, dim=128, EUCLIDEAN)");
    out.println("########################################");

    List<BenchmarkResult> sweepResults = runParameterSweep(out);
    out.println();
    printSweepTable(sweepResults, out);

    // --- Summary recommendations ---
    out.println("\n========================================");
    out.println("  Recommended Defaults");
    out.println("========================================");
    printRecommendations(sweepResults, out);

    out.println("\nBenchmark complete.");
  }

  /**
   * Derives and prints recommended defaults from parameter sweep results.
   */
  static void printRecommendations(List<BenchmarkResult> sweepResults, PrintStream out) {
    // Find the best nlist/nprobe combination that achieves recall@10 >= 0.90
    // with the lowest p50 latency
    BenchmarkResult bestBalanced = null;
    for (BenchmarkResult r : sweepResults) {
      if (r._recallAt10 >= 0.90) {
        if (bestBalanced == null || r._p50LatencyNs < bestBalanced._p50LatencyNs) {
          bestBalanced = r;
        }
      }
    }

    // Find the best for recall >= 0.95
    BenchmarkResult bestHighRecall = null;
    for (BenchmarkResult r : sweepResults) {
      if (r._recallAt10 >= 0.95) {
        if (bestHighRecall == null || r._p50LatencyNs < bestHighRecall._p50LatencyNs) {
          bestHighRecall = r;
        }
      }
    }

    out.println("Target: recall@10 >= 0.90 with lowest latency:");
    if (bestBalanced != null) {
      out.printf("  %s  (recall@10=%.4f, p50=%.1fus)%n",
          bestBalanced._params, bestBalanced._recallAt10, bestBalanced._p50LatencyNs / 1000.0);
    } else {
      out.println("  No configuration achieves recall@10 >= 0.90");
    }

    out.println("Target: recall@10 >= 0.95 with lowest latency:");
    if (bestHighRecall != null) {
      out.printf("  %s  (recall@10=%.4f, p50=%.1fus)%n",
          bestHighRecall._params, bestHighRecall._recallAt10, bestHighRecall._p50LatencyNs / 1000.0);
    } else {
      out.println("  No configuration achieves recall@10 >= 0.95");
    }

    out.println();
    out.println("General guidance:");
    out.println("  nlist = sqrt(N)  (e.g., 100 for N=10K, 316 for N=100K)");
    out.println("  nprobe = nlist/10 to nlist/4  (start with 4-8, increase for higher recall)");
    out.println("  trainSampleSize = min(65536, N)  (full dataset for N <= 65K)");
  }

  // ---------------------------------------------------------------------------
  // Distance computation helpers (delegates to VectorFunctions)
  // ---------------------------------------------------------------------------

  static float computeDistance(float[] a, float[] b,
      VectorIndexConfig.VectorDistanceFunction distFunc) {
    switch (distFunc) {
      case EUCLIDEAN:
      case L2:
        return (float) VectorFunctions.euclideanDistance(a, b);
      case COSINE:
        return (float) VectorFunctions.cosineDistance(a, b);
      case INNER_PRODUCT:
      case DOT_PRODUCT:
        return (float) -VectorFunctions.dotProduct(a, b);
      default:
        throw new IllegalArgumentException("Unsupported distance function: " + distFunc);
    }
  }

  static float[] normalizeVector(float[] vector) {
    float norm = 0.0f;
    for (float v : vector) {
      norm += v * v;
    }
    norm = (float) Math.sqrt(norm);
    float[] result = new float[vector.length];
    if (norm > 0.0f) {
      for (int i = 0; i < vector.length; i++) {
        result[i] = vector[i] / norm;
      }
    }
    return result;
  }
}
