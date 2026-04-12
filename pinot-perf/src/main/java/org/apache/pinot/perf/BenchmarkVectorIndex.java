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
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
import org.apache.pinot.segment.spi.index.creator.VectorQuantizerType;


/**
 * Benchmark harness comparing exact scan, HNSW (Lucene), IVF_FLAT, and IVF_PQ vector indexes.
 *
 * <p>This benchmark measures build time, index size, query latency (p50/p99), and recall@K
 * for each index type across synthetic datasets of configurable size and dimensionality.</p>
 *
 * <p>This class is also the canonical entry point for the broader vector benchmark suite.
 * Set {@code -Dpinot.perf.vector.mode=} to one of {@code frontier}, {@code sanity},
 * {@code filters}, {@code features}, or {@code suite} to select the scenario to run.</p>
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
  private static final int NUM_QUERIES = Integer.getInteger("pinot.perf.vector.queries", 200);

  /** Number of warmup queries before timing. */
  private static final int WARMUP_QUERIES = Integer.getInteger("pinot.perf.vector.warmupQueries", 50);

  /** Column name used for index creation. */
  private static final String COLUMN_NAME = "embedding";

  /** Poll interval for heap sampling during index builds. */
  private static final long MEMORY_POLL_INTERVAL_MS = Long.getLong("pinot.perf.vector.memoryPollMs", 10L);

  /** Backend name used for IVF_PQ frontier runs. */
  private static final String IVF_PQ_INDEX_TYPE = "IVF_PQ";

  /** System property selecting which vector benchmark mode to run. */
  private static final String MODE_PROPERTY = "pinot.perf.vector.mode";

  /** Default benchmark mode when no explicit mode is supplied. */
  private static final String DEFAULT_MODE = "frontier";

  /** Fully qualified creator class name for IVF_PQ. */
  private static final String IVF_PQ_CREATOR_CLASS =
      "org.apache.pinot.segment.local.segment.index.vector.IvfPqVectorIndexCreator";

  /** Fully qualified reader class name for IVF_PQ. */
  private static final String IVF_PQ_READER_CLASS =
      "org.apache.pinot.segment.local.segment.index.readers.vector.IvfPqVectorIndexReader";

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

  /**
   * Generates Gaussian vectors with widely varying magnitudes to stress INNER_PRODUCT ranking.
   */
  static float[][] generateMagnitudeSkewedVectors(int count, int dimension, long seed) {
    Random rng = new Random(seed ^ 0x9E3779B97F4A7C15L);
    float[][] vectors = generateGaussianVectors(count, dimension, seed);
    for (int i = 0; i < count; i++) {
      float scale = (float) Math.pow(2.0d, (rng.nextDouble() * 6.0d) - 3.0d);
      for (int d = 0; d < dimension; d++) {
        vectors[i][d] *= scale;
      }
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
  static BuildMetrics buildIvfFlatIndex(File indexDir, float[][] corpus, int dimension, int nlist,
      VectorIndexConfig.VectorDistanceFunction distFunc)
      throws IOException {
    VectorIndexConfig config = createIvfConfig(dimension, nlist, distFunc);
    return measureBuild(corpus.length, () -> {
      try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN_NAME, indexDir, config)) {
        for (float[] vector : corpus) {
          creator.add(vector);
        }
        creator.seal();
      }
    });
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
    return createIvfConfig("IVF_FLAT", dimension, nlist, distFunc, VectorQuantizerType.FLAT);
  }

  static VectorIndexConfig createIvfConfig(String indexType, int dimension, int nlist,
      VectorIndexConfig.VectorDistanceFunction distFunc, VectorQuantizerType quantizerType) {
    Map<String, String> props = new HashMap<>();
    props.put("vectorIndexType", indexType);
    props.put("vectorDimension", String.valueOf(dimension));
    props.put("vectorDistanceFunction", distFunc.name());
    props.put("nlist", String.valueOf(nlist));
    props.put("trainingSeed", String.valueOf(SEED));
    props.put("quantizer", quantizerType.name());
    return new VectorIndexConfig(false, indexType, dimension, 1, distFunc, props);
  }

  /**
   * Builds an IVF_PQ config with the supplied coarse quantizer and product quantizer knobs.
   */
  static VectorIndexConfig createIvfPqConfig(int dimension, int corpusSize, int nlist, int pqM, int pqNbits,
      VectorIndexConfig.VectorDistanceFunction distFunc) {
    Map<String, String> props = new HashMap<>();
    props.put("vectorIndexType", IVF_PQ_INDEX_TYPE);
    props.put("vectorDimension", String.valueOf(dimension));
    props.put("vectorDistanceFunction", distFunc.name());
    props.put("nlist", String.valueOf(nlist));
    props.put("pqM", String.valueOf(pqM));
    props.put("pqNbits", String.valueOf(pqNbits));
    props.put("trainSampleSize", String.valueOf(Math.max(nlist, Math.min(65536, corpusSize))));
    props.put("trainingSeed", String.valueOf(SEED));
    return new VectorIndexConfig(false, IVF_PQ_INDEX_TYPE, dimension, 1, distFunc, props);
  }

  /**
   * Returns true if the named class is available on the runtime classpath.
   */
  static boolean isClassAvailable(String className) {
    try {
      Class.forName(className);
      return true;
    } catch (ClassNotFoundException | LinkageError e) {
      return false;
    }
  }

  /**
   * Builds a vector index through reflection. This keeps the harness forward-compatible with
   * backend implementations that may not exist in the current checkout.
   */
  static BuildMetrics buildVectorIndexReflectively(String creatorClassName, File indexDir, float[][] corpus,
      VectorIndexConfig config)
      throws IOException {
    try {
      return measureBuild(corpus.length, () -> {
        try {
          Class<?> creatorClass = Class.forName(creatorClassName);
          Constructor<?> constructor = creatorClass.getConstructor(String.class, File.class, VectorIndexConfig.class);
          Object creator = constructor.newInstance(COLUMN_NAME, indexDir, config);
          Method addMethod = creatorClass.getMethod("add", float[].class);
          Method sealMethod = creatorClass.getMethod("seal");
          Method closeMethod = findCloseMethod(creatorClass);
          try {
            for (float[] vector : corpus) {
              addMethod.invoke(creator, (Object) vector);
            }
            sealMethod.invoke(creator);
          } finally {
            if (closeMethod != null) {
              closeMethod.invoke(creator);
            }
          }
        } catch (ReflectiveOperationException e) {
          throw new UndeclaredThrowableException(e);
        }
      });
    } catch (UndeclaredThrowableException e) {
      Throwable cause = e.getUndeclaredThrowable();
      if (cause instanceof ReflectiveOperationException) {
        throw new IOException("Failed to build vector index with " + creatorClassName, cause);
      }
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw e;
    }
  }

  /**
   * Opens a reflected IVF reader and exposes the minimal query surface used by the benchmark.
   */
  static ReflectiveVectorReader openReflectiveVectorReader(String readerClassName, File indexDir, int numDocs,
      VectorIndexConfig config)
      throws IOException {
    try {
      Class<?> readerClass = Class.forName(readerClassName);
      Object reader;
      try {
        Constructor<?> constructor =
            readerClass.getConstructor(String.class, File.class, VectorIndexConfig.class);
        reader = constructor.newInstance(COLUMN_NAME, indexDir, config);
      } catch (NoSuchMethodException e) {
        Constructor<?> constructor = readerClass.getConstructor(String.class, File.class, int.class,
            VectorIndexConfig.class);
        reader = constructor.newInstance(COLUMN_NAME, indexDir, numDocs, config);
      }
      return new ReflectiveVectorReader(reader);
    } catch (ReflectiveOperationException e) {
      throw new IOException("Failed to open vector reader with " + readerClassName, e);
    }
  }

  /**
   * Looks up a close method if the backend exposes one.
   */
  static Method findCloseMethod(Class<?> type) {
    try {
      return type.getMethod("close");
    } catch (NoSuchMethodException e) {
      return null;
    }
  }

  /**
   * Simple reflective wrapper for IVF readers.
   */
  static final class ReflectiveVectorReader implements AutoCloseable {
    private final Object _delegate;
    private final Method _getDocIdsMethod;
    private final Method _setNprobeMethod;
    private final Method _closeMethod;

    ReflectiveVectorReader(Object delegate)
        throws ReflectiveOperationException {
      _delegate = delegate;
      Class<?> type = delegate.getClass();
      _getDocIdsMethod = type.getMethod("getDocIds", float[].class, int.class);
      _setNprobeMethod = type.getMethod("setNprobe", int.class);
      _closeMethod = findCloseMethod(type);
    }

    void setNprobe(int nprobe)
        throws IOException {
      try {
        _setNprobeMethod.invoke(_delegate, nprobe);
      } catch (ReflectiveOperationException e) {
        throw new IOException("Failed to set nprobe on reflected reader", e);
      }
    }

    org.roaringbitmap.buffer.ImmutableRoaringBitmap getDocIds(float[] query, int topK)
        throws IOException {
      try {
        return (org.roaringbitmap.buffer.ImmutableRoaringBitmap) _getDocIdsMethod.invoke(_delegate, query, topK);
      } catch (ReflectiveOperationException e) {
        throw new IOException("Failed to query reflected reader", e);
      }
    }

    @Override
    public void close()
        throws IOException {
      if (_closeMethod == null) {
        return;
      }
      try {
        _closeMethod.invoke(_delegate);
      } catch (ReflectiveOperationException e) {
        throw new IOException("Failed to close reflected reader", e);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // HNSW index lifecycle
  // ---------------------------------------------------------------------------

  /**
   * Builds an HNSW (Lucene) index from corpus vectors. Returns the time in nanoseconds.
   */
  static BuildMetrics buildHnswIndex(File indexDir, float[][] corpus, int dimension,
      VectorIndexConfig.VectorDistanceFunction distFunc)
      throws IOException {
    VectorIndexConfig config = createHnswConfig(dimension, distFunc);
    return measureBuild(corpus.length, () -> {
      try (HnswVectorIndexCreator creator = new HnswVectorIndexCreator(COLUMN_NAME, indexDir, config)) {
        for (float[] vector : corpus) {
          creator.add(vector);
        }
        creator.seal();
      }
    });
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
   * Measures query latencies for a reader accessed through reflection.
   */
  static long[] measureReflectiveLatencies(ReflectiveVectorReader reader, float[][] queries, int topK)
      throws IOException {
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

  /**
   * Converts a set of single-threaded query latencies into batch queries-per-second.
   */
  static double queriesPerSecond(long[] latencies) {
    long totalLatencyNs = 0L;
    for (long latency : latencies) {
      totalLatencyNs += latency;
    }
    return totalLatencyNs > 0 ? (latencies.length * 1_000_000_000.0d) / totalLatencyNs : 0.0d;
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

  /**
   * Computes the total size of a directory.
   */
  static long directorySize(File indexDir) {
    return FileUtils.sizeOfDirectory(indexDir);
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
    final double _buildDocsPerSecond;
    final long _peakHeapBytes;
    final long _indexSizeBytes;
    final double _recallAt10;
    final double _recallAt100;
    final long _p50LatencyNs;
    final long _p95LatencyNs;
    final long _p99LatencyNs;
    final double _queriesPerSecond;

    BenchmarkResult(String indexType, String params, long buildTimeNs, double buildDocsPerSecond, long peakHeapBytes,
        long indexSizeBytes,
        double recallAt10, double recallAt100, long p50LatencyNs, long p95LatencyNs, long p99LatencyNs,
        double queriesPerSecond) {
      _indexType = indexType;
      _params = params;
      _buildTimeNs = buildTimeNs;
      _buildDocsPerSecond = buildDocsPerSecond;
      _peakHeapBytes = peakHeapBytes;
      _indexSizeBytes = indexSizeBytes;
      _recallAt10 = recallAt10;
      _recallAt100 = recallAt100;
      _p50LatencyNs = p50LatencyNs;
      _p95LatencyNs = p95LatencyNs;
      _p99LatencyNs = p99LatencyNs;
      _queriesPerSecond = queriesPerSecond;
    }
  }

  static final class DatasetScenario {
    final String _name;
    final float[][] _corpus;
    final float[][] _queries;
    final int _dimension;
    final VectorIndexConfig.VectorDistanceFunction _distanceFunction;

    DatasetScenario(String name, float[][] corpus, float[][] queries, int dimension,
        VectorIndexConfig.VectorDistanceFunction distanceFunction) {
      _name = name;
      _corpus = corpus;
      _queries = queries;
      _dimension = dimension;
      _distanceFunction = distanceFunction;
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
      int[] nlistValues, int[] nprobeValues, int[] pqMValues, int[] pqNbitsValues, PrintStream out)
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
    results.add(new BenchmarkResult("Exact Scan", "-", 0, 0.0, 0, 0,
        1.0, 1.0, percentile(exactLat, 50), percentile(exactLat, 95), percentile(exactLat, 99),
        queriesPerSecond(exactLat)));

    // 2. HNSW benchmark
    File hnswDir = null;
    try {
      hnswDir = Files.createTempDirectory("bench_hnsw_").toFile();
      out.println("Building HNSW index...");
      BuildMetrics hnswBuildMetrics = buildHnswIndex(hnswDir, corpus, dimension, distFunc);
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

        results.add(new BenchmarkResult("HNSW", "M=16,ef=100", hnswBuildMetrics._buildTimeNs,
            hnswBuildMetrics._buildDocsPerSecond, hnswBuildMetrics._peakHeapBytes, hnswSize,
            recall10, recall100, percentile(hnswLat, 50), percentile(hnswLat, 95),
            percentile(hnswLat, 99), queriesPerSecond(hnswLat)));
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

    // 3. IVF_FLAT benchmark for each (quantizer, nlist, nprobe) combination
    for (VectorQuantizerType quantizerType
        : Arrays.asList(VectorQuantizerType.FLAT, VectorQuantizerType.SQ8, VectorQuantizerType.SQ4)) {
      for (int nlist : nlistValues) {
        if (nlist > corpus.length) {
          continue;
        }
        File ivfDir = Files.createTempDirectory("bench_ivf_").toFile();
        try {
          VectorIndexConfig config = createIvfConfig("IVF_FLAT", dimension, nlist, distFunc, quantizerType);
          out.printf("Building IVF_FLAT index (quantizer=%s, nlist=%d)...%n", quantizerType.name(), nlist);
          BuildMetrics ivfBuildMetrics = measureBuild(corpus.length, () -> {
            try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN_NAME, ivfDir, config)) {
              for (float[] vector : corpus) {
                creator.add(vector);
              }
              creator.seal();
            }
          });
          long ivfSize = ivfIndexSize(ivfDir);

          for (int nprobe : nprobeValues) {
            if (nprobe > nlist) {
              continue;
            }
            try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, ivfDir, config)) {
              reader.setNprobe(nprobe);

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

              long[] ivfLat = measureIvfLatencies(reader, queries, 10);
              Arrays.sort(ivfLat);

              String paramStr = String.format("q=%s,nlist=%d,nprobe=%d", quantizerType.name(), nlist, nprobe);
              results.add(new BenchmarkResult("IVF_FLAT", paramStr, ivfBuildMetrics._buildTimeNs,
                  ivfBuildMetrics._buildDocsPerSecond, ivfBuildMetrics._peakHeapBytes, ivfSize,
                  recall10, recall100, percentile(ivfLat, 50), percentile(ivfLat, 95),
                  percentile(ivfLat, 99), queriesPerSecond(ivfLat)));
            }
          }
        } finally {
          FileUtils.deleteQuietly(ivfDir);
        }
      }
    }

    // 4. IVF_PQ benchmark for each (nlist, nprobe, pqM, pqNbits) combination.
    if (!isClassAvailable(IVF_PQ_CREATOR_CLASS) || !isClassAvailable(IVF_PQ_READER_CLASS)) {
      out.println("IVF_PQ benchmark skipped: backend classes are not available in this checkout");
    } else {
      for (int nlist : nlistValues) {
        if (nlist > corpus.length) {
          continue;
        }
        for (int pqM : pqMValues) {
          if (pqM <= 0 || dimension % pqM != 0) {
            continue;
          }
          for (int pqNbits : pqNbitsValues) {
            if (pqNbits != 4 && pqNbits != 6 && pqNbits != 8) {
              continue;
            }
            File pqDir = Files.createTempDirectory("bench_ivfpq_").toFile();
            try {
              out.printf("Building IVF_PQ index (nlist=%d, pqM=%d, pqNbits=%d)...%n",
                  nlist, pqM, pqNbits);
              VectorIndexConfig pqConfig = createIvfPqConfig(dimension, corpus.length, nlist, pqM, pqNbits, distFunc);
              BuildMetrics pqBuildMetrics = buildVectorIndexReflectively(
                  IVF_PQ_CREATOR_CLASS, pqDir, corpus, pqConfig);
              long pqSize = directorySize(pqDir);

              for (int nprobe : nprobeValues) {
                if (nprobe > nlist) {
                  continue;
                }
                try (ReflectiveVectorReader reader = openReflectiveVectorReader(
                    IVF_PQ_READER_CLASS, pqDir, corpus.length, pqConfig)) {
                  reader.setNprobe(nprobe);

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

                  long[] pqLat = measureReflectiveLatencies(reader, queries, 10);
                  Arrays.sort(pqLat);

                  String paramStr = String.format("nlist=%d,nprobe=%d,pqM=%d,pqNbits=%d",
                      nlist, nprobe, pqM, pqNbits);
                  results.add(new BenchmarkResult(IVF_PQ_INDEX_TYPE, paramStr, pqBuildMetrics._buildTimeNs,
                      pqBuildMetrics._buildDocsPerSecond, pqBuildMetrics._peakHeapBytes, pqSize,
                      recall10, recall100, percentile(pqLat, 50), percentile(pqLat, 95),
                      percentile(pqLat, 99), queriesPerSecond(pqLat)));
                }
              }
            } finally {
              FileUtils.deleteQuietly(pqDir);
            }
          }
        }
      }
    }

    return results;
  }

  static void printResultsTable(List<BenchmarkResult> results, PrintStream out) {
    out.printf("%-14s %-22s %10s %12s %12s %10s %10s %10s %10s %10s %10s %10s%n",
        "Index", "Parameters", "Build(ms)", "Build(K/s)", "PeakHeap(MB)", "Size(KB)", "Recall@10", "Recall@100",
        "p50(us)", "p95(us)", "p99(us)", "QPS");
    out.println("-".repeat(158));
    for (BenchmarkResult r : results) {
      out.printf("%-14s %-22s %10.1f %12.1f %12.1f %10.1f %10.4f %10.4f %10.1f %10.1f %10.1f %10.1f%n",
          r._indexType, r._params,
          r._buildTimeNs / 1_000_000.0,
          r._buildDocsPerSecond / 1000.0,
          r._peakHeapBytes / (1024.0 * 1024.0),
          r._indexSizeBytes / 1024.0,
          r._recallAt10, r._recallAt100,
          r._p50LatencyNs / 1000.0,
          r._p95LatencyNs / 1000.0,
          r._p99LatencyNs / 1000.0,
          r._queriesPerSecond);
    }
  }

  /**
   * Prints a small backend-oriented summary so the frontier output stays readable.
   */
  static void printBackendSummary(List<BenchmarkResult> results, PrintStream out) {
    Map<String, List<BenchmarkResult>> byBackend = new LinkedHashMap<>();
    for (BenchmarkResult result : results) {
      byBackend.computeIfAbsent(result._indexType, key -> new ArrayList<>()).add(result);
    }

    out.println();
    out.println("Backend summary:");
    for (String backend : Arrays.asList("Exact Scan", "HNSW", "IVF_FLAT", IVF_PQ_INDEX_TYPE)) {
      List<BenchmarkResult> backendResults = byBackend.get(backend);
      if (backendResults == null || backendResults.isEmpty()) {
        out.printf("  %-10s : skipped or unavailable%n", backend);
        continue;
      }
      BenchmarkResult best = backendResults.get(0);
      for (BenchmarkResult candidate : backendResults) {
        if (candidate._recallAt10 > best._recallAt10
            || (candidate._recallAt10 == best._recallAt10 && candidate._p50LatencyNs < best._p50LatencyNs)) {
          best = candidate;
        }
      }
      out.printf(
          "  %-10s : %s | recall@10=%.4f | p50=%.1fus | qps=%.1f"
              + " | build=%.1fK docs/s | peakHeap=%.1fMB | size=%.1fKB%n",
          backend, best._params, best._recallAt10, best._p50LatencyNs / 1000.0, best._queriesPerSecond,
          best._buildDocsPerSecond / 1000.0, best._peakHeapBytes / (1024.0 * 1024.0),
          best._indexSizeBytes / 1024.0);
    }
  }

  /**
   * Prints the fastest configuration per backend that satisfies a target recall.
   */
  static void printRecallTargetSummary(List<BenchmarkResult> results, double recallTarget, PrintStream out) {
    out.printf("%nTarget recall summary (recall@10 >= %.2f):%n", recallTarget);
    for (String backend : Arrays.asList("Exact Scan", "HNSW", "IVF_FLAT", IVF_PQ_INDEX_TYPE)) {
      BenchmarkResult best = null;
      for (BenchmarkResult result : results) {
        if (!backend.equals(result._indexType) || result._recallAt10 < recallTarget) {
          continue;
        }
        if (best == null
            || result._queriesPerSecond > best._queriesPerSecond
            || (result._queriesPerSecond == best._queriesPerSecond && result._p50LatencyNs < best._p50LatencyNs)) {
          best = result;
        }
      }
      if (best == null) {
        out.printf("  %-10s : no configuration reached the target%n", backend);
      } else {
        out.printf("  %-10s : %s | qps=%.1f | p50=%.1fus | p95=%.1fus%n",
            backend, best._params, best._queriesPerSecond, best._p50LatencyNs / 1000.0,
            best._p95LatencyNs / 1000.0);
      }
    }
  }

  /**
   * Parses a comma-separated list of positive integers from a system property.
   */
  static int[] parseIntListProperty(String propertyName, int[] defaultValues) {
    String raw = System.getProperty(propertyName);
    if (raw == null || raw.trim().isEmpty()) {
      return defaultValues;
    }
    String[] parts = raw.split(",");
    int[] values = new int[parts.length];
    for (int i = 0; i < parts.length; i++) {
      values[i] = Integer.parseInt(parts[i].trim());
    }
    return values;
  }

  // ---------------------------------------------------------------------------
  // Main entry point
  // ---------------------------------------------------------------------------

  /**
   * Runs the complete benchmark suite and prints results to stdout.
   */
  public static void main(String[] args)
      throws Exception {
    String mode = args.length > 0 ? args[0] : System.getProperty(MODE_PROPERTY, DEFAULT_MODE);
    runMode(mode, System.out);
  }

  static void runMode(String mode, PrintStream out)
      throws Exception {
    String normalizedMode = mode == null ? DEFAULT_MODE : mode.trim().toLowerCase();
    switch (normalizedMode) {
      case "frontier":
        runFrontierBenchmark(out);
        return;
      case "sanity":
        BenchmarkVectorSanity.run(out);
        return;
      case "filters":
        BenchmarkVectorFilterWorkloads.run(out);
        return;
      case "features":
        BenchmarkVectorFeatureWorkloads.run(out);
        return;
      case "suite":
        runFrontierBenchmark(out);
        out.println();
        BenchmarkVectorSanity.run(out);
        out.println();
        BenchmarkVectorFilterWorkloads.run(out);
        out.println();
        BenchmarkVectorFeatureWorkloads.run(out);
        return;
      default:
        throw new IllegalArgumentException(
            "Unsupported vector benchmark mode: " + mode
                + ". Expected one of: frontier, sanity, filters, features, suite");
    }
  }

  static List<DatasetScenario> createFrontierScenarios() {
    List<DatasetScenario> scenarios = new ArrayList<>();

    int l2Dimension = Integer.getInteger("pinot.perf.vector.frontier.l2.dimension", 128);
    int l2Size = Integer.getInteger("pinot.perf.vector.frontier.l2.size", 10_000);
    scenarios.add(new DatasetScenario(
        "l2-gaussian-" + l2Dimension + "d-" + l2Size,
        generateGaussianVectors(l2Size, l2Dimension, SEED),
        generateGaussianVectors(NUM_QUERIES, l2Dimension, SEED + 1_000L),
        l2Dimension,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN));

    int cosine768Size = Integer.getInteger("pinot.perf.vector.frontier.cosine768.size", 10_000);
    scenarios.add(new DatasetScenario(
        "cosine-normalized-768d-" + cosine768Size,
        generateNormalizedVectors(cosine768Size, 768, SEED + 2_000L),
        generateNormalizedVectors(NUM_QUERIES, 768, SEED + 3_000L),
        768,
        VectorIndexConfig.VectorDistanceFunction.COSINE));

    int cosine1536Size = Integer.getInteger("pinot.perf.vector.frontier.cosine1536.size", 5_000);
    scenarios.add(new DatasetScenario(
        "cosine-normalized-1536d-" + cosine1536Size,
        generateNormalizedVectors(cosine1536Size, 1536, SEED + 4_000L),
        generateNormalizedVectors(NUM_QUERIES, 1536, SEED + 5_000L),
        1536,
        VectorIndexConfig.VectorDistanceFunction.COSINE));

    int dotSize = Integer.getInteger("pinot.perf.vector.frontier.dot768.size", 10_000);
    scenarios.add(new DatasetScenario(
        "inner-product-skew-768d-" + dotSize,
        generateMagnitudeSkewedVectors(dotSize, 768, SEED + 6_000L),
        generateMagnitudeSkewedVectors(NUM_QUERIES, 768, SEED + 7_000L),
        768,
        VectorIndexConfig.VectorDistanceFunction.INNER_PRODUCT));

    return scenarios;
  }

  static void runFrontierBenchmark(PrintStream out)
      throws Exception {
    out.println("========================================");
    out.println("  Apache Pinot Vector ANN Frontier");
    out.println("========================================");
    out.println("Mode: frontier");
    out.printf("JDK: %s%n", System.getProperty("java.version"));
    out.printf("OS: %s %s%n", System.getProperty("os.name"), System.getProperty("os.arch"));
    out.printf("Seed: %d%n", SEED);
    out.printf("Queries: %d (warmup: %d)%n", NUM_QUERIES, WARMUP_QUERIES);
    int[] nlistValues = parseIntListProperty("pinot.perf.vector.frontier.nlist", new int[]{32, 64, 128});
    int[] nprobeValues = parseIntListProperty("pinot.perf.vector.frontier.nprobe", new int[]{1, 2, 4, 8, 16});
    int[] pqMValues = parseIntListProperty("pinot.perf.vector.frontier.pqM", new int[]{16, 32});
    int[] pqNbitsValues = parseIntListProperty("pinot.perf.vector.frontier.pqNbits", new int[]{8});
    out.printf("IVF knobs: nlist=%s nprobe=%s pqM=%s pqNbits=%s%n",
        Arrays.toString(nlistValues), Arrays.toString(nprobeValues),
        Arrays.toString(pqMValues), Arrays.toString(pqNbitsValues));
    out.println();

    for (DatasetScenario scenario : createFrontierScenarios()) {
      out.println("\n########################################");
      out.printf("# Scenario: %s%n", scenario._name);
      out.println("########################################");
      List<BenchmarkResult> results = runDatasetBenchmark(
          scenario._name, scenario._corpus, scenario._queries, scenario._dimension,
          scenario._distanceFunction, nlistValues, nprobeValues, pqMValues, pqNbitsValues, out);
      out.println();
      printResultsTable(results, out);
      printBackendSummary(results, out);
      printRecallTargetSummary(results, 0.95d, out);
    }

    out.println("\nFrontier benchmark complete.");
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

  @FunctionalInterface
  interface IoRunnable {
    void run()
        throws IOException;
  }

  static BuildMetrics measureBuild(int numDocs, IoRunnable buildRunnable)
      throws IOException {
    long baselineHeapBytes = usedHeapBytes();
    PeakHeapTracker tracker = new PeakHeapTracker(MEMORY_POLL_INTERVAL_MS, baselineHeapBytes);
    Thread monitorThread = new Thread(tracker, "vector-benchmark-heap-tracker");
    monitorThread.setDaemon(true);
    monitorThread.start();

    long start = System.nanoTime();
    IOException ioException = null;
    RuntimeException runtimeException = null;
    Error error = null;
    try {
      buildRunnable.run();
    } catch (IOException e) {
      ioException = e;
    } catch (RuntimeException e) {
      runtimeException = e;
    } catch (Error e) {
      error = e;
    } finally {
      long elapsedNs = System.nanoTime() - start;
      tracker.stop();
      try {
        monitorThread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while waiting for heap tracker to stop", e);
      }
      BuildMetrics buildMetrics = new BuildMetrics(elapsedNs, numDocs, tracker.getPeakHeapDeltaBytes());
      if (ioException == null && runtimeException == null && error == null) {
        return buildMetrics;
      }
      if (ioException != null) {
        throw ioException;
      }
      if (runtimeException != null) {
        throw runtimeException;
      }
      throw error;
    }
  }

  static long usedHeapBytes() {
    Runtime runtime = Runtime.getRuntime();
    return runtime.totalMemory() - runtime.freeMemory();
  }

  static final class PeakHeapTracker implements Runnable {
    private final long _pollIntervalMs;
    private final long _baselineHeapBytes;
    private volatile boolean _running = true;
    private volatile long _peakHeapBytes;

    PeakHeapTracker(long pollIntervalMs, long baselineHeapBytes) {
      _pollIntervalMs = pollIntervalMs;
      _baselineHeapBytes = baselineHeapBytes;
      _peakHeapBytes = baselineHeapBytes;
    }

    @Override
    public void run() {
      while (_running) {
        _peakHeapBytes = Math.max(_peakHeapBytes, usedHeapBytes());
        try {
          Thread.sleep(_pollIntervalMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
      _peakHeapBytes = Math.max(_peakHeapBytes, usedHeapBytes());
    }

    void stop() {
      _running = false;
    }

    long getPeakHeapDeltaBytes() {
      return Math.max(0L, _peakHeapBytes - _baselineHeapBytes);
    }
  }

  static final class BuildMetrics {
    final long _buildTimeNs;
    final double _buildDocsPerSecond;
    final long _peakHeapBytes;

    BuildMetrics(long buildTimeNs, int numDocs, long peakHeapBytes) {
      _buildTimeNs = buildTimeNs;
      _buildDocsPerSecond = buildTimeNs > 0 ? (numDocs * 1_000_000_000.0) / buildTimeNs : 0.0;
      _peakHeapBytes = peakHeapBytes;
    }
  }

  static final class LatencySummary {
    final long _p50Ns;
    final long _p95Ns;
    final long _p99Ns;

    LatencySummary(long p50Ns, long p95Ns, long p99Ns) {
      _p50Ns = p50Ns;
      _p95Ns = p95Ns;
      _p99Ns = p99Ns;
    }
  }
}
