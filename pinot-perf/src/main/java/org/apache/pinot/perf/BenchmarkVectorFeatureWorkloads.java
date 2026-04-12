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
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndexSearcherPool;
import org.apache.pinot.segment.local.realtime.impl.vector.MutableVectorIndex;
import org.apache.pinot.segment.local.segment.index.readers.vector.HnswVectorIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.vector.IvfFlatVectorIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.vector.IvfPqVectorIndexReader;
import org.apache.pinot.segment.local.segment.index.vector.IvfFlatVectorIndexCreator;
import org.apache.pinot.segment.local.segment.index.vector.IvfPqVectorIndexCreator;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorQuantizerType;
import org.apache.pinot.segment.spi.index.reader.EfSearchAware;


/**
 * Feature workload benchmark for Pinot-specific runtime surfaces.
 *
 * <p>This suite focuses on the feature areas that do not show up in broad ANN frontiers:
 * quantized IVF build/search trade-offs, HNSW runtime knobs on immutable and mutable indexes,
 * mutable ingestion cost, and mixed immutable/mutable candidate fan-out.</p>
 */
public final class BenchmarkVectorFeatureWorkloads {
  private static final String COLUMN_NAME = "embedding";
  private static final long SEED = 20260412L;

  private static final int DIMENSION =
      Integer.getInteger(
          "pinot.perf.vector.features.dimension", Integer.getInteger("pinot.perf.vector.dimension", 768));
  private static final int NUM_VECTORS =
      Integer.getInteger("pinot.perf.vector.features.size", 12_000);
  private static final int NUM_QUERIES =
      Integer.getInteger("pinot.perf.vector.features.queries", Integer.getInteger("pinot.perf.vector.queries", 80));
  private static final int WARMUP_QUERIES =
      Integer.getInteger("pinot.perf.vector.features.warmupQueries", 20);
  private static final int TOP_K =
      Integer.getInteger("pinot.perf.vector.features.topK", Integer.getInteger("pinot.perf.vector.topK", 10));
  private static final int NLIST = Integer.getInteger("pinot.perf.vector.features.nlist", 96);
  private static final int NPROBE = Integer.getInteger("pinot.perf.vector.features.nprobe", 8);
  private static final int MUTABLE_PERCENT =
      Integer.getInteger("pinot.perf.vector.features.mutablePercent", 10);
  private static final int PQ_M = Integer.getInteger("pinot.perf.vector.features.pqM", 16);
  private static final int PQ_NBITS = Integer.getInteger("pinot.perf.vector.features.pqNbits", 8);
  private static final int HNSW_EF_SEARCH =
      Integer.getInteger("pinot.perf.vector.features.hnswEfSearch", 64);

  private BenchmarkVectorFeatureWorkloads() {
  }

  public static void main(String[] args)
      throws Exception {
    run(System.out);
  }

  static void run(PrintStream out)
      throws Exception {
    out.println("========================================");
    out.println("  Apache Pinot Vector Feature Workloads");
    out.println("========================================");
    out.printf("JDK: %s%n", System.getProperty("java.version"));
    out.printf("OS: %s %s%n", System.getProperty("os.name"), System.getProperty("os.arch"));
    out.printf("Seed: %d%n", SEED);
    out.printf("Vectors: %d | Dimension: %d | Queries: %d | Warmup: %d%n",
        NUM_VECTORS, DIMENSION, NUM_QUERIES, WARMUP_QUERIES);
    out.printf("topK=%d | nlist=%d | nprobe=%d | mutablePercent=%d | hnswEfSearch=%d%n",
        TOP_K, NLIST, NPROBE, MUTABLE_PERCENT, HNSW_EF_SEARCH);

    RealtimeLuceneTextIndexSearcherPool.init(1);

    benchmarkQuantizedIvf(out);
    benchmarkMutableIngestion(out);
    benchmarkHnswRuntimeControls(out);
    benchmarkMixedMutableImmutable(out);
  }

  private static void benchmarkQuantizedIvf(PrintStream out)
      throws Exception {
    out.println();
    out.println("=== Quantized IVF ===");
    out.printf("%-10s %-10s %10s %10s %10s %10s %10s %10s%n",
        "Backend", "Variant", "Build(ms)", "Size(KB)", "Recall@10", "p50(us)", "p95(us)", "QPS");

    float[][] corpus = BenchmarkVectorIndex.generateGaussianVectors(NUM_VECTORS, DIMENSION, SEED);
    float[][] queries = BenchmarkVectorIndex.generateGaussianVectors(NUM_QUERIES, DIMENSION, SEED + 1_000L);
    int[][] truth = BenchmarkVectorIndex.computeGroundTruth(
        corpus, queries, TOP_K, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);

    List<QuantizerBenchmarkResult> results = new ArrayList<>();
    for (VectorQuantizerType quantizerType
        : Arrays.asList(VectorQuantizerType.FLAT, VectorQuantizerType.SQ8, VectorQuantizerType.SQ4)) {
      File indexDir = Files.createTempDirectory("bench_features_ivf_").toFile();
      try {
        VectorIndexConfig config =
            createIvfFlatConfig("IVF_FLAT", DIMENSION, NLIST, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN,
                quantizerType);
        BenchmarkVectorIndex.BuildMetrics buildMetrics =
            BenchmarkVectorIndex.measureBuild(corpus.length, () -> buildIvfFlatIndex(indexDir, corpus, config));
        long sizeBytes = BenchmarkVectorIndex.ivfIndexSize(indexDir);
        try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, indexDir, config)) {
          reader.setNprobe(NPROBE);
          long[] latencies = measureLatencies(queries, query -> reader.getDocIds(query, TOP_K));
          double recall10 = measureRecall(truth, queries, query -> BenchmarkVectorIndex.bitmapToSet(reader.getDocIds(
              query, TOP_K)));
          results.add(new QuantizerBenchmarkResult("IVF_FLAT", quantizerType.name(), buildMetrics, sizeBytes,
              recall10, summarizeLatencies(latencies), BenchmarkVectorIndex.queriesPerSecond(latencies)));
        }
      } finally {
        FileUtils.deleteQuietly(indexDir);
      }
    }

    if (PQ_M > 0 && DIMENSION % PQ_M == 0) {
      File indexDir = Files.createTempDirectory("bench_features_ivfpq_").toFile();
      try {
        VectorIndexConfig config = BenchmarkVectorIndex.createIvfPqConfig(DIMENSION, corpus.length, NLIST, PQ_M,
            PQ_NBITS, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
        BenchmarkVectorIndex.BuildMetrics buildMetrics =
            BenchmarkVectorIndex.measureBuild(corpus.length, () -> buildIvfPqIndex(indexDir, corpus, config));
        long sizeBytes = FileUtils.sizeOfDirectory(indexDir);
        try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader(COLUMN_NAME, indexDir, config)) {
          reader.setNprobe(NPROBE);
          long[] latencies = measureLatencies(queries, query -> reader.getDocIds(query, TOP_K));
          double recall10 = measureRecall(truth, queries, query -> BenchmarkVectorIndex.bitmapToSet(reader.getDocIds(
              query, TOP_K)));
          results.add(new QuantizerBenchmarkResult("IVF_PQ", "PQ", buildMetrics, sizeBytes, recall10,
              summarizeLatencies(latencies), BenchmarkVectorIndex.queriesPerSecond(latencies)));
        }
      } finally {
        FileUtils.deleteQuietly(indexDir);
      }
    }

    for (QuantizerBenchmarkResult result : results) {
      out.printf("%-10s %-10s %10.1f %10.1f %10.4f %10.1f %10.1f %10.1f%n",
          result._backend, result._variant,
          result._buildMetrics._buildTimeNs / 1_000_000.0,
          result._sizeBytes / 1024.0,
          result._recallAt10,
          result._latencies._p50Ns / 1000.0,
          result._latencies._p95Ns / 1000.0,
          result._queriesPerSecond);
    }
  }

  private static void benchmarkMutableIngestion(PrintStream out)
      throws Exception {
    out.println();
    out.println("=== Mutable HNSW Ingestion ===");
    out.printf("%-18s %14s %14s %14s%n", "Workload", "build(ms)", "docs/sec", "peakHeap(MB)");

    float[][] corpus = BenchmarkVectorIndex.generateGaussianVectors(NUM_VECTORS, DIMENSION, SEED + 5_000L);
    BenchmarkVectorIndex.BuildMetrics buildMetrics = BenchmarkVectorIndex.measureBuild(corpus.length, () -> {
      try (MutableVectorIndex index = new MutableVectorIndex("mutableIngestion_" + System.nanoTime(), COLUMN_NAME,
          createMutableHnswConfig(DIMENSION, corpus.length, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN))) {
        for (int i = 0; i < corpus.length; i++) {
          index.add(box(corpus[i]), null, i);
        }
      }
    });

    out.printf("%-18s %14.1f %14.1f %14.1f%n", "mutable_hnsw_add",
        buildMetrics._buildTimeNs / 1_000_000.0,
        buildMetrics._buildDocsPerSecond,
        buildMetrics._peakHeapBytes / (1024.0 * 1024.0));
  }

  private static void benchmarkHnswRuntimeControls(PrintStream out)
      throws Exception {
    out.println();
    out.println("=== HNSW Runtime Controls ===");
    out.printf("%-10s %-34s %10s %10s %10s %10s%n",
        "Surface", "Controls", "Recall@10", "p50(us)", "p95(us)", "QPS");

    float[][] corpus = BenchmarkVectorIndex.generateGaussianVectors(NUM_VECTORS, DIMENSION, SEED + 10_000L);
    float[][] queries = BenchmarkVectorIndex.generateGaussianVectors(NUM_QUERIES, DIMENSION, SEED + 11_000L);
    int[][] truth = BenchmarkVectorIndex.computeGroundTruth(
        corpus, queries, TOP_K, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    List<HnswControl> controls = Arrays.asList(
        HnswControl.defaultControls(),
        new HnswControl("ef=" + HNSW_EF_SEARCH, HNSW_EF_SEARCH, true, true),
        new HnswControl("ef=" + HNSW_EF_SEARCH + ",unbounded,noRel", HNSW_EF_SEARCH, false, false));

    File immutableDir = Files.createTempDirectory("bench_features_hnsw_immutable_").toFile();
    try {
      try {
        BenchmarkVectorIndex.buildHnswIndex(
            immutableDir, corpus, DIMENSION, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
        try (HnswVectorIndexReader reader = BenchmarkVectorIndex.openHnswReader(
            immutableDir, corpus.length, DIMENSION, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN)) {
          for (HnswControl control : controls) {
            ControlBenchmarkResult result = benchmarkImmutableHnswControl(reader, queries, truth, control);
            out.printf("%-10s %-34s %10.4f %10.1f %10.1f %10.1f%n",
                "immutable", result._controlLabel, result._recallAt10,
                result._latencies._p50Ns / 1000.0, result._latencies._p95Ns / 1000.0,
                result._queriesPerSecond);
          }
        }
      } catch (Throwable t) {
        printHnswSkip(out, "immutable runtime controls", t);
      }
    } finally {
      FileUtils.deleteQuietly(immutableDir);
    }

    try (MutableVectorIndex mutableIndex = buildMutableIndex(
        corpus, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, "mutableControls")) {
      for (HnswControl control : controls) {
        ControlBenchmarkResult result = benchmarkMutableHnswControl(mutableIndex, queries, truth, control);
        out.printf("%-10s %-34s %10.4f %10.1f %10.1f %10.1f%n",
            "mutable", result._controlLabel, result._recallAt10,
            result._latencies._p50Ns / 1000.0, result._latencies._p95Ns / 1000.0,
            result._queriesPerSecond);
      }
    }
  }

  private static void benchmarkMixedMutableImmutable(PrintStream out)
      throws Exception {
    out.println();
    out.println("=== Mixed Mutable/Immutable HNSW ===");
    out.printf("%-34s %10s %10s %10s %10s%n", "Controls", "Recall@10", "p50(us)", "p95(us)", "QPS");

    float[][] corpus = BenchmarkVectorIndex.generateGaussianVectors(NUM_VECTORS, DIMENSION, SEED + 20_000L);
    float[][] queries = BenchmarkVectorIndex.generateGaussianVectors(NUM_QUERIES, DIMENSION, SEED + 21_000L);
    int[][] truth = BenchmarkVectorIndex.computeGroundTruth(
        corpus, queries, TOP_K, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);

    int mutableCount = Math.max(1, corpus.length * MUTABLE_PERCENT / 100);
    int immutableCount = corpus.length - mutableCount;
    float[][] immutableCorpus = Arrays.copyOfRange(corpus, 0, immutableCount);
    float[][] mutableCorpus = Arrays.copyOfRange(corpus, immutableCount, corpus.length);

    List<HnswControl> controls = Arrays.asList(
        HnswControl.defaultControls(),
        new HnswControl("ef=" + HNSW_EF_SEARCH, HNSW_EF_SEARCH, true, true),
        new HnswControl("ef=" + HNSW_EF_SEARCH + ",unbounded,noRel", HNSW_EF_SEARCH, false, false));

    File immutableDir = Files.createTempDirectory("bench_features_hnsw_mixed_").toFile();
    try {
      try {
        BenchmarkVectorIndex.buildHnswIndex(
            immutableDir, immutableCorpus, DIMENSION, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
        try (HnswVectorIndexReader immutableReader = BenchmarkVectorIndex.openHnswReader(
            immutableDir, immutableCorpus.length, DIMENSION, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
            MutableVectorIndex mutableIndex = buildMutableIndex(
                mutableCorpus, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, "mutableMixed")) {
          for (HnswControl control : controls) {
            applyControl(immutableReader, control);
            applyControl(mutableIndex, control);
            try {
              long[] latencies = measureLatencies(queries,
                  query -> searchMixed(immutableReader, immutableCorpus, mutableIndex, mutableCorpus, query));
              double recall10 = measureRecall(truth, queries,
                  query -> searchMixed(immutableReader, immutableCorpus, mutableIndex, mutableCorpus, query));
              LatencySummary summary = summarizeLatencies(latencies);
              out.printf("%-34s %10.4f %10.1f %10.1f %10.1f%n",
                  control._label, recall10, summary._p50Ns / 1000.0, summary._p95Ns / 1000.0,
                  BenchmarkVectorIndex.queriesPerSecond(latencies));
            } finally {
              clearControl(immutableReader);
              clearControl(mutableIndex);
            }
          }
        }
      } catch (Throwable t) {
        printHnswSkip(out, "mixed mutable/immutable HNSW", t);
      }
    } finally {
      FileUtils.deleteQuietly(immutableDir);
    }
  }

  private static void printHnswSkip(PrintStream out, String workload, Throwable t) {
    out.printf("  %s skipped: %s: %s%n", workload, t.getClass().getSimpleName(), t.getMessage());
    out.println("  HNSW runtime benchmarks require MAVEN_OPTS='--add-opens=java.base/java.net=ALL-UNNAMED "
        + "--enable-native-access=ALL-UNNAMED --add-modules=jdk.incubator.vector' on JDK 21+.");
  }

  private static ControlBenchmarkResult benchmarkImmutableHnswControl(HnswVectorIndexReader reader, float[][] queries,
      int[][] truth, HnswControl control)
      throws Exception {
    applyControl(reader, control);
    try {
      long[] latencies = measureLatencies(queries, query -> reader.getDocIds(query, TOP_K));
      double recall10 = measureRecall(truth, queries, query -> toSet(reader.getDocIds(query, TOP_K).toArray()));
      return new ControlBenchmarkResult(control._label, recall10, summarizeLatencies(latencies),
          BenchmarkVectorIndex.queriesPerSecond(latencies));
    } finally {
      clearControl(reader);
    }
  }

  private static ControlBenchmarkResult benchmarkMutableHnswControl(MutableVectorIndex index, float[][] queries,
      int[][] truth, HnswControl control)
      throws Exception {
    applyControl(index, control);
    try {
      long[] latencies = measureLatencies(queries, query -> index.getDocIds(query, TOP_K));
      double recall10 = measureRecall(truth, queries, query -> toSet(index.getDocIds(query, TOP_K).toArray()));
      return new ControlBenchmarkResult(control._label, recall10, summarizeLatencies(latencies),
          BenchmarkVectorIndex.queriesPerSecond(latencies));
    } finally {
      clearControl(index);
    }
  }

  private static MutableVectorIndex buildMutableIndex(float[][] corpus,
      VectorIndexConfig.VectorDistanceFunction distanceFunction, String segmentPrefix) {
    MutableVectorIndex index =
        new MutableVectorIndex(segmentPrefix + "_" + System.nanoTime(), COLUMN_NAME,
            createMutableHnswConfig(DIMENSION, corpus.length, distanceFunction));
    for (int i = 0; i < corpus.length; i++) {
      index.add(box(corpus[i]), null, i);
    }
    return index;
  }

  private static void buildIvfFlatIndex(File indexDir, float[][] corpus, VectorIndexConfig config)
      throws IOException {
    try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN_NAME, indexDir, config)) {
      for (float[] vector : corpus) {
        creator.add(vector);
      }
      creator.seal();
    }
  }

  private static void buildIvfPqIndex(File indexDir, float[][] corpus, VectorIndexConfig config)
      throws IOException {
    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(COLUMN_NAME, indexDir, config)) {
      for (float[] vector : corpus) {
        creator.add(vector);
      }
      creator.seal();
    }
  }

  private static VectorIndexConfig createIvfFlatConfig(String backendType, int dimension, int nlist,
      VectorIndexConfig.VectorDistanceFunction distanceFunction, VectorQuantizerType quantizerType) {
    java.util.Map<String, String> properties = new java.util.HashMap<>();
    properties.put("vectorIndexType", backendType);
    properties.put("vectorDimension", String.valueOf(dimension));
    properties.put("vectorDistanceFunction", distanceFunction.name());
    properties.put("nlist", String.valueOf(nlist));
    properties.put("nprobe", String.valueOf(NPROBE));
    properties.put("trainingSeed", String.valueOf(SEED));
    properties.put("quantizer", quantizerType.name());
    return new VectorIndexConfig(false, backendType, dimension, 1, distanceFunction, properties);
  }

  private static VectorIndexConfig createMutableHnswConfig(int dimension, int numDocs,
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    java.util.Map<String, String> properties = new java.util.HashMap<>();
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", String.valueOf(dimension));
    properties.put("vectorDistanceFunction", distanceFunction.name());
    properties.put("commitDocs", String.valueOf(Math.max(1, numDocs)));
    properties.put("commitIntervalMs", String.valueOf(Long.MAX_VALUE));
    return new VectorIndexConfig(false, "HNSW", dimension, 1, distanceFunction, properties);
  }

  private static long[] measureLatencies(float[][] queries, SearchAction action)
      throws Exception {
    int warmup = Math.min(WARMUP_QUERIES, queries.length);
    for (int i = 0; i < warmup; i++) {
      action.search(queries[i]);
    }
    long[] latencies = new long[queries.length];
    for (int i = 0; i < queries.length; i++) {
      long start = System.nanoTime();
      action.search(queries[i]);
      latencies[i] = System.nanoTime() - start;
    }
    return latencies;
  }

  private static double measureRecall(int[][] truth, float[][] queries, SearchResultProvider resultProvider)
      throws Exception {
    double recall = 0.0d;
    for (int i = 0; i < queries.length; i++) {
      recall += BenchmarkVectorIndex.computeRecall(truth[i], resultProvider.search(queries[i]));
    }
    return recall / queries.length;
  }

  private static LatencySummary summarizeLatencies(long[] latencies) {
    long[] copy = Arrays.copyOf(latencies, latencies.length);
    Arrays.sort(copy);
    return new LatencySummary(
        BenchmarkVectorIndex.percentile(copy, 50),
        BenchmarkVectorIndex.percentile(copy, 95),
        BenchmarkVectorIndex.percentile(copy, 99));
  }

  private static void applyControl(EfSearchAware reader, HnswControl control) {
    if (control._efSearch != null) {
      reader.setEfSearch(control._efSearch);
    }
    reader.setUseRelativeDistance(control._useRelativeDistance);
    reader.setUseBoundedQueue(control._useBoundedQueue);
  }

  private static void clearControl(EfSearchAware reader) {
    reader.clearEfSearch();
    reader.clearUseRelativeDistance();
    reader.clearUseBoundedQueue();
  }

  private static Set<Integer> searchMixed(HnswVectorIndexReader immutableReader, float[][] immutableCorpus,
      MutableVectorIndex mutableIndex, float[][] mutableCorpus, float[] query) {
    PriorityQueue<ScoredDoc> heap = new PriorityQueue<>(TOP_K,
        (left, right) -> Float.compare(right._distance, left._distance));
    int[] immutableMatches = immutableReader.getDocIds(query, TOP_K).toArray();
    for (int docId : immutableMatches) {
      float distance = BenchmarkVectorIndex.computeDistance(
          query, immutableCorpus[docId], VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
      offer(heap, docId, distance, TOP_K);
    }
    int[] mutableMatches = mutableIndex.getDocIds(query, TOP_K).toArray();
    for (int docId : mutableMatches) {
      float distance = BenchmarkVectorIndex.computeDistance(
          query, mutableCorpus[docId], VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
      offer(heap, immutableCorpus.length + docId, distance, TOP_K);
    }
    Set<Integer> result = new HashSet<>();
    for (ScoredDoc scoredDoc : heap) {
      result.add(scoredDoc._docId);
    }
    return result;
  }

  private static void offer(PriorityQueue<ScoredDoc> heap, int docId, float distance, int maxSize) {
    if (heap.size() < maxSize) {
      heap.offer(new ScoredDoc(docId, distance));
    } else if (distance < heap.peek()._distance) {
      heap.poll();
      heap.offer(new ScoredDoc(docId, distance));
    }
  }

  private static Set<Integer> toSet(int[] docIds) {
    Set<Integer> result = new HashSet<>(docIds.length);
    for (int docId : docIds) {
      result.add(docId);
    }
    return result;
  }

  private static Object[] box(float[] vector) {
    Object[] boxed = new Object[vector.length];
    for (int i = 0; i < vector.length; i++) {
      boxed[i] = vector[i];
    }
    return boxed;
  }

  @FunctionalInterface
  private interface SearchAction {
    void search(float[] query)
        throws Exception;
  }

  @FunctionalInterface
  private interface SearchResultProvider {
    Set<Integer> search(float[] query)
        throws Exception;
  }

  private static final class QuantizerBenchmarkResult {
    private final String _backend;
    private final String _variant;
    private final BenchmarkVectorIndex.BuildMetrics _buildMetrics;
    private final long _sizeBytes;
    private final double _recallAt10;
    private final LatencySummary _latencies;
    private final double _queriesPerSecond;

    private QuantizerBenchmarkResult(String backend, String variant, BenchmarkVectorIndex.BuildMetrics buildMetrics,
        long sizeBytes, double recallAt10, LatencySummary latencies, double queriesPerSecond) {
      _backend = backend;
      _variant = variant;
      _buildMetrics = buildMetrics;
      _sizeBytes = sizeBytes;
      _recallAt10 = recallAt10;
      _latencies = latencies;
      _queriesPerSecond = queriesPerSecond;
    }
  }

  private static final class HnswControl {
    private final String _label;
    private final Integer _efSearch;
    private final boolean _useRelativeDistance;
    private final boolean _useBoundedQueue;

    private HnswControl(String label, Integer efSearch, boolean useRelativeDistance, boolean useBoundedQueue) {
      _label = label;
      _efSearch = efSearch;
      _useRelativeDistance = useRelativeDistance;
      _useBoundedQueue = useBoundedQueue;
    }

    private static HnswControl defaultControls() {
      return new HnswControl("default", null, true, true);
    }
  }

  private static final class ControlBenchmarkResult {
    private final String _controlLabel;
    private final double _recallAt10;
    private final LatencySummary _latencies;
    private final double _queriesPerSecond;

    private ControlBenchmarkResult(String controlLabel, double recallAt10, LatencySummary latencies,
        double queriesPerSecond) {
      _controlLabel = controlLabel;
      _recallAt10 = recallAt10;
      _latencies = latencies;
      _queriesPerSecond = queriesPerSecond;
    }
  }

  private static final class LatencySummary {
    private final long _p50Ns;
    private final long _p95Ns;
    private final long _p99Ns;

    private LatencySummary(long p50Ns, long p95Ns, long p99Ns) {
      _p50Ns = p50Ns;
      _p95Ns = p95Ns;
      _p99Ns = p99Ns;
    }
  }

  private static final class ScoredDoc {
    private final int _docId;
    private final float _distance;

    private ScoredDoc(int docId, float distance) {
      _docId = docId;
      _distance = distance;
    }
  }
}
