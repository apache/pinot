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
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndexSearcherPool;
import org.apache.pinot.segment.local.realtime.impl.vector.MutableVectorIndex;
import org.apache.pinot.segment.local.segment.index.readers.vector.HnswVectorIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.vector.IvfFlatVectorIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.vector.IvfOnDiskVectorIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.vector.IvfPqVectorIndexReader;
import org.apache.pinot.segment.local.segment.index.vector.IvfFlatVectorIndexCreator;
import org.apache.pinot.segment.local.segment.index.vector.IvfPqVectorIndexCreator;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorQuantizerType;
import org.apache.pinot.segment.spi.index.reader.EfSearchAware;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Compatibility entry point for the vector benchmark suite's {@code closeout} mode.
 *
 * <p>This benchmark intentionally targets the concrete code paths landed in the final phase:
 * quantized IVF search, HNSW runtime controls, IVF_ON_DISK filter-aware ANN, approximate radius
 * candidate generation, and mixed immutable/mutable HNSW query fan-out.</p>
 *
 * <p>Prefer {@link BenchmarkVectorIndex} with
 * {@code -Dpinot.perf.vector.mode=closeout}. This class remains so older scripts continue to work.</p>
 */
public final class BenchmarkVectorCloseoutFeatures {
  private static final String COLUMN_NAME = "embedding";
  private static final long SEED = 20260410L;

  private static final int DIMENSION = Integer.getInteger("pinot.perf.vector.closeout.dimension", 128);
  private static final int NUM_VECTORS = Integer.getInteger("pinot.perf.vector.closeout.size", 12_000);
  private static final int NUM_QUERIES = Integer.getInteger("pinot.perf.vector.closeout.queries", 80);
  private static final int WARMUP_QUERIES = Integer.getInteger("pinot.perf.vector.closeout.warmupQueries", 20);
  private static final int TOP_K = Integer.getInteger("pinot.perf.vector.closeout.topK", 10);
  private static final int NLIST = Integer.getInteger("pinot.perf.vector.closeout.nlist", 96);
  private static final int NPROBE = Integer.getInteger("pinot.perf.vector.closeout.nprobe", 8);
  private static final int RADIUS_MAX_CANDIDATES =
      Integer.getInteger("pinot.perf.vector.closeout.radiusMaxCandidates", 512);
  private static final int RADIUS_TARGET_MATCHES =
      Integer.getInteger("pinot.perf.vector.closeout.radiusTargetMatches", 24);
  private static final int MUTABLE_PERCENT = Integer.getInteger("pinot.perf.vector.closeout.mutablePercent", 10);
  private static final int PQ_M = Integer.getInteger("pinot.perf.vector.closeout.pqM", 16);
  private static final int PQ_NBITS = Integer.getInteger("pinot.perf.vector.closeout.pqNbits", 8);
  private static final int HNSW_EF_SEARCH =
      Integer.getInteger("pinot.perf.vector.closeout.hnswEfSearch", 64);

  private BenchmarkVectorCloseoutFeatures() {
  }

  public static void main(String[] args)
      throws Exception {
    run(System.out);
  }

  static void run(PrintStream out)
      throws Exception {
    out.println("========================================");
    out.println("  Apache Pinot Vector Close-out Benchmark");
    out.println("========================================");
    out.printf("JDK: %s%n", System.getProperty("java.version"));
    out.printf("OS: %s %s%n", System.getProperty("os.name"), System.getProperty("os.arch"));
    out.printf("Seed: %d%n", SEED);
    out.printf("Vectors: %d | Dimension: %d | Queries: %d | Warmup: %d%n",
        NUM_VECTORS, DIMENSION, NUM_QUERIES, WARMUP_QUERIES);
    out.printf("topK=%d | nlist=%d | nprobe=%d | radiusMaxCandidates=%d | mutablePercent=%d%n",
        TOP_K, NLIST, NPROBE, RADIUS_MAX_CANDIDATES, MUTABLE_PERCENT);
    out.println();

    RealtimeLuceneTextIndexSearcherPool.init(1);

    benchmarkQuantizedIvfSearch(out);
    benchmarkHnswRuntimeControls(out);
    benchmarkIvfOnDiskFilterAware(out);
    benchmarkApproximateRadius(out);
    benchmarkMixedMutableImmutable(out);
  }

  private static void benchmarkQuantizedIvfSearch(PrintStream out)
      throws Exception {
    out.println();
    out.println("=== Quantized IVF Search ===");
    out.printf("%-10s %-10s %10s %10s %10s %10s %10s%n",
        "Backend", "Quantizer", "Build(ms)", "Size(KB)", "Recall@10", "p50(us)", "p95(us)");

    float[][] corpus = BenchmarkVectorIndex.generateGaussianVectors(NUM_VECTORS, DIMENSION, SEED);
    float[][] queries = BenchmarkVectorIndex.generateGaussianVectors(NUM_QUERIES, DIMENSION, SEED + 1_000L);
    int[][] truth = BenchmarkVectorIndex.computeGroundTruth(
        corpus, queries, TOP_K, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);

    List<QuantizerBenchmarkResult> results = new ArrayList<>();
    for (VectorQuantizerType quantizerType
        : Arrays.asList(VectorQuantizerType.FLAT, VectorQuantizerType.SQ8, VectorQuantizerType.SQ4)) {
      File indexDir = Files.createTempDirectory("bench_closeout_ivf_").toFile();
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
          double recall10 = measureRecall(
              truth, queries, query -> toSet(reader.getDocIds(query, TOP_K)));
          results.add(new QuantizerBenchmarkResult("IVF_FLAT", quantizerType.name(), buildMetrics, sizeBytes,
              recall10, summarizeLatencies(latencies)));
        }
      } finally {
        FileUtils.deleteQuietly(indexDir);
      }
    }

    if (PQ_M > 0 && DIMENSION % PQ_M == 0) {
      File indexDir = Files.createTempDirectory("bench_closeout_ivfpq_").toFile();
      try {
        VectorIndexConfig config = BenchmarkVectorIndex.createIvfPqConfig(DIMENSION, corpus.length, NLIST, PQ_M,
            PQ_NBITS, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
        BenchmarkVectorIndex.BuildMetrics buildMetrics =
            BenchmarkVectorIndex.measureBuild(corpus.length, () -> buildIvfPqIndex(indexDir, corpus, config));
        long sizeBytes = FileUtils.sizeOfDirectory(indexDir);
        try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader(COLUMN_NAME, indexDir, config)) {
          reader.setNprobe(NPROBE);
          long[] latencies = measureLatencies(queries, query -> reader.getDocIds(query, TOP_K));
          double recall10 = measureRecall(
              truth, queries, query -> BenchmarkVectorIndex.bitmapToSet(reader.getDocIds(query, TOP_K)));
          results.add(new QuantizerBenchmarkResult("IVF_PQ", "PQ", buildMetrics, sizeBytes, recall10,
              summarizeLatencies(latencies)));
        }
      } finally {
        FileUtils.deleteQuietly(indexDir);
      }
    }

    for (QuantizerBenchmarkResult result : results) {
      out.printf("%-10s %-10s %10.1f %10.1f %10.4f %10.1f %10.1f%n",
          result._backend, result._quantizer,
          result._buildMetrics._buildTimeNs / 1_000_000.0,
          result._sizeBytes / 1024.0,
          result._recallAt10,
          result._latencies._p50Ns / 1000.0,
          result._latencies._p95Ns / 1000.0);
    }
  }

  private static void benchmarkHnswRuntimeControls(PrintStream out)
      throws Exception {
    out.println();
    out.println("=== HNSW Runtime Controls ===");
    out.printf("%-10s %-34s %10s %10s %10s%n",
        "Surface", "Controls", "Recall@10", "p50(us)", "p95(us)");

    float[][] corpus = BenchmarkVectorIndex.generateGaussianVectors(NUM_VECTORS, DIMENSION, SEED + 10_000L);
    float[][] queries = BenchmarkVectorIndex.generateGaussianVectors(NUM_QUERIES, DIMENSION, SEED + 11_000L);
    int[][] truth = BenchmarkVectorIndex.computeGroundTruth(
        corpus, queries, TOP_K, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    List<HnswControl> controls = Arrays.asList(
        HnswControl.defaultControls(),
        new HnswControl("ef=" + HNSW_EF_SEARCH, HNSW_EF_SEARCH, true, true),
        new HnswControl("ef=" + HNSW_EF_SEARCH + ",unbounded,noRel", HNSW_EF_SEARCH, false, false));

    File immutableDir = Files.createTempDirectory("bench_closeout_hnsw_immutable_").toFile();
    try {
      BenchmarkVectorIndex.buildHnswIndex(
          immutableDir, corpus, DIMENSION, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
      try (HnswVectorIndexReader reader = BenchmarkVectorIndex.openHnswReader(
          immutableDir, corpus.length, DIMENSION, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN)) {
        for (HnswControl control : controls) {
          ControlBenchmarkResult result =
              benchmarkImmutableHnswControl(reader, corpus, queries, truth, control);
          out.printf("%-10s %-34s %10.4f %10.1f %10.1f%n",
              "immutable", result._controlLabel, result._recallAt10,
              result._latencies._p50Ns / 1000.0, result._latencies._p95Ns / 1000.0);
        }
      }
    } finally {
      FileUtils.deleteQuietly(immutableDir);
    }

    try (MutableVectorIndex mutableIndex = buildMutableIndex(
        corpus, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, "closeoutMutableControls")) {
      for (HnswControl control : controls) {
        ControlBenchmarkResult result =
            benchmarkMutableHnswControl(mutableIndex, corpus, queries, truth, control);
        out.printf("%-10s %-34s %10.4f %10.1f %10.1f%n",
            "mutable", result._controlLabel, result._recallAt10,
            result._latencies._p50Ns / 1000.0, result._latencies._p95Ns / 1000.0);
      }
    }
  }

  private static void benchmarkIvfOnDiskFilterAware(PrintStream out)
      throws Exception {
    out.println();
    out.println("=== IVF_ON_DISK Filter-aware ANN ===");
    out.printf("%-12s %10s %10s %10s %10s %10s%n",
        "Selectivity", "Recall@10", "p50(us)", "p95(us)", "exactP50", "exactP95");

    float[][] corpus = BenchmarkVectorIndex.generateGaussianVectors(NUM_VECTORS, DIMENSION, SEED + 20_000L);
    float[][] queries = BenchmarkVectorIndex.generateGaussianVectors(NUM_QUERIES, DIMENSION, SEED + 21_000L);
    VectorIndexConfig.VectorDistanceFunction distanceFunction = VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN;
    File indexDir = Files.createTempDirectory("bench_closeout_ivf_ondisk_").toFile();
    try {
      VectorIndexConfig creatorConfig =
          createIvfFlatConfig("IVF_FLAT", DIMENSION, NLIST, distanceFunction, VectorQuantizerType.SQ8);
      buildIvfFlatIndex(indexDir, corpus, creatorConfig);
      VectorIndexConfig readerConfig =
          createIvfFlatConfig("IVF_ON_DISK", DIMENSION, NLIST, distanceFunction, VectorQuantizerType.SQ8);
      try (IvfOnDiskVectorIndexReader reader = new IvfOnDiskVectorIndexReader(COLUMN_NAME, indexDir, readerConfig)) {
        reader.setNprobe(NPROBE);
        for (double selectivity : Arrays.asList(1.0d, 0.1d, 0.01d)) {
          MutableRoaringBitmap filterBitmap = createFilterBitmap(corpus.length, selectivity, SEED + (long) (selectivity
              * 1_000_000));
          int[] allowedDocIds = filterBitmap.toArray();
          long[] filteredLatencies = measureLatencies(
              queries, query -> reader.getDocIds(query, TOP_K, filterBitmap));
          long[] exactLatencies = measureLatencies(
              queries, query -> exactFilteredTopK(corpus, query, allowedDocIds, TOP_K, distanceFunction));
          double recall10 = measureRecallAgainstFilteredTruth(
              corpus, queries, allowedDocIds, TOP_K, distanceFunction,
              query -> BenchmarkVectorIndex.bitmapToSet(reader.getDocIds(query, TOP_K, filterBitmap)));

          LatencySummary filteredSummary = summarizeLatencies(filteredLatencies);
          LatencySummary exactSummary = summarizeLatencies(exactLatencies);
          out.printf("%-12s %10.4f %10.1f %10.1f %10.1f %10.1f%n",
              formatPercent(selectivity), recall10,
              filteredSummary._p50Ns / 1000.0, filteredSummary._p95Ns / 1000.0,
              exactSummary._p50Ns / 1000.0, exactSummary._p95Ns / 1000.0);
        }
      }
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  private static void benchmarkApproximateRadius(PrintStream out)
      throws Exception {
    out.println();
    out.println("=== Approximate Radius ===");
    out.printf("%-10s %-10s %-14s %10s %10s %10s %10s%n",
        "Backend", "Distance", "Mode", "Recall", "avgHits", "p50(us)", "p95(us)");

    runRadiusScenario(out, "IVF_FLAT", VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN,
        BenchmarkVectorIndex.generateGaussianVectors(NUM_VECTORS, DIMENSION, SEED + 30_000L),
        BenchmarkVectorIndex.generateGaussianVectors(NUM_QUERIES, DIMENSION, SEED + 31_000L));
    runRadiusScenario(out, "IVF_ON_DISK", VectorIndexConfig.VectorDistanceFunction.COSINE,
        BenchmarkVectorIndex.generateNormalizedVectors(NUM_VECTORS, DIMENSION, SEED + 32_000L),
        BenchmarkVectorIndex.generateNormalizedVectors(NUM_QUERIES, DIMENSION, SEED + 33_000L));
  }

  private static void benchmarkMixedMutableImmutable(PrintStream out)
      throws Exception {
    out.println();
    out.println("=== Mixed Mutable/Immutable HNSW ===");
    out.printf("%-34s %10s %10s %10s%n", "Controls", "Recall@10", "p50(us)", "p95(us)");

    float[][] corpus = BenchmarkVectorIndex.generateGaussianVectors(NUM_VECTORS, DIMENSION, SEED + 40_000L);
    float[][] queries = BenchmarkVectorIndex.generateGaussianVectors(NUM_QUERIES, DIMENSION, SEED + 41_000L);
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

    File immutableDir = Files.createTempDirectory("bench_closeout_hnsw_mixed_").toFile();
    try {
      BenchmarkVectorIndex.buildHnswIndex(
          immutableDir, immutableCorpus, DIMENSION, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
      try (HnswVectorIndexReader immutableReader = BenchmarkVectorIndex.openHnswReader(
          immutableDir, immutableCorpus.length, DIMENSION, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
          MutableVectorIndex mutableIndex = buildMutableIndex(
              mutableCorpus, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, "closeoutMutableMixed")) {
        for (HnswControl control : controls) {
          applyControl(immutableReader, control);
          applyControl(mutableIndex, control);
          try {
            long[] latencies = measureLatencies(
                queries, query -> searchMixed(immutableReader, immutableCorpus, mutableIndex, mutableCorpus, query));
            double recall10 = measureRecall(
                truth, queries,
                query -> searchMixed(immutableReader, immutableCorpus, mutableIndex, mutableCorpus, query));
            LatencySummary summary = summarizeLatencies(latencies);
            out.printf("%-34s %10.4f %10.1f %10.1f%n",
                control._label, recall10, summary._p50Ns / 1000.0, summary._p95Ns / 1000.0);
          } finally {
            clearControl(immutableReader);
            clearControl(mutableIndex);
          }
        }
      }
    } finally {
      FileUtils.deleteQuietly(immutableDir);
    }
  }

  private static void runRadiusScenario(PrintStream out, String backend,
      VectorIndexConfig.VectorDistanceFunction distanceFunction, float[][] corpus, float[][] queries)
      throws Exception {
    File indexDir = Files.createTempDirectory("bench_closeout_radius_" + backend.toLowerCase() + "_").toFile();
    try {
      VectorIndexConfig creatorConfig =
          createIvfFlatConfig("IVF_FLAT", DIMENSION, NLIST, distanceFunction, VectorQuantizerType.SQ8);
      buildIvfFlatIndex(indexDir, corpus, creatorConfig);
      if ("IVF_FLAT".equals(backend)) {
        VectorIndexConfig readerConfig =
            createIvfFlatConfig("IVF_FLAT", DIMENSION, NLIST, distanceFunction, VectorQuantizerType.SQ8);
        try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, indexDir, readerConfig)) {
          reader.setNprobe(NPROBE);
          printRadiusResults(out, backend, distanceFunction, corpus, queries,
              (query, ignoredThreshold) -> reader.getDocIds(query, RADIUS_MAX_CANDIDATES),
              (query, threshold) -> reader.getDocIdsWithinApproximateRadius(query, threshold, RADIUS_MAX_CANDIDATES));
        }
      } else {
        VectorIndexConfig readerConfig =
            createIvfFlatConfig("IVF_ON_DISK", DIMENSION, NLIST, distanceFunction, VectorQuantizerType.SQ8);
        try (IvfOnDiskVectorIndexReader reader = new IvfOnDiskVectorIndexReader(COLUMN_NAME, indexDir, readerConfig)) {
          reader.setNprobe(NPROBE);
          printRadiusResults(out, backend, distanceFunction, corpus, queries,
              (query, ignoredThreshold) -> reader.getDocIds(query, RADIUS_MAX_CANDIDATES),
              (query, threshold) -> reader.getDocIdsWithinApproximateRadius(query, threshold, RADIUS_MAX_CANDIDATES));
        }
      }
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  private static void printRadiusResults(PrintStream out, String backend,
      VectorIndexConfig.VectorDistanceFunction distanceFunction, float[][] corpus, float[][] queries,
      RadiusCandidateProvider topKProvider, RadiusCandidateProvider approximateProvider)
      throws Exception {
    RadiusMetrics topKMetrics = benchmarkRadiusMode(
        corpus, queries, distanceFunction, "vector_index_topk_with_scan", topKProvider);
    RadiusMetrics approximateMetrics = benchmarkRadiusMode(
        corpus, queries, distanceFunction, "vector_index_approx_radius_with_scan", approximateProvider);
    RadiusMetrics exactMetrics = benchmarkExactRadius(corpus, queries, distanceFunction);

    for (RadiusMetrics metrics : Arrays.asList(topKMetrics, approximateMetrics, exactMetrics)) {
      out.printf("%-10s %-10s %-14s %10.4f %10.1f %10.1f %10.1f%n",
          backend, distanceFunction.name(), metrics._mode,
          metrics._recall, metrics._avgMatches,
          metrics._latencies._p50Ns / 1000.0, metrics._latencies._p95Ns / 1000.0);
    }
  }

  private static RadiusMetrics benchmarkRadiusMode(float[][] corpus, float[][] queries,
      VectorIndexConfig.VectorDistanceFunction distanceFunction, String mode, RadiusCandidateProvider provider)
      throws Exception {
    long[] latencies = new long[queries.length];
    double recall = 0.0d;
    long totalMatches = 0L;

    int warmup = Math.min(WARMUP_QUERIES, queries.length);
    for (int i = 0; i < warmup; i++) {
      RadiusTruth truth = computeRadiusTruth(corpus, queries[i], distanceFunction);
      ImmutableRoaringBitmap candidates = provider.search(queries[i], truth._threshold);
      filterRadiusCandidates(corpus, queries[i], candidates, truth._threshold, distanceFunction);
    }

    for (int i = 0; i < queries.length; i++) {
      RadiusTruth truth = computeRadiusTruth(corpus, queries[i], distanceFunction);
      long start = System.nanoTime();
      ImmutableRoaringBitmap candidates = provider.search(queries[i], truth._threshold);
      Set<Integer> matches =
          filterRadiusCandidates(corpus, queries[i], candidates, truth._threshold, distanceFunction);
      latencies[i] = System.nanoTime() - start;
      recall += computeRadiusRecall(truth._matchingDocIds, matches);
      totalMatches += matches.size();
    }
    return new RadiusMetrics(mode, recall / queries.length, (double) totalMatches / queries.length,
        summarizeLatencies(latencies));
  }

  private static RadiusMetrics benchmarkExactRadius(float[][] corpus, float[][] queries,
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    long[] latencies = new long[queries.length];
    long totalMatches = 0L;
    for (int i = 0; i < queries.length; i++) {
      RadiusTruth truth = computeRadiusTruth(corpus, queries[i], distanceFunction);
      long start = System.nanoTime();
      Set<Integer> exactMatches = collectRadiusMatches(corpus, queries[i], truth._threshold, distanceFunction);
      latencies[i] = System.nanoTime() - start;
      totalMatches += exactMatches.size();
    }
    return new RadiusMetrics("exact_scan", 1.0d, (double) totalMatches / queries.length, summarizeLatencies(latencies));
  }

  private static ControlBenchmarkResult benchmarkImmutableHnswControl(HnswVectorIndexReader reader, float[][] corpus,
      float[][] queries, int[][] truth, HnswControl control)
      throws Exception {
    applyControl(reader, control);
    try {
      long[] latencies = measureLatencies(queries, query -> reader.getDocIds(query, TOP_K));
      double recall10 = measureRecall(truth, queries, query -> toSet(reader.getDocIds(query, TOP_K)));
      return new ControlBenchmarkResult(control._label, recall10, summarizeLatencies(latencies));
    } finally {
      clearControl(reader);
    }
  }

  private static ControlBenchmarkResult benchmarkMutableHnswControl(MutableVectorIndex index, float[][] corpus,
      float[][] queries, int[][] truth, HnswControl control)
      throws Exception {
    applyControl(index, control);
    try {
      long[] latencies = measureLatencies(queries, query -> index.getDocIds(query, TOP_K));
      double recall10 = measureRecall(truth, queries, query -> toSet(index.getDocIds(query, TOP_K).toArray()));
      return new ControlBenchmarkResult(control._label, recall10, summarizeLatencies(latencies));
    } finally {
      clearControl(index);
    }
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

  private static VectorIndexConfig createIvfFlatConfig(String backendType, int dimension, int nlist,
      VectorIndexConfig.VectorDistanceFunction distanceFunction, VectorQuantizerType quantizerType) {
    Map<String, String> properties = new HashMap<>();
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
    Map<String, String> properties = new HashMap<>();
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

  private static double measureRecallAgainstFilteredTruth(float[][] corpus, float[][] queries, int[] allowedDocIds,
      int topK, VectorIndexConfig.VectorDistanceFunction distanceFunction, SearchResultProvider resultProvider)
      throws Exception {
    double recall = 0.0d;
    for (int i = 0; i < queries.length; i++) {
      int[] filteredTruth = exactFilteredTopK(corpus, queries[i], allowedDocIds, topK, distanceFunction);
      recall += computeFilteredRecall(filteredTruth, resultProvider.search(queries[i]));
    }
    return recall / queries.length;
  }

  private static double computeFilteredRecall(int[] truth, Set<Integer> matches) {
    if (truth.length == 0) {
      return matches.isEmpty() ? 1.0d : 0.0d;
    }
    int hits = 0;
    for (int docId : truth) {
      if (matches.contains(docId)) {
        hits++;
      }
    }
    return (double) hits / truth.length;
  }

  private static int[] exactFilteredTopK(float[][] corpus, float[] query, int[] allowedDocIds, int topK,
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    if (allowedDocIds.length == 0) {
      return new int[0];
    }
    float[] distances = new float[allowedDocIds.length];
    Integer[] ordinals = new Integer[allowedDocIds.length];
    for (int i = 0; i < allowedDocIds.length; i++) {
      distances[i] = BenchmarkVectorIndex.computeDistance(query, corpus[allowedDocIds[i]], distanceFunction);
      ordinals[i] = i;
    }
    Arrays.sort(ordinals, (left, right) -> Float.compare(distances[left], distances[right]));
    int resultSize = Math.min(topK, allowedDocIds.length);
    int[] result = new int[resultSize];
    for (int i = 0; i < resultSize; i++) {
      result[i] = allowedDocIds[ordinals[i]];
    }
    return result;
  }

  private static MutableRoaringBitmap createFilterBitmap(int numDocs, double selectivity, long seed) {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    int targetDocs = Math.max(1, (int) Math.round(numDocs * selectivity));
    Random random = new Random(seed);
    while (bitmap.getCardinality() < targetDocs) {
      bitmap.add(random.nextInt(numDocs));
    }
    return bitmap;
  }

  private static RadiusTruth computeRadiusTruth(float[][] corpus, float[] query,
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    float[] distances = new float[corpus.length];
    Integer[] docIds = new Integer[corpus.length];
    for (int i = 0; i < corpus.length; i++) {
      distances[i] = BenchmarkVectorIndex.computeDistance(query, corpus[i], distanceFunction);
      docIds[i] = i;
    }
    Arrays.sort(docIds, (left, right) -> Float.compare(distances[left], distances[right]));
    int rank = Math.min(Math.max(1, RADIUS_TARGET_MATCHES), corpus.length) - 1;
    float threshold = distances[docIds[rank]];
    Set<Integer> matches = new HashSet<>();
    for (int docId = 0; docId < corpus.length; docId++) {
      if (distances[docId] <= threshold) {
        matches.add(docId);
      }
    }
    return new RadiusTruth(threshold, matches);
  }

  private static Set<Integer> filterRadiusCandidates(float[][] corpus, float[] query, ImmutableRoaringBitmap candidates,
      float threshold, VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    Set<Integer> matches = new HashSet<>();
    org.roaringbitmap.IntIterator iterator = candidates.getIntIterator();
    while (iterator.hasNext()) {
      int docId = iterator.next();
      float distance = BenchmarkVectorIndex.computeDistance(query, corpus[docId], distanceFunction);
      if (distance <= threshold) {
        matches.add(docId);
      }
    }
    return matches;
  }

  private static Set<Integer> collectRadiusMatches(float[][] corpus, float[] query, float threshold,
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    Set<Integer> matches = new HashSet<>();
    for (int docId = 0; docId < corpus.length; docId++) {
      float distance = BenchmarkVectorIndex.computeDistance(query, corpus[docId], distanceFunction);
      if (distance <= threshold) {
        matches.add(docId);
      }
    }
    return matches;
  }

  private static double computeRadiusRecall(Set<Integer> truth, Set<Integer> result) {
    if (truth.isEmpty()) {
      return result.isEmpty() ? 1.0d : 0.0d;
    }
    int hits = 0;
    for (Integer docId : truth) {
      if (result.contains(docId)) {
        hits++;
      }
    }
    return (double) hits / truth.size();
  }

  private static Set<Integer> searchMixed(HnswVectorIndexReader immutableReader, float[][] immutableCorpus,
      MutableVectorIndex mutableIndex, float[][] mutableCorpus, float[] query) {
    PriorityQueue<ScoredDoc> heap = new PriorityQueue<>(TOP_K, (left, right) -> Float.compare(right._distance,
        left._distance));
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

  private static Set<Integer> toSet(MutableRoaringBitmap bitmap) {
    return toSet(bitmap.toArray());
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

  private static String formatPercent(double value) {
    return String.format("%.0f%%", value * 100.0d);
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

  @FunctionalInterface
  private interface RadiusCandidateProvider {
    ImmutableRoaringBitmap search(float[] query, float threshold)
        throws Exception;
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

  private static final class QuantizerBenchmarkResult {
    private final String _backend;
    private final String _quantizer;
    private final BenchmarkVectorIndex.BuildMetrics _buildMetrics;
    private final long _sizeBytes;
    private final double _recallAt10;
    private final LatencySummary _latencies;

    private QuantizerBenchmarkResult(String backend, String quantizer,
        BenchmarkVectorIndex.BuildMetrics buildMetrics, long sizeBytes, double recallAt10,
        LatencySummary latencies) {
      _backend = backend;
      _quantizer = quantizer;
      _buildMetrics = buildMetrics;
      _sizeBytes = sizeBytes;
      _recallAt10 = recallAt10;
      _latencies = latencies;
    }
  }

  private static final class ControlBenchmarkResult {
    private final String _controlLabel;
    private final double _recallAt10;
    private final LatencySummary _latencies;

    private ControlBenchmarkResult(String controlLabel, double recallAt10, LatencySummary latencies) {
      _controlLabel = controlLabel;
      _recallAt10 = recallAt10;
      _latencies = latencies;
    }
  }

  private static final class RadiusTruth {
    private final float _threshold;
    private final Set<Integer> _matchingDocIds;

    private RadiusTruth(float threshold, Set<Integer> matchingDocIds) {
      _threshold = threshold;
      _matchingDocIds = matchingDocIds;
    }
  }

  private static final class RadiusMetrics {
    private final String _mode;
    private final double _recall;
    private final double _avgMatches;
    private final LatencySummary _latencies;

    private RadiusMetrics(String mode, double recall, double avgMatches, LatencySummary latencies) {
      _mode = mode;
      _recall = recall;
      _avgMatches = avgMatches;
      _latencies = latencies;
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
