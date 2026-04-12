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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.readers.vector.IvfFlatVectorIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.vector.IvfOnDiskVectorIndexReader;
import org.apache.pinot.segment.local.segment.index.vector.IvfFlatVectorIndexCreator;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorQuantizerType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Pinot-specific query workload benchmark for filtered ANN and radius execution.
 *
 * <p>Unlike broad ANN frontiers, this suite focuses on the query shapes Pinot actually has to
 * execute correctly: exact filtered top-K, exact radius, filter-aware ANN on immutable IVF
 * backends, and approximate-radius candidate generation with exact refinement.</p>
 */
public final class BenchmarkVectorFilterWorkloads {
  private static final String COLUMN_NAME = "embedding";
  private static final long SEED = 20260411L;

  private static final int DIMENSION =
      Integer.getInteger("pinot.perf.vector.filters.dimension", Integer.getInteger("pinot.perf.vector.dimension", 768));
  private static final int NUM_VECTORS =
      Integer.getInteger("pinot.perf.vector.filters.size", Integer.getInteger("pinot.perf.vector.n", 12_000));
  private static final int NUM_QUERIES =
      Integer.getInteger("pinot.perf.vector.filters.queries", Integer.getInteger("pinot.perf.vector.queries", 80));
  private static final int WARMUP_QUERIES =
      Integer.getInteger("pinot.perf.vector.filters.warmupQueries", 20);
  private static final int TOP_K =
      Integer.getInteger("pinot.perf.vector.filters.topK", Integer.getInteger("pinot.perf.vector.topK", 10));
  private static final int NLIST = Integer.getInteger("pinot.perf.vector.filters.nlist", 96);
  private static final int NPROBE = Integer.getInteger("pinot.perf.vector.filters.nprobe", 8);
  private static final int RADIUS_TARGET_MATCHES =
      Integer.getInteger("pinot.perf.vector.filters.radiusTargetMatches", 24);
  private static final int RADIUS_MAX_CANDIDATES =
      Integer.getInteger("pinot.perf.vector.filters.radiusMaxCandidates", 512);
  private static final VectorQuantizerType FILTER_QUANTIZER = VectorQuantizerType.valueOf(
      System.getProperty("pinot.perf.vector.filters.quantizer", VectorQuantizerType.SQ8.name()));

  private BenchmarkVectorFilterWorkloads() {
  }

  public static void main(String[] args)
      throws Exception {
    run(System.out);
  }

  static void run(PrintStream out)
      throws Exception {
    out.println("========================================");
    out.println("  Apache Pinot Vector Filter Workloads");
    out.println("========================================");
    out.printf("JDK: %s%n", System.getProperty("java.version"));
    out.printf("OS: %s %s%n", System.getProperty("os.name"), System.getProperty("os.arch"));
    out.printf("Seed: %d%n", SEED);
    out.printf("Vectors: %d | Dimension: %d | Queries: %d | Warmup: %d%n",
        NUM_VECTORS, DIMENSION, NUM_QUERIES, WARMUP_QUERIES);
    out.printf("topK=%d | nlist=%d | nprobe=%d | quantizer=%s | radiusMaxCandidates=%d%n",
        TOP_K, NLIST, NPROBE, FILTER_QUANTIZER.name(), RADIUS_MAX_CANDIDATES);

    benchmarkExactBaselines(out);
    benchmarkFilterAwareAnn(out);
    benchmarkApproximateRadius(out);
  }

  private static void benchmarkExactBaselines(PrintStream out) {
    out.println();
    out.println("=== Exact Query Baselines ===");
    out.printf("%-28s %12s %12s %12s %12s%n", "Workload", "avgHits", "p50(us)", "p95(us)", "QPS");

    float[][] corpus = BenchmarkVectorIndex.generateGaussianVectors(NUM_VECTORS, DIMENSION, SEED);
    float[][] queries = BenchmarkVectorIndex.generateGaussianVectors(NUM_QUERIES, DIMENSION, SEED + 1_000L);
    boolean[] selectivity10 = generateSelectiveFilter(NUM_VECTORS, 0.10d, SEED + 2_000L);
    boolean[] selectivity01 = generateSelectiveFilter(NUM_VECTORS, 0.01d, SEED + 3_000L);
    VectorIndexConfig.VectorDistanceFunction distanceFunction = VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN;

    ExactBaselineResult exactTopK =
        benchmarkTopK(corpus, queries, distanceFunction, null, "exact_topk");
    ExactBaselineResult filteredTopK10 =
        benchmarkTopK(corpus, queries, distanceFunction, selectivity10, "exact_filtered_topk_10%");
    ExactBaselineResult filteredTopK01 =
        benchmarkTopK(corpus, queries, distanceFunction, selectivity01, "exact_filtered_topk_1%");
    ExactBaselineResult exactRadius =
        benchmarkRadius(corpus, queries, distanceFunction, null, "exact_radius");
    ExactBaselineResult filteredRadius10 =
        benchmarkRadius(corpus, queries, distanceFunction, selectivity10, "exact_filtered_radius_10%");

    for (ExactBaselineResult result
        : Arrays.asList(exactTopK, filteredTopK10, filteredTopK01, exactRadius, filteredRadius10)) {
      out.printf("%-28s %12.1f %12.1f %12.1f %12.1f%n",
          result._label, result._averageHits,
          result._latencySummary._p50Ns / 1000.0,
          result._latencySummary._p95Ns / 1000.0,
          result._queriesPerSecond);
    }
  }

  private static void benchmarkFilterAwareAnn(PrintStream out)
      throws Exception {
    out.println();
    out.println("=== Filter-aware ANN Selectivity Sweep ===");
    out.printf("%-12s %-12s %10s %10s %10s %10s %10s %10s%n",
        "Backend", "Selectivity", "Recall@10", "avgHits", "p50(us)", "p95(us)", "QPS", "exactP50");

    float[][] corpus = BenchmarkVectorIndex.generateGaussianVectors(NUM_VECTORS, DIMENSION, SEED + 10_000L);
    float[][] queries = BenchmarkVectorIndex.generateGaussianVectors(NUM_QUERIES, DIMENSION, SEED + 11_000L);
    VectorIndexConfig.VectorDistanceFunction distanceFunction = VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN;

    File indexDir = Files.createTempDirectory("bench_filters_ivf_").toFile();
    try {
      VectorIndexConfig creatorConfig =
          createIvfConfig("IVF_FLAT", DIMENSION, NLIST, distanceFunction, FILTER_QUANTIZER);
      buildIvfFlatIndex(indexDir, corpus, creatorConfig);

      try (IvfFlatVectorIndexReader ivfFlatReader = new IvfFlatVectorIndexReader(COLUMN_NAME, indexDir, creatorConfig);
          IvfOnDiskVectorIndexReader ivfOnDiskReader =
              new IvfOnDiskVectorIndexReader(COLUMN_NAME, indexDir,
                  createIvfConfig("IVF_ON_DISK", DIMENSION, NLIST, distanceFunction, FILTER_QUANTIZER))) {
        ivfFlatReader.setNprobe(NPROBE);
        ivfOnDiskReader.setNprobe(NPROBE);

        for (double selectivity : Arrays.asList(1.0d, 0.10d, 0.01d, 0.001d)) {
          MutableRoaringBitmap filterBitmap = createFilterBitmap(corpus.length, selectivity,
              SEED + (long) (selectivity * 1_000_000));
          int[] allowedDocIds = filterBitmap.toArray();
          ExactBaselineResult exactBaseline =
              benchmarkExactFilteredTopK(corpus, queries, allowedDocIds, distanceFunction,
                  "exact_filtered_topk_" + formatPercent(selectivity));
          FilterResult flatResult = benchmarkFilterAwareBackend(
              "IVF_FLAT", (query, bitmap) -> ivfFlatReader.getDocIds(query, TOP_K, bitmap),
              corpus, queries, filterBitmap, allowedDocIds, distanceFunction);
          FilterResult onDiskResult = benchmarkFilterAwareBackend(
              "IVF_ON_DISK", (query, bitmap) -> ivfOnDiskReader.getDocIds(query, TOP_K, bitmap),
              corpus, queries, filterBitmap, allowedDocIds, distanceFunction);

          for (FilterResult result : Arrays.asList(flatResult, onDiskResult)) {
            out.printf("%-12s %-12s %10.4f %10.1f %10.1f %10.1f %10.1f %10.1f%n",
                result._backend, formatPercent(selectivity), result._recallAt10, result._averageHits,
                result._latencySummary._p50Ns / 1000.0,
                result._latencySummary._p95Ns / 1000.0,
                result._queriesPerSecond,
                exactBaseline._latencySummary._p50Ns / 1000.0);
          }
        }
      }
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  private static void benchmarkApproximateRadius(PrintStream out)
      throws Exception {
    out.println();
    out.println("=== Approximate Radius Workloads ===");
    out.printf("%-12s %-10s %-16s %10s %10s %10s %10s %10s%n",
        "Backend", "Distance", "Mode", "Recall", "avgHits", "p50(us)", "p95(us)", "QPS");

    runRadiusScenario(out, "IVF_FLAT", VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN,
        BenchmarkVectorIndex.generateGaussianVectors(NUM_VECTORS, DIMENSION, SEED + 20_000L),
        BenchmarkVectorIndex.generateGaussianVectors(NUM_QUERIES, DIMENSION, SEED + 21_000L));
    runRadiusScenario(out, "IVF_ON_DISK", VectorIndexConfig.VectorDistanceFunction.COSINE,
        BenchmarkVectorIndex.generateNormalizedVectors(NUM_VECTORS, DIMENSION, SEED + 22_000L),
        BenchmarkVectorIndex.generateNormalizedVectors(NUM_QUERIES, DIMENSION, SEED + 23_000L));
  }

  private static ExactBaselineResult benchmarkTopK(float[][] corpus, float[][] queries,
      VectorIndexConfig.VectorDistanceFunction distanceFunction, boolean[] filter, String label) {
    long[] latencies = new long[queries.length];
    long totalHits = 0L;
    int warmup = Math.min(WARMUP_QUERIES, queries.length);
    for (int i = 0; i < warmup; i++) {
      if (filter == null) {
        BenchmarkVectorIndex.exactTopK(corpus, queries[i], TOP_K, distanceFunction);
      } else {
        filteredExactTopK(corpus, queries[i], TOP_K, distanceFunction, filter);
      }
    }
    for (int i = 0; i < queries.length; i++) {
      long start = System.nanoTime();
      int[] matches = filter == null
          ? BenchmarkVectorIndex.exactTopK(corpus, queries[i], TOP_K, distanceFunction)
          : filteredExactTopK(corpus, queries[i], TOP_K, distanceFunction, filter);
      latencies[i] = System.nanoTime() - start;
      totalHits += matches.length;
    }
    return new ExactBaselineResult(label, (double) totalHits / queries.length,
        summarizeLatencies(latencies), BenchmarkVectorIndex.queriesPerSecond(latencies));
  }

  private static ExactBaselineResult benchmarkRadius(float[][] corpus, float[][] queries,
      VectorIndexConfig.VectorDistanceFunction distanceFunction, boolean[] filter, String label) {
    long[] latencies = new long[queries.length];
    long totalHits = 0L;
    int warmup = Math.min(WARMUP_QUERIES, queries.length);
    for (int i = 0; i < warmup; i++) {
      RadiusTruth truth = computeRadiusTruth(corpus, queries[i], distanceFunction);
      if (filter == null) {
        collectRadiusMatches(corpus, queries[i], truth._threshold, distanceFunction);
      } else {
        collectFilteredRadiusMatches(corpus, queries[i], truth._threshold, distanceFunction, filter);
      }
    }
    for (int i = 0; i < queries.length; i++) {
      RadiusTruth truth = computeRadiusTruth(corpus, queries[i], distanceFunction);
      long start = System.nanoTime();
      Set<Integer> matches = filter == null
          ? collectRadiusMatches(corpus, queries[i], truth._threshold, distanceFunction)
          : collectFilteredRadiusMatches(corpus, queries[i], truth._threshold, distanceFunction, filter);
      latencies[i] = System.nanoTime() - start;
      totalHits += matches.size();
    }
    return new ExactBaselineResult(label, (double) totalHits / queries.length,
        summarizeLatencies(latencies), BenchmarkVectorIndex.queriesPerSecond(latencies));
  }

  private static ExactBaselineResult benchmarkExactFilteredTopK(float[][] corpus, float[][] queries,
      int[] allowedDocIds,
      VectorIndexConfig.VectorDistanceFunction distanceFunction, String label) {
    long[] latencies = new long[queries.length];
    long totalHits = 0L;
    int warmup = Math.min(WARMUP_QUERIES, queries.length);
    for (int i = 0; i < warmup; i++) {
      exactFilteredTopK(corpus, queries[i], allowedDocIds, TOP_K, distanceFunction);
    }
    for (int i = 0; i < queries.length; i++) {
      long start = System.nanoTime();
      int[] matches = exactFilteredTopK(corpus, queries[i], allowedDocIds, TOP_K, distanceFunction);
      latencies[i] = System.nanoTime() - start;
      totalHits += matches.length;
    }
    return new ExactBaselineResult(label, (double) totalHits / queries.length,
        summarizeLatencies(latencies), BenchmarkVectorIndex.queriesPerSecond(latencies));
  }

  private static FilterResult benchmarkFilterAwareBackend(String backend, FilterReader reader, float[][] corpus,
      float[][] queries, ImmutableRoaringBitmap filterBitmap, int[] allowedDocIds,
      VectorIndexConfig.VectorDistanceFunction distanceFunction)
      throws Exception {
    int[][] truth = new int[queries.length][];
    for (int i = 0; i < queries.length; i++) {
      truth[i] = exactFilteredTopK(corpus, queries[i], allowedDocIds, TOP_K, distanceFunction);
    }

    int warmup = Math.min(WARMUP_QUERIES, queries.length);
    for (int i = 0; i < warmup; i++) {
      reader.search(queries[i], filterBitmap);
    }

    long[] latencies = new long[queries.length];
    double recall = 0.0d;
    long totalHits = 0L;
    for (int i = 0; i < queries.length; i++) {
      long start = System.nanoTime();
      ImmutableRoaringBitmap result = reader.search(queries[i], filterBitmap);
      latencies[i] = System.nanoTime() - start;
      Set<Integer> matches = BenchmarkVectorIndex.bitmapToSet(result);
      recall += computeFilteredRecall(truth[i], matches);
      totalHits += matches.size();
    }
    return new FilterResult(backend, recall / queries.length, (double) totalHits / queries.length,
        summarizeLatencies(latencies), BenchmarkVectorIndex.queriesPerSecond(latencies));
  }

  private static void runRadiusScenario(PrintStream out, String backend,
      VectorIndexConfig.VectorDistanceFunction distanceFunction, float[][] corpus, float[][] queries)
      throws Exception {
    File indexDir = Files.createTempDirectory("bench_filters_radius_" + backend.toLowerCase() + "_").toFile();
    try {
      VectorIndexConfig creatorConfig =
          createIvfConfig("IVF_FLAT", DIMENSION, NLIST, distanceFunction, FILTER_QUANTIZER);
      buildIvfFlatIndex(indexDir, corpus, creatorConfig);
      if ("IVF_FLAT".equals(backend)) {
        try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, indexDir, creatorConfig)) {
          reader.setNprobe(NPROBE);
          printRadiusResults(out, backend, distanceFunction, corpus, queries,
              (query, ignoredThreshold) -> reader.getDocIds(query, RADIUS_MAX_CANDIDATES),
              (query, threshold) -> reader.getDocIdsWithinApproximateRadius(query, threshold, RADIUS_MAX_CANDIDATES));
        }
      } else {
        VectorIndexConfig readerConfig =
            createIvfConfig("IVF_ON_DISK", DIMENSION, NLIST, distanceFunction, FILTER_QUANTIZER);
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
        corpus, queries, distanceFunction, "topk_then_scan", topKProvider);
    RadiusMetrics approximateMetrics = benchmarkRadiusMode(
        corpus, queries, distanceFunction, "approx_radius", approximateProvider);
    RadiusMetrics exactMetrics = benchmarkExactRadius(corpus, queries, distanceFunction);

    for (RadiusMetrics metrics : Arrays.asList(topKMetrics, approximateMetrics, exactMetrics)) {
      out.printf("%-12s %-10s %-16s %10.4f %10.1f %10.1f %10.1f %10.1f%n",
          backend, distanceFunction.name(), metrics._mode,
          metrics._recall, metrics._avgMatches,
          metrics._latencies._p50Ns / 1000.0,
          metrics._latencies._p95Ns / 1000.0,
          metrics._queriesPerSecond);
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
        summarizeLatencies(latencies), BenchmarkVectorIndex.queriesPerSecond(latencies));
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
    return new RadiusMetrics("exact_scan", 1.0d, (double) totalMatches / queries.length,
        summarizeLatencies(latencies), BenchmarkVectorIndex.queriesPerSecond(latencies));
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

  private static VectorIndexConfig createIvfConfig(String backendType, int dimension, int nlist,
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

  private static int[] filteredExactTopK(float[][] corpus, float[] query, int topK,
      VectorIndexConfig.VectorDistanceFunction distanceFunction, boolean[] filter) {
    int n = corpus.length;
    float[] distances = new float[n];
    int validCount = 0;
    for (int i = 0; i < n; i++) {
      if (!filter[i]) {
        distances[i] = Float.MAX_VALUE;
        continue;
      }
      distances[i] = BenchmarkVectorIndex.computeDistance(query, corpus[i], distanceFunction);
      validCount++;
    }
    if (validCount == 0) {
      return new int[0];
    }
    Integer[] indices = new Integer[n];
    for (int i = 0; i < n; i++) {
      indices[i] = i;
    }
    final float[] distanceRef = distances;
    Arrays.sort(indices, (left, right) -> Float.compare(distanceRef[left], distanceRef[right]));
    int[] result = new int[Math.min(topK, validCount)];
    for (int i = 0; i < result.length; i++) {
      result[i] = indices[i];
    }
    return result;
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
    int[] result = new int[Math.min(topK, allowedDocIds.length)];
    for (int i = 0; i < result.length; i++) {
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

  private static Set<Integer> collectRadiusMatches(float[][] corpus, float[] query, float threshold,
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    Set<Integer> matches = new HashSet<>();
    for (int docId = 0; docId < corpus.length; docId++) {
      if (BenchmarkVectorIndex.computeDistance(query, corpus[docId], distanceFunction) <= threshold) {
        matches.add(docId);
      }
    }
    return matches;
  }

  private static Set<Integer> collectFilteredRadiusMatches(float[][] corpus, float[] query, float threshold,
      VectorIndexConfig.VectorDistanceFunction distanceFunction, boolean[] filter) {
    Set<Integer> matches = new HashSet<>();
    for (int docId = 0; docId < corpus.length; docId++) {
      if (filter[docId] && BenchmarkVectorIndex.computeDistance(query, corpus[docId], distanceFunction) <= threshold) {
        matches.add(docId);
      }
    }
    return matches;
  }

  private static Set<Integer> filterRadiusCandidates(float[][] corpus, float[] query, ImmutableRoaringBitmap candidates,
      float threshold, VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    Set<Integer> matches = new HashSet<>();
    org.roaringbitmap.IntIterator iterator = candidates.getIntIterator();
    while (iterator.hasNext()) {
      int docId = iterator.next();
      if (BenchmarkVectorIndex.computeDistance(query, corpus[docId], distanceFunction) <= threshold) {
        matches.add(docId);
      }
    }
    return matches;
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

  private static boolean[] generateSelectiveFilter(int numDocs, double selectivity, long seed) {
    Random random = new Random(seed);
    boolean[] filter = new boolean[numDocs];
    for (int i = 0; i < numDocs; i++) {
      filter[i] = random.nextDouble() < selectivity;
    }
    return filter;
  }

  private static BenchmarkVectorIndex.LatencySummary summarizeLatencies(long[] latencies) {
    long[] copy = Arrays.copyOf(latencies, latencies.length);
    Arrays.sort(copy);
    return new BenchmarkVectorIndex.LatencySummary(
        BenchmarkVectorIndex.percentile(copy, 50),
        BenchmarkVectorIndex.percentile(copy, 95),
        BenchmarkVectorIndex.percentile(copy, 99));
  }

  private static String formatPercent(double selectivity) {
    return String.format("%.1f%%", selectivity * 100.0d);
  }

  @FunctionalInterface
  private interface FilterReader {
    ImmutableRoaringBitmap search(float[] query, ImmutableRoaringBitmap filterBitmap);
  }

  @FunctionalInterface
  private interface RadiusCandidateProvider {
    ImmutableRoaringBitmap search(float[] query, float threshold)
        throws Exception;
  }

  private static final class ExactBaselineResult {
    private final String _label;
    private final double _averageHits;
    private final BenchmarkVectorIndex.LatencySummary _latencySummary;
    private final double _queriesPerSecond;

    private ExactBaselineResult(String label, double averageHits, BenchmarkVectorIndex.LatencySummary latencySummary,
        double queriesPerSecond) {
      _label = label;
      _averageHits = averageHits;
      _latencySummary = latencySummary;
      _queriesPerSecond = queriesPerSecond;
    }
  }

  private static final class FilterResult {
    private final String _backend;
    private final double _recallAt10;
    private final double _averageHits;
    private final BenchmarkVectorIndex.LatencySummary _latencySummary;
    private final double _queriesPerSecond;

    private FilterResult(String backend, double recallAt10, double averageHits,
        BenchmarkVectorIndex.LatencySummary latencySummary, double queriesPerSecond) {
      _backend = backend;
      _recallAt10 = recallAt10;
      _averageHits = averageHits;
      _latencySummary = latencySummary;
      _queriesPerSecond = queriesPerSecond;
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
    private final BenchmarkVectorIndex.LatencySummary _latencies;
    private final double _queriesPerSecond;

    private RadiusMetrics(String mode, double recall, double avgMatches,
        BenchmarkVectorIndex.LatencySummary latencies, double queriesPerSecond) {
      _mode = mode;
      _recall = recall;
      _avgMatches = avgMatches;
      _latencies = latencies;
      _queriesPerSecond = queriesPerSecond;
    }
  }
}
