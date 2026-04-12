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

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Random;
import org.apache.pinot.common.function.scalar.VectorFunctions;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;


/**
 * Benchmark harness for Phase 3 vector query semantics: filtered ANN, threshold search,
 * and compound retrieval patterns.
 *
 * <p>This benchmark operates at the vector-level (no full segment/query engine) to measure
 * the isolated cost of execution mode selection, filtered candidate reduction, and threshold
 * refinement.</p>
 *
 * <h3>Usage</h3>
 * <pre>
 *   ./mvnw -pl pinot-perf -am compile
 *   java -cp pinot-perf/target/classes:... org.apache.pinot.perf.BenchmarkVectorPhase3
 * </pre>
 */
public final class BenchmarkVectorPhase3 {

  private static final long SEED = 42L;
  private static final int NUM_QUERIES = 200;
  private static final int WARMUP_QUERIES = 50;
  private static final PrintStream OUT = System.out;

  private BenchmarkVectorPhase3() {
  }

  public static void main(String[] args) {
    int n = Integer.getInteger("pinot.perf.vector.n", 50000);
    int dim = Integer.getInteger("pinot.perf.vector.dim", 128);
    int topK = Integer.getInteger("pinot.perf.vector.topK", 10);

    OUT.println("=== Phase 3 Vector Query Semantics Benchmark ===");
    OUT.printf("Corpus: %d vectors, dimension: %d, topK: %d%n%n", n, dim, topK);

    float[][] corpus = BenchmarkVectorIndex.generateGaussianVectors(n, dim, SEED);
    float[][] queries = BenchmarkVectorIndex.generateGaussianVectors(NUM_QUERIES, dim, SEED + 1000);
    boolean[] filterMask = generateFilterMask(n, SEED + 2000);

    VectorIndexConfig.VectorDistanceFunction distFunc = VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN;

    // 1. Exact scan baseline
    benchmarkExactScan(corpus, queries, topK, distFunc);

    // 2. Filtered exact scan at various selectivities
    for (double selectivity : new double[]{0.01, 0.1, 0.5, 0.9}) {
      boolean[] selectiveFilter = generateSelectiveFilter(n, selectivity, SEED + 3000);
      benchmarkFilteredExactScan(corpus, queries, topK, distFunc, selectiveFilter, selectivity);
    }

    // 3. Threshold scan baseline
    // For 128-dim Gaussian vectors, L2 squared distances are ~256 on average.
    // Use thresholds that produce meaningful result counts.
    for (float threshold : new float[]{200.0f, 240.0f, 260.0f}) {
      benchmarkThresholdScan(corpus, queries, threshold, distFunc);
    }

    // 4. Filtered threshold scan
    for (float threshold : new float[]{240.0f}) {
      for (double selectivity : new double[]{0.1, 0.5}) {
        boolean[] selectiveFilter = generateSelectiveFilter(n, selectivity, SEED + 3000);
        benchmarkFilteredThresholdScan(corpus, queries, threshold, distFunc, selectiveFilter, selectivity);
      }
    }

    OUT.println("\n=== Benchmark complete ===");
  }

  private static void benchmarkExactScan(float[][] corpus, float[][] queries, int topK,
      VectorIndexConfig.VectorDistanceFunction distFunc) {
    // Warmup
    for (int i = 0; i < WARMUP_QUERIES; i++) {
      exactTopK(corpus, queries[i % queries.length], topK, distFunc);
    }

    long[] latencies = new long[NUM_QUERIES];
    for (int i = 0; i < NUM_QUERIES; i++) {
      long start = System.nanoTime();
      exactTopK(corpus, queries[i], topK, distFunc);
      latencies[i] = System.nanoTime() - start;
    }

    Arrays.sort(latencies);
    OUT.printf("Exact scan (topK=%d): p50=%.2fms p95=%.2fms%n",
        topK, latencies[NUM_QUERIES / 2] / 1e6, latencies[(int) (NUM_QUERIES * 0.95)] / 1e6);
  }

  private static void benchmarkFilteredExactScan(float[][] corpus, float[][] queries, int topK,
      VectorIndexConfig.VectorDistanceFunction distFunc, boolean[] filter, double selectivity) {
    // Warmup
    for (int i = 0; i < WARMUP_QUERIES; i++) {
      filteredExactTopK(corpus, queries[i % queries.length], topK, distFunc, filter);
    }

    long[] latencies = new long[NUM_QUERIES];
    int totalResults = 0;
    for (int i = 0; i < NUM_QUERIES; i++) {
      long start = System.nanoTime();
      int[] result = filteredExactTopK(corpus, queries[i], topK, distFunc, filter);
      latencies[i] = System.nanoTime() - start;
      totalResults += result.length;
    }

    Arrays.sort(latencies);
    OUT.printf("Filtered exact scan (selectivity=%.0f%%, topK=%d): p50=%.2fms p95=%.2fms avgResults=%.1f%n",
        selectivity * 100, topK, latencies[NUM_QUERIES / 2] / 1e6, latencies[(int) (NUM_QUERIES * 0.95)] / 1e6,
        totalResults / (double) NUM_QUERIES);
  }

  private static void benchmarkThresholdScan(float[][] corpus, float[][] queries, float threshold,
      VectorIndexConfig.VectorDistanceFunction distFunc) {
    // Warmup
    for (int i = 0; i < WARMUP_QUERIES; i++) {
      thresholdScan(corpus, queries[i % queries.length], threshold, distFunc);
    }

    long[] latencies = new long[NUM_QUERIES];
    int totalResults = 0;
    for (int i = 0; i < NUM_QUERIES; i++) {
      long start = System.nanoTime();
      int count = thresholdScan(corpus, queries[i], threshold, distFunc);
      latencies[i] = System.nanoTime() - start;
      totalResults += count;
    }

    Arrays.sort(latencies);
    OUT.printf("Threshold scan (threshold=%.1f): p50=%.2fms p95=%.2fms avgResults=%.1f%n",
        threshold, latencies[NUM_QUERIES / 2] / 1e6, latencies[(int) (NUM_QUERIES * 0.95)] / 1e6,
        totalResults / (double) NUM_QUERIES);
  }

  private static void benchmarkFilteredThresholdScan(float[][] corpus, float[][] queries, float threshold,
      VectorIndexConfig.VectorDistanceFunction distFunc, boolean[] filter, double selectivity) {
    // Warmup
    for (int i = 0; i < WARMUP_QUERIES; i++) {
      filteredThresholdScan(corpus, queries[i % queries.length], threshold, distFunc, filter);
    }

    long[] latencies = new long[NUM_QUERIES];
    int totalResults = 0;
    for (int i = 0; i < NUM_QUERIES; i++) {
      long start = System.nanoTime();
      int count = filteredThresholdScan(corpus, queries[i], threshold, distFunc, filter);
      latencies[i] = System.nanoTime() - start;
      totalResults += count;
    }

    Arrays.sort(latencies);
    OUT.printf("Filtered threshold scan (threshold=%.1f, selectivity=%.0f%%): "
            + "p50=%.2fms p95=%.2fms avgResults=%.1f%n",
        threshold, selectivity * 100, latencies[NUM_QUERIES / 2] / 1e6,
        latencies[(int) (NUM_QUERIES * 0.95)] / 1e6, totalResults / (double) NUM_QUERIES);
  }

  // --- Core search methods ---

  static int[] exactTopK(float[][] corpus, float[] query, int topK,
      VectorIndexConfig.VectorDistanceFunction distFunc) {
    return BenchmarkVectorIndex.exactTopK(corpus, query, topK, distFunc);
  }

  static int[] filteredExactTopK(float[][] corpus, float[] query, int topK,
      VectorIndexConfig.VectorDistanceFunction distFunc, boolean[] filter) {
    int n = corpus.length;
    float[] distances = new float[n];
    int validCount = 0;
    for (int i = 0; i < n; i++) {
      if (!filter[i]) {
        distances[i] = Float.MAX_VALUE;
        continue;
      }
      distances[i] = computeDistance(query, corpus[i], distFunc);
      validCount++;
    }
    if (validCount == 0) {
      return new int[0];
    }
    Integer[] indices = new Integer[n];
    for (int i = 0; i < n; i++) {
      indices[i] = i;
    }
    final float[] d = distances;
    Arrays.sort(indices, (a, b) -> Float.compare(d[a], d[b]));
    int resultSize = Math.min(topK, validCount);
    int[] result = new int[resultSize];
    for (int i = 0; i < resultSize; i++) {
      result[i] = indices[i];
    }
    return result;
  }

  static int thresholdScan(float[][] corpus, float[] query, float threshold,
      VectorIndexConfig.VectorDistanceFunction distFunc) {
    int count = 0;
    for (float[] vector : corpus) {
      if (computeDistance(query, vector, distFunc) <= threshold) {
        count++;
      }
    }
    return count;
  }

  static int filteredThresholdScan(float[][] corpus, float[] query, float threshold,
      VectorIndexConfig.VectorDistanceFunction distFunc, boolean[] filter) {
    int count = 0;
    for (int i = 0; i < corpus.length; i++) {
      if (filter[i] && computeDistance(query, corpus[i], distFunc) <= threshold) {
        count++;
      }
    }
    return count;
  }

  // --- Utility methods ---

  static boolean[] generateFilterMask(int n, long seed) {
    return generateSelectiveFilter(n, 0.5, seed);
  }

  static boolean[] generateSelectiveFilter(int n, double selectivity, long seed) {
    Random rng = new Random(seed);
    boolean[] mask = new boolean[n];
    for (int i = 0; i < n; i++) {
      mask[i] = rng.nextDouble() < selectivity;
    }
    return mask;
  }

  static float computeDistance(float[] a, float[] b, VectorIndexConfig.VectorDistanceFunction distFunc) {
    switch (distFunc) {
      case EUCLIDEAN:
      case L2:
        return (float) VectorFunctions.euclideanDistance(a, b);
      case COSINE:
        return (float) VectorFunctions.cosineDistance(a, b, 1.0d);
      default:
        throw new IllegalArgumentException("Unsupported: " + distFunc);
    }
  }
}
