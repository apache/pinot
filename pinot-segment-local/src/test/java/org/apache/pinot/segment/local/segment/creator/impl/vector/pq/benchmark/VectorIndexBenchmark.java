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
package org.apache.pinot.segment.local.segment.creator.impl.vector.pq.benchmark;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.vector.pq.IvfPqVectorIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.vector.pq.VectorDistanceUtil;
import org.apache.pinot.segment.local.segment.index.readers.vector.IvfPqVectorIndexReader;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.Test;


/**
 * Reproducible benchmark suite for vector index backends.
 *
 * <p>Compares: exact scan, IVF_PQ (various configs).
 * HNSW benchmark requires Lucene integration which is tested separately.
 *
 * <p>This benchmark is designed to run as a unit test but produces a formatted
 * results table on stdout. It is NOT run as part of normal CI — invoke explicitly with:
 * {@code mvn -pl pinot-segment-local -Dtest=VectorIndexBenchmark test}
 */
public class VectorIndexBenchmark {
  private static final int DIMENSION = 64;
  private static final int NUM_VECTORS = 10_000;
  private static final int NUM_QUERIES = 100;
  private static final int[] TOP_K_VALUES = {10, 100};

  @Test(enabled = false, description = "Manual benchmark — run with -Dtest=VectorIndexBenchmark")
  public void runBenchmark()
      throws IOException {
    System.out.println("=== Vector Index Benchmark ===");
    System.out.println("Vectors: " + NUM_VECTORS + ", Dimension: " + DIMENSION + ", Queries: " + NUM_QUERIES);
    System.out.println();

    // Generate L2-oriented dataset (uniform random)
    Random random = new Random(42);
    float[][] l2Vectors = generateUniformVectors(NUM_VECTORS, DIMENSION, random);
    float[][] l2Queries = generateUniformVectors(NUM_QUERIES, DIMENSION, new Random(999));

    // Generate normalized cosine dataset
    float[][] cosineVectors = generateNormalizedVectors(NUM_VECTORS, DIMENSION, random);
    float[][] cosineQueries = generateNormalizedVectors(NUM_QUERIES, DIMENSION, new Random(999));

    System.out.println("## L2/Euclidean Dataset");
    benchmarkDataset("L2", l2Vectors, l2Queries, "EUCLIDEAN",
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);

    System.out.println();
    System.out.println("## Cosine Dataset (normalized vectors)");
    benchmarkDataset("Cosine", cosineVectors, cosineQueries, "COSINE",
        VectorIndexConfig.VectorDistanceFunction.COSINE);
  }

  private void benchmarkDataset(String label, float[][] vectors, float[][] queries, String distFuncStr,
      VectorIndexConfig.VectorDistanceFunction distFunc)
      throws IOException {
    int distFuncCode = distFunc == VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN ? 1 : 0;

    // Precompute exact results for recall computation
    Map<Integer, int[][]> exactResults = new HashMap<>();
    for (int topK : TOP_K_VALUES) {
      int[][] exact = new int[queries.length][];
      for (int q = 0; q < queries.length; q++) {
        exact[q] = findExactTopK(queries[q], vectors, topK, distFuncCode);
      }
      exactResults.put(topK, exact);
    }

    // Exact scan benchmark
    benchmarkExactScan(label, vectors, queries, exactResults, distFuncCode);

    // IVF_PQ configs to benchmark
    int[][] configs = {
        // {nlist, pqM, pqNbits, nprobe}
        {64, 8, 8, 4},
        {64, 8, 8, 8},
        {64, 8, 8, 16},
        {128, 16, 8, 8},
        {128, 16, 8, 16},
        {256, 16, 8, 16},
        {256, 16, 8, 32},
    };

    printHeader();
    for (int[] cfg : configs) {
      benchmarkIvfPq(label, vectors, queries, exactResults, distFuncStr, distFunc, cfg[0], cfg[1], cfg[2], cfg[3]);
    }

    // Rerank on/off comparison
    System.out.println();
    System.out.println("### Rerank Effect (nlist=128, pqM=16, pqNbits=8, nprobe=16)");
    printHeader();
    benchmarkIvfPqRerank(label, vectors, queries, exactResults, distFuncStr, distFunc, 128, 16, 8, 16, true);
    benchmarkIvfPqRerank(label, vectors, queries, exactResults, distFuncStr, distFunc, 128, 16, 8, 16, false);
  }

  private void benchmarkExactScan(String label, float[][] vectors, float[][] queries,
      Map<Integer, int[][]> exactResults, int distFuncCode) {
    System.out.println("### Exact Scan");
    System.out.printf("| %-35s | %10s | %10s | %10s | %10s | %10s | %10s |%n",
        "Config", "BuildMs", "SizeMB", "Recall@10", "Recall@100", "p50us", "p95us");
    System.out.println("|" + "-".repeat(37) + "|" + ("-".repeat(12) + "|").repeat(6));

    long[] latencies = new long[queries.length];
    for (int q = 0; q < queries.length; q++) {
      long start = System.nanoTime();
      findExactTopK(queries[q], vectors, 100, distFuncCode);
      latencies[q] = (System.nanoTime() - start) / 1000; // microseconds
    }
    Arrays.sort(latencies);
    System.out.printf("| %-35s | %10d | %10.2f | %10.4f | %10.4f | %10d | %10d |%n",
        "ExactScan", 0, 0.0, 1.0, 1.0,
        latencies[(int) (queries.length * 0.5)], latencies[(int) (queries.length * 0.95)]);
    System.out.println();
  }

  private void printHeader() {
    System.out.printf("| %-35s | %10s | %10s | %10s | %10s | %10s | %10s |%n",
        "Config", "BuildMs", "SizeMB", "Recall@10", "Recall@100", "p50us", "p95us");
    System.out.println("|" + "-".repeat(37) + "|" + ("-".repeat(12) + "|").repeat(6));
  }

  private void benchmarkIvfPq(String label, float[][] vectors, float[][] queries,
      Map<Integer, int[][]> exactResults, String distFuncStr, VectorIndexConfig.VectorDistanceFunction distFunc,
      int nlist, int pqM, int pqNbits, int nprobe)
      throws IOException {
    benchmarkIvfPqRerank(label, vectors, queries, exactResults, distFuncStr, distFunc,
        nlist, pqM, pqNbits, nprobe, true);
  }

  private void benchmarkIvfPqRerank(String label, float[][] vectors, float[][] queries,
      Map<Integer, int[][]> exactResults, String distFuncStr, VectorIndexConfig.VectorDistanceFunction distFunc,
      int nlist, int pqM, int pqNbits, int nprobe, boolean rerank)
      throws IOException {
    File tempDir = new File(FileUtils.getTempDirectory(), "bench_" + System.nanoTime());
    tempDir.mkdirs();

    try {
      VectorIndexConfig config = createConfig(distFuncStr, distFunc, nlist, pqM, pqNbits);

      // Build
      long buildStart = System.nanoTime();
      try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator("emb", tempDir, config)) {
        for (float[] v : vectors) {
          creator.add(v);
        }
        creator.seal();
      }
      long buildMs = (System.nanoTime() - buildStart) / 1_000_000;

      File indexFile = new File(tempDir, "emb" + IvfPqVectorIndexCreator.FILE_EXTENSION);
      double sizeMB = indexFile.length() / (1024.0 * 1024.0);

      // Search
      Map<String, String> opts = new HashMap<>();
      opts.put(QueryOptionKey.VECTOR_NPROBE, String.valueOf(nprobe));
      opts.put(QueryOptionKey.VECTOR_EXACT_RERANK, String.valueOf(rerank));

      long[] latencies = new long[queries.length];
      double recall10 = 0;
      double recall100 = 0;

      try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader("emb", tempDir, vectors.length, nprobe)) {
        for (int q = 0; q < queries.length; q++) {
          long start = System.nanoTime();
          ImmutableRoaringBitmap result10 = reader.getDocIds(queries[q], 10, opts);
          latencies[q] = (System.nanoTime() - start) / 1000;
          recall10 += computeRecall(result10, exactResults.get(10)[q]);

          ImmutableRoaringBitmap result100 = reader.getDocIds(queries[q], 100, opts);
          recall100 += computeRecall(result100, exactResults.get(100)[q]);
        }
      }

      recall10 /= queries.length;
      recall100 /= queries.length;
      Arrays.sort(latencies);

      String configStr = String.format("IVF_PQ(nl=%d,m=%d,nb=%d,np=%d%s)",
          nlist, pqM, pqNbits, nprobe, rerank ? ",rr" : "");
      System.out.printf("| %-35s | %10d | %10.2f | %10.4f | %10.4f | %10d | %10d |%n",
          configStr, buildMs, sizeMB, recall10, recall100,
          latencies[(int) (queries.length * 0.5)], latencies[(int) (queries.length * 0.95)]);
    } finally {
      FileUtils.deleteDirectory(tempDir);
    }
  }

  private VectorIndexConfig createConfig(String distFuncStr, VectorIndexConfig.VectorDistanceFunction distFunc,
      int nlist, int pqM, int pqNbits) {
    Map<String, String> props = new HashMap<>();
    props.put("vectorIndexType", "IVF_PQ");
    props.put("vectorDimension", String.valueOf(DIMENSION));
    props.put("vectorDistanceFunction", distFuncStr);
    props.put("nlist", String.valueOf(nlist));
    props.put("pqM", String.valueOf(pqM));
    props.put("pqNbits", String.valueOf(pqNbits));
    props.put("trainSampleSize", String.valueOf(Math.min(NUM_VECTORS, 65536)));
    props.put("trainingSeed", "42");
    return new VectorIndexConfig(false, "IVF_PQ", DIMENSION, 2, distFunc, props);
  }

  private double computeRecall(ImmutableRoaringBitmap result, int[] exactTopK) {
    int hits = 0;
    for (int docId : exactTopK) {
      if (result.contains(docId)) {
        hits++;
      }
    }
    return (double) hits / exactTopK.length;
  }

  private int[] findExactTopK(float[] query, float[][] vectors, int k, int distFuncCode) {
    float[] distances = new float[vectors.length];
    int[] indices = new int[vectors.length];
    for (int i = 0; i < vectors.length; i++) {
      distances[i] = VectorDistanceUtil.computeDistance(query, vectors[i], query.length, distFuncCode);
      indices[i] = i;
    }
    for (int i = 0; i < k; i++) {
      int minIdx = i;
      for (int j = i + 1; j < vectors.length; j++) {
        if (distances[j] < distances[minIdx]) {
          minIdx = j;
        }
      }
      float tmpD = distances[i];
      distances[i] = distances[minIdx];
      distances[minIdx] = tmpD;
      int tmpI = indices[i];
      indices[i] = indices[minIdx];
      indices[minIdx] = tmpI;
    }
    int[] result = new int[k];
    System.arraycopy(indices, 0, result, 0, k);
    return result;
  }

  private static float[][] generateUniformVectors(int n, int dim, Random random) {
    float[][] vectors = new float[n][dim];
    for (int i = 0; i < n; i++) {
      for (int d = 0; d < dim; d++) {
        vectors[i][d] = random.nextFloat() * 2 - 1;
      }
    }
    return vectors;
  }

  private static float[][] generateNormalizedVectors(int n, int dim, Random random) {
    float[][] vectors = new float[n][dim];
    for (int i = 0; i < n; i++) {
      float norm = 0;
      for (int d = 0; d < dim; d++) {
        vectors[i][d] = (float) random.nextGaussian();
        norm += vectors[i][d] * vectors[i][d];
      }
      norm = (float) Math.sqrt(norm);
      for (int d = 0; d < dim; d++) {
        vectors[i][d] /= norm;
      }
    }
    return vectors;
  }
}
