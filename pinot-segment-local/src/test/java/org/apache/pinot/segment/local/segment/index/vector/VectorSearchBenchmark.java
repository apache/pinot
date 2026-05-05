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
package org.apache.pinot.segment.local.segment.index.vector;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.pinot.segment.local.segment.index.readers.vector.IvfFlatVectorIndexReader;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Manual benchmark for vector search operations across different backends and configurations.
 * All test methods are disabled by default so they do not run in CI. Enable individually
 * when running locally for performance evaluation.
 *
 * <p>This is a comparative micro-benchmark that measures:</p>
 * <ul>
 *   <li>IVF_FLAT search latency with different nprobe values</li>
 *   <li>SQ8 vs SQ4 vs FLAT quantizer encode/decode/distance performance</li>
 *   <li>Pre-filter vs post-filter ANN with different selectivities</li>
 *   <li>Recall quality across configurations</li>
 * </ul>
 *
 * <p>Run with: {@code mvn test -pl pinot-segment-local -Dtest=VectorSearchBenchmark -Dcheckstyle.skip}</p>
 */
public class VectorSearchBenchmark {

  private static final int DIMENSION = 128;
  private static final int NUM_VECTORS = 50000;
  private static final int NUM_QUERIES = 100;
  private static final int TOP_K = 10;
  private static final int NLIST = 128;
  private static final Random RANDOM = new Random(42);

  private float[][] _vectors;
  private float[][] _queries;
  private File _tempDir;
  private IvfFlatVectorIndexReader _ivfReader;

  @BeforeClass
  public void setUp() throws Exception {
    // Generate dataset
    _vectors = new float[NUM_VECTORS][DIMENSION];
    for (int i = 0; i < NUM_VECTORS; i++) {
      for (int d = 0; d < DIMENSION; d++) {
        _vectors[i][d] = RANDOM.nextFloat() * 2 - 1;
      }
    }

    _queries = new float[NUM_QUERIES][DIMENSION];
    for (int i = 0; i < NUM_QUERIES; i++) {
      for (int d = 0; d < DIMENSION; d++) {
        _queries[i][d] = RANDOM.nextFloat() * 2 - 1;
      }
    }

    // Build IVF_FLAT index
    _tempDir = Files.createTempDirectory("vector-bench").toFile();
    Map<String, String> props = new HashMap<>();
    props.put("vectorIndexType", "IVF_FLAT");
    props.put("vectorDimension", String.valueOf(DIMENSION));
    props.put("vectorDistanceFunction", "EUCLIDEAN");
    props.put("version", "1");
    props.put("nlist", String.valueOf(NLIST));
    props.put("trainingSeed", "42");
    VectorIndexConfig config = new VectorIndexConfig(false, "IVF_FLAT", DIMENSION, 1,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, props);

    try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator("embedding", _tempDir, config)) {
      for (float[] vector : _vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    _ivfReader = new IvfFlatVectorIndexReader("embedding", _tempDir, config);
  }

  @AfterClass
  public void tearDown() throws IOException {
    if (_ivfReader != null) {
      _ivfReader.close();
    }
    if (_tempDir != null) {
      for (File f : _tempDir.listFiles()) {
        f.delete();
      }
      _tempDir.delete();
    }
  }

  // -----------------------------------------------------------------------
  // Benchmark 1: IVF_FLAT nprobe sweep
  // -----------------------------------------------------------------------

  @Test(enabled = false)
  public void benchmarkNprobeSweep() {
    System.out.println("\n=== IVF_FLAT nprobe Sweep (" + NUM_VECTORS + " vectors, dim=" + DIMENSION + ") ===");
    System.out.printf("%-10s %-15s %-15s %-10s%n", "nprobe", "avg_latency_us", "avg_recall@10", "candidates");

    int[] nprobeValues = {1, 2, 4, 8, 16, 32, 64, 128};
    for (int nprobe : nprobeValues) {
      _ivfReader.setNprobe(nprobe);

      long totalLatencyNs = 0;
      double totalRecall = 0;
      int totalCandidates = 0;

      for (int q = 0; q < NUM_QUERIES; q++) {
        long start = System.nanoTime();
        MutableRoaringBitmap result = _ivfReader.getDocIds(_queries[q], TOP_K);
        long elapsed = System.nanoTime() - start;

        totalLatencyNs += elapsed;
        totalCandidates += result.getCardinality();

        // Compute recall against brute-force
        int[] exactTopK = bruteForceTopK(_queries[q], TOP_K);
        int overlap = 0;
        for (int docId : exactTopK) {
          if (result.contains(docId)) {
            overlap++;
          }
        }
        totalRecall += (double) overlap / TOP_K;
      }

      _ivfReader.clearNprobe();

      System.out.printf("%-10d %-15.1f %-15.3f %-10.1f%n",
          nprobe,
          (double) totalLatencyNs / NUM_QUERIES / 1000,
          totalRecall / NUM_QUERIES,
          (double) totalCandidates / NUM_QUERIES);
    }
  }

  // -----------------------------------------------------------------------
  // Benchmark 2: SQ8 vs SQ4 vs FLAT quantizer
  // -----------------------------------------------------------------------

  @Test(enabled = false)
  public void benchmarkQuantizers() {
    System.out.println("\n=== Quantizer Comparison (dim=" + DIMENSION + ", " + NUM_VECTORS + " vectors) ===");
    System.out.printf("%-8s %-15s %-15s %-15s %-12s %-10s%n",
        "Type", "encode_us/vec", "decode_us/vec", "dist_us/vec", "bytes/vec", "recall@10");

    // Train quantizers
    float[][] trainSample = new float[Math.min(10000, NUM_VECTORS)][];
    System.arraycopy(_vectors, 0, trainSample, 0, trainSample.length);

    ScalarQuantizer sq8 = ScalarQuantizer.train(trainSample, DIMENSION, ScalarQuantizer.BitWidth.SQ8);
    ScalarQuantizer sq4 = ScalarQuantizer.train(trainSample, DIMENSION, ScalarQuantizer.BitWidth.SQ4);

    // Encode all vectors
    byte[][] encodedSq8 = new byte[NUM_VECTORS][];
    byte[][] encodedSq4 = new byte[NUM_VECTORS][];

    // Benchmark encode
    long sq8EncodeNs = benchmarkEncode(sq8, _vectors, encodedSq8);
    long sq4EncodeNs = benchmarkEncode(sq4, _vectors, encodedSq4);

    // Benchmark decode
    long sq8DecodeNs = benchmarkDecode(sq8, encodedSq8);
    long sq4DecodeNs = benchmarkDecode(sq4, encodedSq4);

    // Benchmark distance
    long flatDistNs = benchmarkFlatDistance(_queries, _vectors);
    long sq8DistNs = benchmarkQuantizedDistance(sq8, _queries, encodedSq8);
    long sq4DistNs = benchmarkQuantizedDistance(sq4, _queries, encodedSq4);

    // Benchmark recall
    double sq8Recall = benchmarkQuantizedRecall(sq8, _queries, encodedSq8, TOP_K);
    double sq4Recall = benchmarkQuantizedRecall(sq4, _queries, encodedSq4, TOP_K);

    int distIterCount = Math.min(1000, NUM_VECTORS);
    System.out.printf("%-8s %-15s %-15s %-15.1f %-12d %-10.3f%n",
        "FLAT", "N/A", "N/A",
        (double) flatDistNs / NUM_QUERIES / distIterCount * 1000,
        DIMENSION * 4, 1.0);
    System.out.printf("%-8s %-15.1f %-15.1f %-15.1f %-12d %-10.3f%n",
        "SQ8",
        (double) sq8EncodeNs / NUM_VECTORS / 1000,
        (double) sq8DecodeNs / NUM_VECTORS / 1000,
        (double) sq8DistNs / NUM_QUERIES / distIterCount * 1000,
        sq8.getEncodedBytesPerVector(), sq8Recall);
    System.out.printf("%-8s %-15.1f %-15.1f %-15.1f %-12d %-10.3f%n",
        "SQ4",
        (double) sq4EncodeNs / NUM_VECTORS / 1000,
        (double) sq4DecodeNs / NUM_VECTORS / 1000,
        (double) sq4DistNs / NUM_QUERIES / distIterCount * 1000,
        sq4.getEncodedBytesPerVector(), sq4Recall);
  }

  // -----------------------------------------------------------------------
  // Benchmark 3: Pre-filter vs post-filter selectivity sweep
  // -----------------------------------------------------------------------

  @Test(enabled = false)
  public void benchmarkFilterSelectivity() {
    System.out.println("\n=== Filter Selectivity Sweep (pre-filter vs no-filter) ===");
    System.out.printf("%-15s %-15s %-15s %-15s %-10s%n",
        "selectivity", "nofilt_us", "prefilt_us", "speedup", "recall");

    double[] selectivities = {1.0, 0.5, 0.2, 0.1, 0.05, 0.01};
    for (double sel : selectivities) {
      int filteredCount = (int) (NUM_VECTORS * sel);
      MutableRoaringBitmap filterBitmap = new MutableRoaringBitmap();
      for (int i = 0; i < filteredCount; i++) {
        filterBitmap.add(RANDOM.nextInt(NUM_VECTORS));
      }

      _ivfReader.setNprobe(8);

      // Unfiltered
      long unfilteredNs = 0;
      for (int q = 0; q < NUM_QUERIES; q++) {
        long start = System.nanoTime();
        _ivfReader.getDocIds(_queries[q], TOP_K);
        unfilteredNs += System.nanoTime() - start;
      }

      // Pre-filtered
      long filteredNs = 0;
      double totalRecall = 0;
      for (int q = 0; q < NUM_QUERIES; q++) {
        long start = System.nanoTime();
        ImmutableRoaringBitmap result = _ivfReader.getDocIds(_queries[q], TOP_K, filterBitmap);
        filteredNs += System.nanoTime() - start;

        // Check all results are in filter
        org.roaringbitmap.IntIterator it = result.getIntIterator();
        int inFilter = 0;
        while (it.hasNext()) {
          if (filterBitmap.contains(it.next())) {
            inFilter++;
          }
        }
        totalRecall += result.getCardinality() > 0 ? (double) inFilter / result.getCardinality() : 1.0;
      }

      _ivfReader.clearNprobe();

      double avgUnfiltered = (double) unfilteredNs / NUM_QUERIES / 1000;
      double avgFiltered = (double) filteredNs / NUM_QUERIES / 1000;
      double speedup = avgUnfiltered / Math.max(avgFiltered, 1);

      System.out.printf("%-15.2f %-15.1f %-15.1f %-15.2fx %-10.3f%n",
          sel, avgUnfiltered, avgFiltered, speedup, totalRecall / NUM_QUERIES);
    }
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private int[] bruteForceTopK(float[] query, int k) {
    float[] distances = new float[NUM_VECTORS];
    int[] indices = new int[NUM_VECTORS];
    for (int i = 0; i < NUM_VECTORS; i++) {
      distances[i] = euclideanDistance(query, _vectors[i]);
      indices[i] = i;
    }
    // Partial sort for top-k
    for (int i = 0; i < k; i++) {
      for (int j = i + 1; j < NUM_VECTORS; j++) {
        if (distances[j] < distances[i]) {
          float tmpD = distances[i];
          distances[i] = distances[j];
          distances[j] = tmpD;
          int tmpI = indices[i];
          indices[i] = indices[j];
          indices[j] = tmpI;
        }
      }
    }
    int[] result = new int[k];
    System.arraycopy(indices, 0, result, 0, k);
    return result;
  }

  private static float euclideanDistance(float[] a, float[] b) {
    float sum = 0;
    for (int d = 0; d < a.length; d++) {
      float diff = a[d] - b[d];
      sum += diff * diff;
    }
    return (float) Math.sqrt(sum);
  }

  private long benchmarkEncode(ScalarQuantizer sq, float[][] vectors, byte[][] output) {
    long start = System.nanoTime();
    for (int i = 0; i < vectors.length; i++) {
      output[i] = sq.encode(vectors[i]);
    }
    return System.nanoTime() - start;
  }

  private long benchmarkDecode(ScalarQuantizer sq, byte[][] encoded) {
    long start = System.nanoTime();
    for (byte[] e : encoded) {
      sq.decode(e);
    }
    return System.nanoTime() - start;
  }

  private long benchmarkFlatDistance(float[][] queries, float[][] vectors) {
    int iterCount = Math.min(1000, vectors.length);
    long start = System.nanoTime();
    for (float[] query : queries) {
      for (int i = 0; i < iterCount; i++) {
        euclideanDistance(query, vectors[i]);
      }
    }
    return System.nanoTime() - start;
  }

  private long benchmarkQuantizedDistance(ScalarQuantizer sq, float[][] queries, byte[][] encoded) {
    int iterCount = Math.min(1000, encoded.length);
    long start = System.nanoTime();
    for (float[] query : queries) {
      for (int i = 0; i < iterCount; i++) {
        sq.computeDistance(query, encoded[i], VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
      }
    }
    return System.nanoTime() - start;
  }

  private double benchmarkQuantizedRecall(ScalarQuantizer sq, float[][] queries, byte[][] encoded, int k) {
    double totalRecall = 0;
    for (int q = 0; q < queries.length; q++) {
      int[] exactTopK = bruteForceTopK(queries[q], k);
      // Find approximate top-k using quantized distances
      float[] approxDists = new float[encoded.length];
      int[] approxIndices = new int[encoded.length];
      for (int i = 0; i < encoded.length; i++) {
        approxDists[i] = sq.computeDistance(queries[q], encoded[i],
            VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
        approxIndices[i] = i;
      }
      for (int i = 0; i < k; i++) {
        for (int j = i + 1; j < encoded.length; j++) {
          if (approxDists[j] < approxDists[i]) {
            float tmpD = approxDists[i];
            approxDists[i] = approxDists[j];
            approxDists[j] = tmpD;
            int tmpI = approxIndices[i];
            approxIndices[i] = approxIndices[j];
            approxIndices[j] = tmpI;
          }
        }
      }
      int overlap = 0;
      for (int i = 0; i < k; i++) {
        for (int exact : exactTopK) {
          if (approxIndices[i] == exact) {
            overlap++;
            break;
          }
        }
      }
      totalRecall += (double) overlap / k;
    }
    return totalRecall / queries.length;
  }
}
