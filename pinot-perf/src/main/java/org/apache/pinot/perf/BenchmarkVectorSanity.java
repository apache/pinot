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
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.readers.vector.IvfFlatVectorIndexReader;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;


/**
 * Fast sanity check for the vector benchmark suite.
 *
 * <p>This mode intentionally stays small so benchmark changes can be smoke-tested before running
 * the heavier workload suites.</p>
 */
public final class BenchmarkVectorSanity {
  private static final int NUM_VECTORS =
      Integer.getInteger("pinot.perf.vector.sanity.size", 5_000);
  private static final int DIMENSION =
      Integer.getInteger("pinot.perf.vector.sanity.dimension", 128);
  private static final int NUM_QUERIES =
      Integer.getInteger("pinot.perf.vector.sanity.queries", 100);
  private static final int TOP_K =
      Integer.getInteger("pinot.perf.vector.sanity.topK", 10);
  private static final int NLIST =
      Integer.getInteger("pinot.perf.vector.sanity.nlist", 64);
  private static final int[] NPROBE_VALUES =
      BenchmarkVectorIndex.parseIntListProperty("pinot.perf.vector.sanity.nprobe", new int[]{1, 4, 8, 16, 32, 64});

  private BenchmarkVectorSanity() {
  }

  /**
   * Validates exact scan recall, IVF_FLAT recall at various nprobe, and IVF_PQ when available.
   */
  public static void main(String[] args)
      throws Exception {
    run(System.out);
  }

  static void run(PrintStream out)
      throws Exception {
    out.println("=== Sanity validation run ===");
    VectorIndexConfig.VectorDistanceFunction distFunc = VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN;

    float[][] corpus = BenchmarkVectorIndex.generateGaussianVectors(NUM_VECTORS, DIMENSION, 42L);
    float[][] queries = BenchmarkVectorIndex.generateGaussianVectors(NUM_QUERIES, DIMENSION, 1042L);

    // Ground truth
    int[][] gt10 = BenchmarkVectorIndex.computeGroundTruth(corpus, queries, TOP_K, distFunc);

    // Sanity: exact scan
    double exactRecall = 0;
    for (int q = 0; q < queries.length; q++) {
      int[] exact = BenchmarkVectorIndex.exactTopK(corpus, queries[q], TOP_K, distFunc);
      Set<Integer> exactSet = new HashSet<>();
      for (int id : exact) {
        exactSet.add(id);
      }
      exactRecall += BenchmarkVectorIndex.computeRecall(gt10[q], exactSet);
    }
    exactRecall /= queries.length;
    out.printf("Exact scan recall@10: %.4f (expected 1.0)%n", exactRecall);

    File ivfDir = Files.createTempDirectory("bench_validate_").toFile();
    try {
      BenchmarkVectorIndex.buildIvfFlatIndex(ivfDir, corpus, DIMENSION, NLIST, distFunc);

      for (int nprobe : NPROBE_VALUES) {
        if (nprobe > NLIST) {
          continue;
        }
        try (IvfFlatVectorIndexReader reader =
            BenchmarkVectorIndex.openIvfReader(ivfDir, DIMENSION, NLIST, nprobe, distFunc)) {
          reader.setNprobe(nprobe);

          double recall = 0;
          for (int q = 0; q < queries.length; q++) {
            Set<Integer> r = BenchmarkVectorIndex.bitmapToSet(reader.getDocIds(queries[q], TOP_K));
            recall += BenchmarkVectorIndex.computeRecall(gt10[q], r);
          }
          recall /= queries.length;

          long[] latencies = BenchmarkVectorIndex.measureIvfLatencies(reader, queries, TOP_K);
          Arrays.sort(latencies);

          out.printf("IVF_FLAT nlist=%d nprobe=%d  recall@10=%.4f  p50=%.1fus  p99=%.1fus%n",
              NLIST, nprobe, recall,
              BenchmarkVectorIndex.percentile(latencies, 50) / 1000.0,
              BenchmarkVectorIndex.percentile(latencies, 99) / 1000.0);
        }
      }
    } finally {
      FileUtils.deleteQuietly(ivfDir);
    }

    // Optional IVF_PQ validation, gated on backend availability.
    String ivfPqCreator = "org.apache.pinot.segment.local.segment.index.vector.IvfPqVectorIndexCreator";
    String ivfPqReader = "org.apache.pinot.segment.local.segment.index.readers.vector.IvfPqVectorIndexReader";
    if (BenchmarkVectorIndex.isClassAvailable(ivfPqCreator) && BenchmarkVectorIndex.isClassAvailable(ivfPqReader)) {
      int pqM = 16;
      int pqNbits = 8;
      VectorIndexConfig pqConfig =
          BenchmarkVectorIndex.createIvfPqConfig(DIMENSION, corpus.length, NLIST, pqM, pqNbits, distFunc);
      File pqDir = Files.createTempDirectory("bench_validate_pq_").toFile();
      try {
        BenchmarkVectorIndex.buildVectorIndexReflectively(ivfPqCreator, pqDir, corpus, pqConfig);

        for (int nprobe : NPROBE_VALUES) {
          if (nprobe > NLIST) {
            continue;
          }
          try (BenchmarkVectorIndex.ReflectiveVectorReader reader = BenchmarkVectorIndex.openReflectiveVectorReader(
              ivfPqReader, pqDir, corpus.length, pqConfig)) {
            reader.setNprobe(nprobe);

            double recall = 0;
            for (int q = 0; q < queries.length; q++) {
              Set<Integer> r = BenchmarkVectorIndex.bitmapToSet(reader.getDocIds(queries[q], TOP_K));
              recall += BenchmarkVectorIndex.computeRecall(gt10[q], r);
            }
            recall /= queries.length;

            long[] latencies = BenchmarkVectorIndex.measureReflectiveLatencies(reader, queries, TOP_K);
            Arrays.sort(latencies);

            out.printf("IVF_PQ nlist=%d nprobe=%d pqM=%d pqNbits=%d  recall@10=%.4f  p50=%.1fus  p99=%.1fus%n",
                NLIST, nprobe, pqM, pqNbits, recall,
                BenchmarkVectorIndex.percentile(latencies, 50) / 1000.0,
                BenchmarkVectorIndex.percentile(latencies, 99) / 1000.0);
          }
        }
      } finally {
        FileUtils.deleteQuietly(pqDir);
      }
    } else {
      out.println("IVF_PQ validation skipped: backend classes are not available in this checkout");
    }

    out.println("Validation complete.");
  }
}
