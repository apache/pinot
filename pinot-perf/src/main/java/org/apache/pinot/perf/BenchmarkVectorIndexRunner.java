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
 * Compatibility entry point for the vector benchmark suite's {@code quick} mode.
 * Runs a small-scale validation before a full benchmark run.
 *
 * <p>Prefer {@link BenchmarkVectorIndex} with
 * {@code -Dpinot.perf.vector.mode=quick}. This class remains so older scripts continue to work.</p>
 */
public final class BenchmarkVectorIndexRunner {

  private BenchmarkVectorIndexRunner() {
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
    out.println("=== Quick validation run ===");

    int n = 5000;
    int dim = 128;
    VectorIndexConfig.VectorDistanceFunction distFunc = VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN;

    float[][] corpus = BenchmarkVectorIndex.generateGaussianVectors(n, dim, 42L);
    float[][] queries = BenchmarkVectorIndex.generateGaussianVectors(100, dim, 1042L);

    // Ground truth
    int[][] gt10 = BenchmarkVectorIndex.computeGroundTruth(corpus, queries, 10, distFunc);

    // Sanity: exact scan
    double exactRecall = 0;
    for (int q = 0; q < queries.length; q++) {
      int[] exact = BenchmarkVectorIndex.exactTopK(corpus, queries[q], 10, distFunc);
      Set<Integer> exactSet = new HashSet<>();
      for (int id : exact) {
        exactSet.add(id);
      }
      exactRecall += BenchmarkVectorIndex.computeRecall(gt10[q], exactSet);
    }
    exactRecall /= queries.length;
    out.printf("Exact scan recall@10: %.4f (expected 1.0)%n", exactRecall);

    // IVF_FLAT with nlist=64
    int nlist = 64;
    File ivfDir = Files.createTempDirectory("bench_validate_").toFile();
    try {
      BenchmarkVectorIndex.buildIvfFlatIndex(ivfDir, corpus, dim, nlist, distFunc);

      for (int nprobe : new int[]{1, 4, 8, 16, 32, 64}) {
        try (IvfFlatVectorIndexReader reader =
            BenchmarkVectorIndex.openIvfReader(ivfDir, dim, nlist, nprobe, distFunc)) {
          reader.setNprobe(nprobe);

          double recall = 0;
          for (int q = 0; q < queries.length; q++) {
            Set<Integer> r = BenchmarkVectorIndex.bitmapToSet(reader.getDocIds(queries[q], 10));
            recall += BenchmarkVectorIndex.computeRecall(gt10[q], r);
          }
          recall /= queries.length;

          long[] latencies = BenchmarkVectorIndex.measureIvfLatencies(reader, queries, 10);
          Arrays.sort(latencies);

          out.printf("IVF_FLAT nlist=%d nprobe=%d  recall@10=%.4f  p50=%.1fus  p99=%.1fus%n",
              nlist, nprobe, recall,
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
          BenchmarkVectorIndex.createIvfPqConfig(dim, corpus.length, nlist, pqM, pqNbits, distFunc);
      File pqDir = Files.createTempDirectory("bench_validate_pq_").toFile();
      try {
        BenchmarkVectorIndex.buildVectorIndexReflectively(ivfPqCreator, pqDir, corpus, pqConfig);

        for (int nprobe : new int[]{1, 4, 8, 16, 32, 64}) {
          try (BenchmarkVectorIndex.ReflectiveVectorReader reader = BenchmarkVectorIndex.openReflectiveVectorReader(
              ivfPqReader, pqDir, corpus.length, pqConfig)) {
            reader.setNprobe(nprobe);

            double recall = 0;
            for (int q = 0; q < queries.length; q++) {
              Set<Integer> r = BenchmarkVectorIndex.bitmapToSet(reader.getDocIds(queries[q], 10));
              recall += BenchmarkVectorIndex.computeRecall(gt10[q], r);
            }
            recall /= queries.length;

            long[] latencies = BenchmarkVectorIndex.measureReflectiveLatencies(reader, queries, 10);
            Arrays.sort(latencies);

            out.printf("IVF_PQ nlist=%d nprobe=%d pqM=%d pqNbits=%d  recall@10=%.4f  p50=%.1fus  p99=%.1fus%n",
                nlist, nprobe, pqM, pqNbits, recall,
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
