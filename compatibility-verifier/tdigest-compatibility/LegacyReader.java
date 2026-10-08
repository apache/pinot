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

import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;


/// Runs only against one legacy jar in a separate JVM; Pinot classes are deliberately absent from its classpath.
/// Fractional endpoint masses below one and singleton masses between one and two are excluded: legacy 3.3 cannot
/// recompress them without changing mass. Those preserved-byte limitations are tested by Pinot's unit tests.
public final class LegacyReader {
  private LegacyReader() {
  }

  public static void main(String[] args)
      throws Exception {
    if (!LegacyReader.class.desiredAssertionStatus()) {
      throw new IllegalStateException("The legacy compatibility gate requires -ea");
    }
    Path directory = Path.of(args[0]);
    List<String> manifest = Files.readAllLines(directory.resolve("manifest.tsv"));
    assert manifest.size() == 35 : "Missing compatibility fixtures";
    for (String line : manifest) {
      String[] fields = line.split("\t");
      try {
        TDigest digest = MergingDigest.fromBytes(ByteBuffer.wrap(Files.readAllBytes(
            directory.resolve(fields[0] + ".bin"))));
        long size = Long.parseLong(fields[1]);
        double min = Double.parseDouble(fields[2]);
        double max = Double.parseDouble(fields[3]);
        double compression = Double.parseDouble(fields[4]);
        verify(digest, size, min, max, compression, true);
        digest.add(1);
        digest.compress();
        verify(digest, size + 1, Math.min(min, 1), Math.max(max, 1), compression, false);
        if (args[1].equals("3.3")) {
          List<Centroid> centroids = new ArrayList<>(digest.centroids());
          if (centroids.size() > 1) {
            assert centroids.get(0).count() == 1 : "First centroid must have unit weight";
            assert centroids.get(centroids.size() - 1).count() == 1 : "Last centroid must have unit weight";
          }
        }
        ByteBuffer rewritten = ByteBuffer.allocate(digest.byteSize());
        digest.asBytes(rewritten);
        verify(MergingDigest.fromBytes(ByteBuffer.wrap(rewritten.array())), size + 1,
            Math.min(min, 1), Math.max(max, 1), compression, false);
      } catch (AssertionError | Exception e) {
        throw new AssertionError("Legacy " + args[1] + " reader failed for " + fields[0], e);
      }
    }
    System.out.println("Legacy " + args[1] + ": " + manifest.size() + " read/add/compress/rewrite cases passed");
  }

  private static void verify(TDigest digest, long size, double min, double max, double compression,
      boolean exactExtrema) {
    assert digest.size() == size : "Mass changed";
    assert sameExtremum(digest.getMin(), min, exactExtrema) : "Minimum changed";
    assert sameExtremum(digest.getMax(), max, exactExtrema) : "Maximum changed";
    assert digest.compression() == compression : "Compression changed";
    double previous = Double.NEGATIVE_INFINITY;
    for (double quantile : new double[]{0, 0.01, 0.25, 0.5, 0.75, 0.99, 1}) {
      double value = digest.quantile(quantile);
      if (size == 0) {
        assert Double.isNaN(value) : "Empty quantile must be NaN";
      } else {
        assert Double.isFinite(value) && value >= previous : "Quantiles must be finite and monotone";
        previous = value;
      }
    }
  }

  private static boolean sameExtremum(double actual, double expected, boolean exact) {
    // The initial read must preserve header doubles. Legacy recompression can use rounded compact means instead.
    return Double.compare(actual, expected) == 0
        || !exact && Math.abs(actual - expected) <= Math.max(1e-12, Math.abs(expected) * 1e-6);
  }
}
