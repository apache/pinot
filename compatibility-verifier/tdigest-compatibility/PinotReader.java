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

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.pinot.segment.local.customobject.tdigest.TDigestAccumulator;
import org.apache.pinot.segment.local.customobject.tdigest.TDigest;
import org.apache.pinot.segment.local.customobject.tdigest.TDigestCodec;
import org.apache.pinot.segment.local.customobject.tdigest.TDigestCodec.SerializedTDigestMetadata;


/// Checks genuine legacy-written verbose and compact bytes in a JVM containing only Pinot's implementation.
/// Centroid decoding preserves mass and means; quantile parity excludes documented weighted-boundary differences.
public final class PinotReader {
  private static final double[] QUANTILES = {0, 0.25, 0.5, 0.75, 0.99, 1};

  private PinotReader() {
  }

  public static void main(String[] args)
      throws Exception {
    if (!PinotReader.class.desiredAssertionStatus()) {
      throw new IllegalStateException("The legacy compatibility gate requires -ea");
    }
    if (PinotReader.class.getClassLoader().getResource("com/tdunning/math/stats/TDigest.class") != null) {
      throw new IllegalStateException("The Pinot reader classpath must not include tdunning");
    }
    Path directory = Path.of(args[0]);
    List<String> manifest = Files.readAllLines(directory.resolve("manifest.tsv"));
    assert !manifest.isEmpty() : "Missing legacy writer fixtures";
    int quantileCases = 0;
    for (String line : manifest) {
      String[] fields = line.split("\t");
      try {
        byte[] bytes = Files.readAllBytes(directory.resolve(fields[0] + ".bin"));
        ByteBuffer input = ByteBuffer.wrap(bytes);
        SerializedTDigestMetadata metadata = TDigestCodec.inspectSerialized(input);
        int count = metadata.centroidCount();
        assert count == fields.length - 6 - QUANTILES.length : "Centroid count changed";
        double[] means = new double[count];
        double[] weights = new double[count];
        TDigestCodec.decodeSerializedCentroids(ByteBuffer.wrap(bytes), metadata, means, weights);
        double roundingBound = 0;
        for (int i = 0; i < count; i++) {
          String[] centroid = fields[6 + QUANTILES.length + i].split(":");
          double legacyMean = Double.parseDouble(centroid[0]);
          // Compact mean rounding can cross the exact double header extrema; Pinot clamps those endpoints.
          double expectedMean = Math.max(metadata.min(), Math.min(legacyMean, metadata.max()));
          // Legacy Centroid construction multiplies and divides the stored mean by its integer count.
          assert Math.abs(means[i] - expectedMean) <= 8 * Math.ulp(expectedMean)
              : "Centroid mean changed at " + i + ": expected=" + expectedMean + ", actual=" + means[i];
          if (fields[0].endsWith("-compact")) {
            roundingBound = Math.max(roundingBound, 2.0 * Math.ulp((float) legacyMean));
          }
          assert weights[i] == Double.parseDouble(centroid[1]) : "Centroid weight changed";
        }
        TDigest digest = TDigestAccumulator.fromBytes(bytes);
        long size = Long.parseLong(fields[1]);
        double min = Double.parseDouble(fields[2]);
        double max = Double.parseDouble(fields[3]);
        double compression = Double.parseDouble(fields[4]);
        verify(digest, size, min, max, compression);
        if (Boolean.parseBoolean(fields[5])) {
          for (int i = 0; i < QUANTILES.length; i++) {
            double expected = Double.parseDouble(fields[6 + i]);
            double actual = digest.quantile(QUANTILES[i]);
            double tolerance = Math.max(roundingBound, 8 * Math.ulp(expected));
            assert Double.isNaN(expected) ? Double.isNaN(actual) : Math.abs(actual - expected) <= tolerance
                : "Initial p" + QUANTILES[i] * 100 + " changed: expected=" + expected + ", actual=" + actual;
          }
          quantileCases++;
        }
        digest.add(1);
        verify(TDigestAccumulator.fromBytes(digest.serialize()), size + 1, Math.min(min, 1),
            Math.max(max, 1), compression);
      } catch (AssertionError | Exception e) {
        throw new AssertionError("Pinot reader failed for legacy " + args[1] + " " + fields[0], e);
      }
    }
    assert quantileCases > 0 : "Missing initial legacy quantile comparisons";
    System.out.println("Pinot reader of legacy " + args[1] + ": " + manifest.size()
        + " decode/add/rewrite cases passed, " + quantileCases + " initial quantile comparisons");
  }

  private static void verify(TDigest digest, long size, double min, double max, double compression) {
    assert digest.hasValidStatistics() : "Healthy legacy distribution became degraded";
    assert digest.getTotalWeight() == size : "Mass changed";
    assert digest.getMin() == min : "Minimum changed";
    assert digest.getMax() == max : "Maximum changed";
    assert digest.compression() == compression : "Compression changed";
    double previous = Double.NEGATIVE_INFINITY;
    for (double quantile : QUANTILES) {
      double value = digest.quantile(quantile);
      assert Double.isFinite(value) && value >= previous && value >= min && value <= max
          : "Quantiles must be finite, bounded and monotone";
      previous = value;
    }
  }
}
