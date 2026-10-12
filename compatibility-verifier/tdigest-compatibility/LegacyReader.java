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
import java.util.SplittableRandom;


/// Runs only against one legacy jar in a separate JVM; Pinot classes are deliberately absent from its classpath.
/// Fractional endpoint masses below one and singleton masses between one and two are excluded: legacy 3.3 cannot
/// recompress them without changing mass. Those preserved-byte limitations are tested by Pinot's unit tests.
/// Initial value parity covers ordinary continuous/empty/singleton inputs and huge-mean fallback. Weighted endpoint
/// or capacity repair can change interpolation, and 3.2 has older singleton rules; those fixtures retain structural,
/// mass, extrema, and monotonicity checks rather than widening the numerical tolerance.
/// Degraded fixtures characterize oversized and sub-10-header decoding in both legacy readers; preserving opaque
/// bytes does not guarantee reverse-upgrade compatibility.
public final class LegacyReader {
  private static final double[] QUANTILES = {0, 0.25, 0.5, 0.75, 0.99, 1};

  private LegacyReader() {
  }

  public static void main(String[] args)
      throws Exception {
    if (!LegacyReader.class.desiredAssertionStatus()) {
      throw new IllegalStateException("The legacy compatibility gate requires -ea");
    }
    Path directory = Path.of(args[0]);
    List<String> manifest = Files.readAllLines(directory.resolve("manifest.tsv"));
    assert !manifest.isEmpty() : "Missing compatibility fixtures";
    Path output = directory.resolve("legacy-" + args[1]);
    Files.createDirectories(output);
    StringBuilder reverseManifest = new StringBuilder();
    int nativeQuantileCases = 0;
    int degradedCases = 0;
    for (String line : manifest) {
      String[] fields = line.split("\t");
      try {
        byte[] bytes = Files.readAllBytes(directory.resolve(fields[0] + ".bin"));
        if (fields[0].equals("degraded-over-capacity")) {
          verifyDegradedDecodeFailure(bytes);
          degradedCases++;
          continue;
        }
        if (fields[0].equals("degraded-low-compression-header")) {
          if (args[1].equals("3.2")) {
            verifyDegradedDecodeFailure(bytes);
          } else {
            TDigest degraded = MergingDigest.fromBytes(ByteBuffer.wrap(bytes));
            assert degraded.compression() == 10 : "Legacy 3.3 must clamp before allocating";
            assert degraded.size() == 25 && degraded.centroidCount() == 25 : "Lost degraded centroids";
            assert degraded.centroids().stream().anyMatch(c -> Double.isNaN(c.mean())) : "Lost opaque NaN mean";
          }
          degradedCases++;
          continue;
        }
        TDigest digest = MergingDigest.fromBytes(ByteBuffer.wrap(bytes));
        long size = Long.parseLong(fields[1]);
        double min = Double.parseDouble(fields[2]);
        double max = Double.parseDouble(fields[3]);
        double compression = Double.parseDouble(fields[4]);
        verify(digest, size, min, max, compression, true);
        if (Boolean.parseBoolean(fields[5])) {
          verifyInitialQuantiles(digest, fields, compactRoundingBound(bytes));
          nativeQuantileCases++;
        }
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
        // Version 3.2 interpolates a two-point digest differently from Pinot and 3.3.
        export(output, reverseManifest, fields[0] + "-rewritten", digest,
            Boolean.parseBoolean(fields[5]) && digest.size() != 2);
      } catch (AssertionError | Exception e) {
        throw new AssertionError("Legacy " + args[1] + " reader failed for " + fields[0], e);
      }
    }
    assert nativeQuantileCases > 0 : "Missing initial native quantile comparisons";
    assert degradedCases == 2 : "Missing degraded reverse-upgrade cases";
    // Construct inputs in the historical library too, rather than only rewriting Pinot-normalized endpoints.
    MergingDigest weighted = new MergingDigest(100);
    if (args[1].equals("3.3")) {
      weighted.add(-11);
      weighted.add(51);
    }
    weighted.add(-10, 7);
    weighted.add(0, 3);
    weighted.add(50, 11);
    export(output, reverseManifest, "legacy-weighted", weighted, false);
    // Custom backing arrays and merged header extrema are both written by the legacy implementation.
    MergingDigest merged = new MergingDigest(20, 5_000, 1_000);
    MergingDigest source = new MergingDigest(20);
    SplittableRandom random = new SplittableRandom(42);
    for (int i = 0; i < 5_000; i++) {
      merged.add(random.nextDouble());
      source.add(10 + random.nextDouble());
    }
    merged.add(source);
    export(output, reverseManifest, "legacy-custom-capacity-merge", merged, true);
    Files.writeString(output.resolve("manifest.tsv"), reverseManifest);
    System.out.println("Legacy " + args[1] + ": " + (manifest.size() - degradedCases)
        + " read/add/compress/rewrite cases passed, " + nativeQuantileCases + " initial native quantile comparisons, "
        + degradedCases + " degraded reverse-upgrade cases verified");
  }

  private static void verifyDegradedDecodeFailure(byte[] bytes) {
    // Pinot retains this opaque payload unchanged; the old reader allocates fewer entries than its centroid count.
    try {
      MergingDigest.fromBytes(ByteBuffer.wrap(bytes));
    } catch (ArrayIndexOutOfBoundsException e) {
      StackTraceElement origin = e.getStackTrace()[0];
      assert origin.getClassName().equals(MergingDigest.class.getName())
          && origin.getMethodName().equals("fromBytes") : "Unexpected array-bounds failure origin";
      return;
    }
    throw new AssertionError("Expected legacy decoding to reject the preserved oversized degraded payload");
  }

  private static void export(Path output, StringBuilder manifest, String name, TDigest digest,
      boolean compareQuantiles)
      throws Exception {
    digest.compress();
    for (boolean compact : new boolean[]{false, true}) {
      if (compact && digest.centroids().stream().anyMatch(c -> !Float.isFinite((float) c.mean()))) {
        continue;
      }
      ByteBuffer encoded = ByteBuffer.allocate(compact ? digest.smallByteSize() : digest.byteSize());
      if (compact) {
        digest.asSmallBytes(encoded);
      } else {
        digest.asBytes(encoded);
      }
      String file = name + (compact ? "-compact" : "-verbose");
      Files.write(output.resolve(file + ".bin"), encoded.array());
      // Expected values come from the real legacy decoder, including float rounding in compact encodings.
      TDigest decoded = MergingDigest.fromBytes(ByteBuffer.wrap(encoded.array()));
      manifest.append(file).append('\t').append(decoded.size()).append('\t').append(decoded.getMin())
          .append('\t').append(decoded.getMax()).append('\t').append(decoded.compression())
          .append('\t').append(compareQuantiles);
      for (double quantile : QUANTILES) {
        manifest.append('\t').append(decoded.quantile(quantile));
      }
      for (Centroid centroid : decoded.centroids()) {
        manifest.append('\t').append(centroid.mean()).append(':').append(centroid.count());
      }
      manifest.append('\n');
    }
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

  private static void verifyInitialQuantiles(TDigest digest, String[] fields, double roundingBound) {
    assert fields.length == 6 + QUANTILES.length : "Missing native quantile expectations";
    for (int i = 0; i < QUANTILES.length; i++) {
      double expected = Double.parseDouble(fields[6 + i]);
      double actual = digest.quantile(QUANTILES[i]);
      double tolerance = Math.max(roundingBound, 8 * Math.ulp(expected));
      assert Double.isNaN(expected) ? Double.isNaN(actual) : Math.abs(actual - expected) <= tolerance
          : "Initial p" + QUANTILES[i] * 100 + " changed: expected=" + expected + ", actual=" + actual
              + ", tolerance=" + tolerance;
    }
  }

  private static double compactRoundingBound(byte[] bytes) {
    // Fixture weights are exact integers. Interpolation can then move only by float rounding of centroid means.
    ByteBuffer encoded = ByteBuffer.wrap(bytes);
    if (encoded.getInt() != 2) {
      return 0;
    }
    encoded.position(28);
    int count = encoded.getShort();
    double bound = 0;
    for (int i = 0; i < count; i++) {
      float weight = encoded.getFloat();
      assert weight == Math.rint(weight) : "This rounding bound requires exact integer fixture weights";
      float mean = encoded.getFloat();
      bound = Math.max(bound, 2.0 * Math.ulp(mean));
    }
    return bound;
  }

  private static boolean sameExtremum(double actual, double expected, boolean exact) {
    // The initial read must preserve header doubles. Legacy recompression can use rounded compact means instead.
    return Double.compare(actual, expected) == 0
        || !exact && Math.abs(actual - expected) <= Math.max(1e-12, Math.abs(expected) * 1e-6);
  }
}
