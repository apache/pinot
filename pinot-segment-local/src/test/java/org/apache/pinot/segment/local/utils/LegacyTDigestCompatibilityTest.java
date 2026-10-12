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
package org.apache.pinot.segment.local.utils;

import com.google.common.util.concurrent.RateLimiter;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.SplittableRandom;
import org.apache.pinot.segment.local.customobject.tdigest.TDigestAccumulator;
import org.apache.pinot.segment.local.customobject.tdigest.TDigest;
import org.apache.pinot.segment.local.customobject.tdigest.TDigestCodec;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


/// Exports fresh Pinot bytes for the isolated real t-digest 3.2/3.3 reader gate in the unit-test workflow.
/// This test owns its output directory and does not share mutable state with other test classes.
public class LegacyTDigestCompatibilityTest {
  private static final double[] QUANTILES = {0, 0.25, 0.5, 0.75, 0.99, 1};

  @Test
  public void testExportLegacyReaderFixtures()
      throws Exception {
    Path directory = Path.of(System.getProperty("basedir", "."), "target", "tdigest-compat-fixtures");
    Files.createDirectories(directory);
    StringBuilder manifest = new StringBuilder();
    for (double compression : new double[]{10, 20, 100, 500}) {
      for (String state : new String[]{"empty", "singleton", "seeded", "weighted"}) {
        TDigest digest = TDigestAccumulator.forLegacyAggregation(compression);
        switch (state) {
          case "singleton":
            digest.add(3.25);
            break;
          case "seeded":
            SplittableRandom random = new SplittableRandom(42);
            for (int i = 0; i < 5_000; i++) {
              digest.add(Math.pow(random.nextDouble(), 8));
            }
            break;
          case "weighted":
            digest.add(-10, 7);
            digest.add(0, 3);
            digest.add(50, 11);
            break;
          default:
            break;
        }
        export(directory, manifest, compression + "-" + state, digest, !state.equals("weighted"));
      }
    }
    // Exercise low-compression capacity fallback with exact floats and with non-float double means.
    for (double offset : new double[]{0, 1.0e18}) {
      ByteBuffer verbose = ByteBuffer.allocate(32 + 16 * 51);
      verbose.putInt(1).putDouble(offset).putDouble(offset + 256 * 50).putDouble(20).putInt(51);
      for (int i = 0; i < 51; i++) {
        verbose.putDouble(1).putDouble(offset + 256 * i);
      }
      TDigest digest = TDigestAccumulator.fromBytes(verbose.array());
      writeVerbose(directory, manifest, "capacity-" + offset, digest);
    }
    // A compact stored digest can declare more centroids than a default legacy reader allocates.
    ByteBuffer compact = ByteBuffer.allocate(30 + 8 * 600);
    compact.putInt(2).putDouble(0).putDouble(599).putFloat(10);
    compact.putShort((short) 1_000).putShort((short) 5_000).putShort((short) 600);
    for (int i = 0; i < 600; i++) {
      compact.putFloat(1).putFloat(i);
    }
    TDigest digest = TDigestAccumulator.fromBytes(compact.array());
    writeVerbose(directory, manifest, "oversized-compact", digest);
    // Externally stored verbose headers below ten must be normalized before a 3.2 reader allocates its arrays.
    ByteBuffer lowCompression = ByteBuffer.allocate(32 + 16 * 25);
    lowCompression.putInt(1).putDouble(0).putDouble(24).putDouble(5).putInt(25);
    for (int i = 0; i < 25; i++) {
      lowCompression.putDouble(1).putDouble(i);
    }
    digest = TDigestAccumulator.fromBytes(lowCompression.array());
    writeVerbose(directory, manifest, "low-compression-header", digest);
    ByteBuffer zeroWeight = ByteBuffer.allocate(32 + 16 * 3);
    zeroWeight.putInt(1).putDouble(0).putDouble(10).putDouble(100).putInt(3);
    zeroWeight.putDouble(1).putDouble(0).putDouble(0).putDouble(5).putDouble(1).putDouble(10);
    digest = TDigestAccumulator.fromBytes(zeroWeight.array());
    writeVerbose(directory, manifest, "zero-weight-centroid", digest);
    // Repair adjacent zero-mass NaN means before reducing to the smaller fractional-compression reader capacity.
    double[] means = new double[320];
    double[] weights = new double[means.length];
    means[0] = 0.0;
    weights[0] = 1.0;
    for (int i = 1; i <= 100; i++) {
      means[i] = Double.NaN;
    }
    for (int i = 1; i < 220; i++) {
      means[i + 100] = i;
      weights[i + 100] = 1.0;
    }
    byte[] repaired = TDigestCodec.makeLegacyCompatible(
        TDigestCodec.serializeCentroids(100.1, 0.0, 219.0, means, weights, means.length));
    assertEquals(ByteBuffer.wrap(repaired).getInt(28), 211);
    digest = TDigestAccumulator.fromBytes(repaired);
    write(directory, manifest, "zero-weight-capacity-repair", digest, repaired, quantiles(digest), false);
    // Compact fixture fields cannot represent this finite mean; retain the native verbose serialization.
    digest = TDigestAccumulator.forLegacyAggregation(100);
    digest.add(1e100);
    digest.compress();
    double[] hugeExpected = quantiles(digest);
    write(directory, manifest, "huge-mean-small-fallback", digest, TDigestTestUtils.compactFixture(digest),
        hugeExpected, true);
    // Opaque degraded state retains raw headers even when an old reader cannot fit its centroid count.
    for (int compression : new int[]{100, 5}) {
      int centroidCount = compression == 100 ? 300 : 25;
      ByteBuffer degraded = ByteBuffer.allocate(32 + 16 * centroidCount);
      degraded.putInt(1).putDouble(0).putDouble(centroidCount - 1).putDouble(compression).putInt(centroidCount);
      for (int i = 0; i < centroidCount; i++) {
        degraded.putDouble(1).putDouble(i == centroidCount / 2 ? Double.NaN : i);
      }
      assertEquals(TDigestCodec.makeLegacyCompatible(degraded.array()), degraded.array());
      digest = TDigestAccumulator.fromBytes(degraded.array());
      assertFalse(digest.hasValidStatistics());
      byte[] preserved = digest.serialize();
      assertEquals(preserved, degraded.array());
      String name = compression == 100 ? "degraded-over-capacity" : "degraded-low-compression-header";
      write(directory, manifest, name, digest, preserved, quantiles(digest), false);
    }
    Files.writeString(directory.resolve("manifest.tsv"), manifest);
    Files.writeString(directory.resolve("pinot-reader-classpath.txt"), String.join(File.pathSeparator,
        Path.of(LoggerFactory.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toString(),
        Path.of(RateLimiter.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toString()));
  }

  private static void export(Path directory, StringBuilder manifest, String name, TDigest digest,
      boolean compareInitialQuantiles)
      throws Exception {
    digest.compress();
    double[] expected = quantiles(digest);
    write(directory, manifest, name + "-verbose", digest, digest.serialize(), expected,
        compareInitialQuantiles);
    expected = quantiles(digest);
    write(directory, manifest, name + "-compact", digest, TDigestTestUtils.compactFixture(digest), expected,
        compareInitialQuantiles);
  }

  private static double[] quantiles(TDigest digest) {
    double[] values = new double[QUANTILES.length];
    for (int i = 0; i < values.length; i++) {
      values[i] = digest.quantile(QUANTILES[i]);
    }
    return values;
  }

  private static void writeVerbose(Path directory, StringBuilder manifest, String name, TDigest digest)
      throws Exception {
    digest.compress();
    double[] expected = quantiles(digest);
    // Boundary/capacity repairs and historical 3.2 singleton interpolation can intentionally change quantile values.
    write(directory, manifest, name, digest, digest.serialize(), expected, false);
  }

  private static void write(Path directory, StringBuilder manifest, String name, TDigest digest, byte[] bytes,
      double[] expectedQuantiles, boolean compareInitialQuantiles)
      throws Exception {
    Files.write(directory.resolve(name + ".bin"), bytes);
    manifest.append(name).append('\t').append(digest.size()).append('\t').append(digest.getMin()).append('\t')
        .append(digest.getMax()).append('\t').append(digest.compression()).append('\t').append(compareInitialQuantiles);
    for (double value : expectedQuantiles) {
      manifest.append('\t').append(value);
    }
    manifest.append('\n');
  }
}
