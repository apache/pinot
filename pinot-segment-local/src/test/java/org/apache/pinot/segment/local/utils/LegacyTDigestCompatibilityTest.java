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

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.SplittableRandom;
import org.apache.pinot.segment.spi.customobject.TDigest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Exports fresh Pinot bytes for the isolated real t-digest 3.2/3.3 reader gate in the unit-test workflow.
/// This test owns its output directory and does not share mutable state with other test classes.
public class LegacyTDigestCompatibilityTest {
  private static final double[] QUANTILES = {0, 0.25, 0.5, 0.75, 0.99, 1};

  @Test
  public void testExportLegacyReaderFixtures()
      throws Exception {
    Path directory = Path.of(System.getProperty("basedir"), "target", "tdigest-compat-fixtures");
    Files.createDirectories(directory);
    StringBuilder manifest = new StringBuilder();
    int count = 0;
    for (double compression : new double[]{10, 20, 100, 500}) {
      for (String state : new String[]{"empty", "singleton", "seeded", "weighted"}) {
        TDigest digest = TDigestUtils.createMergingDigest(compression);
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
        count += export(directory, manifest, compression + "-" + state, digest, !state.equals("weighted"));
      }
    }
    // Exercise low-compression capacity fallback with exact floats and with non-float double means.
    for (double offset : new double[]{0, 1.0e18}) {
      ByteBuffer verbose = ByteBuffer.allocate(32 + 16 * 51);
      verbose.putInt(1).putDouble(offset).putDouble(offset + 256 * 50).putDouble(20).putInt(51);
      for (int i = 0; i < 51; i++) {
        verbose.putDouble(1).putDouble(offset + 256 * i);
      }
      TDigest digest = TDigestUtils.deserialize(verbose.array());
      writeVerbose(directory, manifest, "capacity-" + offset, digest);
      count++;
    }
    // A compact stored digest can declare more centroids than a default legacy reader allocates.
    ByteBuffer compact = ByteBuffer.allocate(30 + 8 * 600);
    compact.putInt(2).putDouble(0).putDouble(599).putFloat(10);
    compact.putShort((short) 1_000).putShort((short) 5_000).putShort((short) 600);
    for (int i = 0; i < 600; i++) {
      compact.putFloat(1).putFloat(i);
    }
    TDigest digest = TDigestUtils.deserialize(compact.array());
    writeVerbose(directory, manifest, "oversized-compact", digest);
    count++;
    // Externally stored verbose headers below ten must be normalized before a 3.2 reader allocates its arrays.
    ByteBuffer lowCompression = ByteBuffer.allocate(32 + 16 * 25);
    lowCompression.putInt(1).putDouble(0).putDouble(24).putDouble(5).putInt(25);
    for (int i = 0; i < 25; i++) {
      lowCompression.putDouble(1).putDouble(i);
    }
    digest = TDigestUtils.deserialize(lowCompression.array());
    writeVerbose(directory, manifest, "low-compression-header", digest);
    count++;
    ByteBuffer zeroWeight = ByteBuffer.allocate(32 + 16 * 3);
    zeroWeight.putInt(1).putDouble(0).putDouble(10).putDouble(100).putInt(3);
    zeroWeight.putDouble(1).putDouble(0).putDouble(0).putDouble(5).putDouble(1).putDouble(10);
    digest = TDigestUtils.deserialize(zeroWeight.array());
    writeVerbose(directory, manifest, "zero-weight-centroid", digest);
    count++;
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
    byte[] repaired = TDigestUtils.makeLegacyCompatible(
        TDigestUtils.serializeCentroids(100.1, 0.0, 219.0, means, weights, means.length));
    assertEquals(ByteBuffer.wrap(repaired).getInt(28), 211);
    digest = TDigestUtils.deserialize(repaired);
    write(directory, manifest, "zero-weight-capacity-repair", digest, repaired, quantiles(digest), false);
    count++;
    // Compact float fields cannot represent this finite mean; the actual small writer must fall back to verbose.
    digest = TDigestUtils.createMergingDigest(100);
    digest.add(1e100);
    digest.compress();
    double[] hugeExpected = quantiles(digest);
    ByteBuffer hugeSmall = ByteBuffer.allocate(digest.smallByteSize());
    digest.asSmallBytes(hugeSmall);
    write(directory, manifest, "huge-mean-small-fallback", digest, hugeSmall.array(), hugeExpected, true);
    count++;
    Files.writeString(directory.resolve("manifest.tsv"), manifest);
    assertEquals(count, 39);
  }

  private static int export(Path directory, StringBuilder manifest, String name, TDigest digest,
      boolean compareInitialQuantiles)
      throws Exception {
    digest.compress();
    double[] expected = quantiles(digest);
    write(directory, manifest, name + "-verbose", digest, TDigestUtils.serialize(digest), expected,
        compareInitialQuantiles);
    expected = quantiles(digest);
    ByteBuffer compact = ByteBuffer.allocate(digest.smallByteSize());
    digest.asSmallBytes(compact);
    write(directory, manifest, name + "-compact", digest, compact.array(), expected, compareInitialQuantiles);
    return 2;
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
    write(directory, manifest, name, digest, TDigestUtils.serialize(digest), expected, false);
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
