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
        count += export(directory, manifest, compression + "-" + state, digest);
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
      write(directory, manifest, "capacity-" + offset, digest, TDigestUtils.serialize(digest));
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
    write(directory, manifest, "oversized-compact", digest, TDigestUtils.serialize(digest));
    count++;
    // Externally stored verbose headers below ten must be normalized before a 3.2 reader allocates its arrays.
    ByteBuffer lowCompression = ByteBuffer.allocate(32 + 16 * 25);
    lowCompression.putInt(1).putDouble(0).putDouble(24).putDouble(5).putInt(25);
    for (int i = 0; i < 25; i++) {
      lowCompression.putDouble(1).putDouble(i);
    }
    digest = TDigestUtils.deserialize(lowCompression.array());
    write(directory, manifest, "low-compression-header", digest, TDigestUtils.serialize(digest));
    count++;
    ByteBuffer zeroWeight = ByteBuffer.allocate(32 + 16 * 3);
    zeroWeight.putInt(1).putDouble(0).putDouble(10).putDouble(100).putInt(3);
    zeroWeight.putDouble(1).putDouble(0).putDouble(0).putDouble(5).putDouble(1).putDouble(10);
    digest = TDigestUtils.deserialize(zeroWeight.array());
    write(directory, manifest, "zero-weight-centroid", digest, TDigestUtils.serialize(digest));
    count++;
    Files.writeString(directory.resolve("manifest.tsv"), manifest);
    assertEquals(count, 37);
  }

  private static int export(Path directory, StringBuilder manifest, String name, TDigest digest)
      throws Exception {
    write(directory, manifest, name + "-verbose", digest, TDigestUtils.serialize(digest));
    ByteBuffer compact = ByteBuffer.allocate(digest.smallByteSize());
    digest.asSmallBytes(compact);
    write(directory, manifest, name + "-compact", digest, compact.array());
    return 2;
  }

  private static void write(Path directory, StringBuilder manifest, String name, TDigest digest, byte[] bytes)
      throws Exception {
    Files.write(directory.resolve(name + ".bin"), bytes);
    manifest.append(name).append('\t').append(digest.size()).append('\t').append(digest.getMin()).append('\t')
        .append(digest.getMax()).append('\t').append(digest.compression()).append('\n');
  }
}
