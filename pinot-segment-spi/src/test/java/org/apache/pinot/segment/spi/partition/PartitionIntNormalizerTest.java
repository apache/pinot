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
package org.apache.pinot.segment.spi.partition;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class PartitionIntNormalizerTest {

  @Test
  public void testPositiveModulo() {
    PartitionIntNormalizer n = PartitionIntNormalizer.POSITIVE_MODULO;
    assertEquals(n.getPartitionId(0, 7), 0);
    assertEquals(n.getPartitionId(10, 7), 3);
    assertEquals(n.getPartitionId(-1, 7), 6);
    assertEquals(n.getPartitionId(Integer.MIN_VALUE, 7), shiftedMod(Integer.MIN_VALUE, 7));
    assertEquals(n.getPartitionId(Integer.MAX_VALUE, 1024), Integer.MAX_VALUE % 1024);
    assertEquals(n.getPartitionId(7L, 7), 0);
    assertEquals(n.getPartitionId(Long.MIN_VALUE, 7), shiftedMod(Long.MIN_VALUE, 7));
  }

  @Test
  public void testAbs() {
    PartitionIntNormalizer n = PartitionIntNormalizer.ABS;
    assertEquals(n.getPartitionId(10, 7), 3);
    assertEquals(n.getPartitionId(-10, 7), 3);
    assertEquals(n.getPartitionId(Integer.MIN_VALUE, 7), Math.abs(Integer.MIN_VALUE % 7));
    assertEquals(n.getPartitionId(-10L, 7), 3);
  }

  @Test
  public void testMask() {
    PartitionIntNormalizer n = PartitionIntNormalizer.MASK;
    assertEquals(n.getPartitionId(0, 7), 0);
    assertEquals(n.getPartitionId(-1, 7), Integer.MAX_VALUE % 7);
    assertEquals(n.getPartitionId(Integer.MIN_VALUE, 7), 0);
    assertEquals(n.getPartitionId(Long.MIN_VALUE, 7), 0);
  }

  @Test
  public void testAllNormalizersReturnInRange() {
    int[] partitionCounts = {1, 2, 7, 1024};
    int[] hashes = {0, 1, -1, 7, -7, Integer.MIN_VALUE, Integer.MAX_VALUE, 13, -13};
    for (PartitionIntNormalizer n : PartitionIntNormalizer.values()) {
      for (int p : partitionCounts) {
        for (int h : hashes) {
          int pid = n.getPartitionId(h, p);
          assertTrue(pid >= 0 && pid < p,
              n + " produced out-of-range partition " + pid + " for hash=" + h + ", N=" + p);
        }
      }
    }
  }

  @Test
  public void testFromConfigStringRoundTrip() {
    for (PartitionIntNormalizer n : PartitionIntNormalizer.values()) {
      assertEquals(PartitionIntNormalizer.fromConfigString(n.name()), n);
      assertEquals(PartitionIntNormalizer.fromConfigString(n.name().toLowerCase()), n);
      assertEquals(PartitionIntNormalizer.fromConfigString("  " + n.name() + "  "), n);
    }
  }

  @Test
  public void testFromConfigStringRejectsBlank() {
    expectThrows(IllegalArgumentException.class, () -> PartitionIntNormalizer.fromConfigString(null));
    expectThrows(IllegalArgumentException.class, () -> PartitionIntNormalizer.fromConfigString(""));
    expectThrows(IllegalArgumentException.class, () -> PartitionIntNormalizer.fromConfigString("   "));
  }

  @Test
  public void testFromConfigStringRejectsUnknown() {
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
        () -> PartitionIntNormalizer.fromConfigString("not-a-real-normalizer"));
    assertTrue(e.getMessage().contains("not-a-real-normalizer"));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNonPositivePartitionsRejected() {
    PartitionIntNormalizer.POSITIVE_MODULO.getPartitionId(5, 0);
  }

  private static int shiftedMod(long value, int numPartitions) {
    long m = value % numPartitions;
    return (int) (m < 0 ? m + numPartitions : m);
  }
}
