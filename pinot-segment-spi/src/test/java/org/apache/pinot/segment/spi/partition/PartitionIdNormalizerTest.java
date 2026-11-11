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


public class PartitionIdNormalizerTest {

  @Test
  public void testPositiveModulo() {
    PartitionIdNormalizer n = PartitionIdNormalizer.POSITIVE_MODULO;
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
    PartitionIdNormalizer n = PartitionIdNormalizer.ABS;
    assertEquals(n.getPartitionId(10, 7), 3);
    assertEquals(n.getPartitionId(-10, 7), 3);
    assertEquals(n.getPartitionId(Integer.MIN_VALUE, 7), Math.abs(Integer.MIN_VALUE % 7));
    assertEquals(n.getPartitionId(-10L, 7), 3);
  }

  @Test
  public void testMask() {
    PartitionIdNormalizer n = PartitionIdNormalizer.MASK;
    assertEquals(n.getPartitionId(0, 7), 0);
    assertEquals(n.getPartitionId(-1, 7), Integer.MAX_VALUE % 7);
    assertEquals(n.getPartitionId(Integer.MIN_VALUE, 7), 0);
    assertEquals(n.getPartitionId(Long.MIN_VALUE, 7), 0);
  }

  @Test
  public void testPreModuloAbs() {
    PartitionIdNormalizer n = PartitionIdNormalizer.PRE_MODULO_ABS;
    // Plain positive / negative: matches abs(hash) % N.
    assertEquals(n.getPartitionId(10, 7), 10 % 7);
    assertEquals(n.getPartitionId(-10, 7), 10 % 7);
    // The defining corner case: Math.abs(MIN_VALUE) overflows back to MIN_VALUE, so PRE_MODULO_ABS
    // patches that to 0 before the modulo.
    assertEquals(n.getPartitionId(Integer.MIN_VALUE, 7), 0);
    assertEquals(n.getPartitionId(Long.MIN_VALUE, 7), 0);
    // For non-MIN_VALUE inputs, PRE_MODULO_ABS and (Math.abs(value) % N) agree.
    assertEquals(n.getPartitionId(Integer.MAX_VALUE, 1024), Integer.MAX_VALUE % 1024);
    assertEquals(n.getPartitionId(-1234L, 17), 1234 % 17);
  }

  @Test
  public void testNoOpIsIdentity() {
    PartitionIdNormalizer n = PartitionIdNormalizer.NO_OP;
    // Identity for every input — the caller is asserting the value is already in [0, N).
    assertEquals(n.getPartitionId(0, 7), 0);
    assertEquals(n.getPartitionId(3, 7), 3);
    // The framework does NOT validate the input is in range; passing out-of-range is the
    // caller's responsibility.
    assertEquals(n.getPartitionId(-5, 7), -5);
    assertEquals(n.getPartitionId(99, 7), 99);
    // long overload narrows to int.
    assertEquals(n.getPartitionId(42L, 7), 42);
    assertEquals(n.getPartitionId((long) Integer.MAX_VALUE, 1024), Integer.MAX_VALUE);
  }

  @Test
  public void testRangeFoldingNormalizersReturnInRange() {
    int[] partitionCounts = {1, 2, 7, 1024};
    int[] hashes = {0, 1, -1, 7, -7, Integer.MIN_VALUE, Integer.MAX_VALUE, 13, -13};
    // NO_OP is excluded — it's identity and only safe when the caller already produced an
    // in-range value. The other four normalizers fold every signed input into [0, N).
    for (PartitionIdNormalizer n : PartitionIdNormalizer.values()) {
      if (n == PartitionIdNormalizer.NO_OP) {
        continue;
      }
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
    for (PartitionIdNormalizer n : PartitionIdNormalizer.values()) {
      assertEquals(PartitionIdNormalizer.fromConfigString(n.name()), n);
      assertEquals(PartitionIdNormalizer.fromConfigString(n.name().toLowerCase()), n);
      assertEquals(PartitionIdNormalizer.fromConfigString("  " + n.name() + "  "), n);
    }
  }

  @Test
  public void testFromConfigStringRejectsBlank() {
    expectThrows(IllegalArgumentException.class, () -> PartitionIdNormalizer.fromConfigString(null));
    expectThrows(IllegalArgumentException.class, () -> PartitionIdNormalizer.fromConfigString(""));
    expectThrows(IllegalArgumentException.class, () -> PartitionIdNormalizer.fromConfigString("   "));
  }

  @Test
  public void testFromConfigStringRejectsUnknown() {
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
        () -> PartitionIdNormalizer.fromConfigString("not-a-real-normalizer"));
    assertTrue(e.getMessage().contains("not-a-real-normalizer"));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNonPositivePartitionsRejected() {
    PartitionIdNormalizer.POSITIVE_MODULO.getPartitionId(5, 0);
  }

  private static int shiftedMod(long value, int numPartitions) {
    long m = value % numPartitions;
    return (int) (m < 0 ? m + numPartitions : m);
  }
}
