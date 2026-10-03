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
package org.apache.pinot.nativeengine.agg;

import java.util.Random;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class PinotNativeAggTest {

  @BeforeClass
  public void skipIfNativeUnavailable() {
    if (!PinotNativeAgg.isAvailable()) {
      throw new SkipException("Pinot native library not available on this platform; "
          + "set -Dpinot.native.lib.path=<path-to-libpinot_native> to enable.");
    }
  }

  @Test
  public void probeReturnsMagic() {
    assertEquals(PinotNativeAgg.probe(), 0x5049_4E4F);
  }

  @Test
  public void sumLongEmptyIsZero() {
    assertEquals(PinotNativeAgg.sumLong(new long[0], 0), 0.0);
  }

  @Test
  public void sumLongSmallRange() {
    long[] values = new long[100];
    for (int i = 0; i < 100; i++) {
      values[i] = i + 1;
    }
    double expected = 0.0;
    for (int i = 0; i < 100; i++) {
      expected += values[i];
    }
    assertEquals(PinotNativeAgg.sumLong(values, 100), expected);
  }

  @Test
  public void sumLongLargeRandomMatchesJava() {
    Random rng = new Random(42);
    int n = 1_000_000;
    long[] values = new long[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextLong() % 1_000_000L;
    }

    double javaSum = 0.0;
    for (int i = 0; i < n; i++) {
      javaSum += values[i];
    }
    double nativeSum = PinotNativeAgg.sumLong(values, n);

    // For magnitudes within mantissa range we expect exact equality. Allow a tiny
    // relative tolerance to absorb the chunk-reduction-order difference.
    double tolerance = Math.max(1.0, Math.abs(javaSum) * 1e-15);
    assertTrue(Math.abs(nativeSum - javaSum) <= tolerance,
        "native=" + nativeSum + " java=" + javaSum + " diff=" + (nativeSum - javaSum));
  }

  @Test
  public void sumLongRespectsLengthArgument() {
    long[] values = {10, 20, 30, 40, 50};
    // Only sum the first 3 elements
    assertEquals(PinotNativeAgg.sumLong(values, 3), 60.0);
  }
}
