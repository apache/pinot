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

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class HyperLogLogPlusUtilsTest {

  @Test
  public void testMaxByteSizeNormalFormat()
      throws IOException {
    for (int p = 4; p <= 16; p++) {
      int maxByteSize = HyperLogLogPlusUtils.maxByteSize(p, 0);
      HyperLogLogPlus hllPlus = new HyperLogLogPlus(p);
      assertEquals(hllPlus.getBytes().length, maxByteSize);
      for (int i = 0; i < 1000; i++) {
        hllPlus.offer(i);
      }
      byte[] bytes = hllPlus.getBytes();
      assertEquals(bytes.length, maxByteSize);
      assertEquals(HyperLogLogPlusUtils.maxByteSize(bytes), maxByteSize);
    }
  }

  @Test
  public void testMaxByteSizeSparseFormat()
      throws IOException {
    for (int p = 4; p <= 16; p += 2) {
      int m = 1 << p;
      int sparseSetThreshold = (int) (m * 0.75);
      int[] numValuesToFill = new int[]{0, 1, m / 8, m / 4, m / 2, sparseSetThreshold, sparseSetThreshold + 1, 2 * m};
      for (int sp : new int[]{p, 20, HyperLogLogPlusUtils.MAX_SP}) {
        int maxByteSize = HyperLogLogPlusUtils.maxByteSize(p, sp);
        HyperLogLogPlus hllPlus = new HyperLogLogPlus(p, sp);
        int numValues = 0;
        for (int numValuesToReach : numValuesToFill) {
          for (; numValues < numValuesToReach; numValues++) {
            hllPlus.offer(numValues);
          }
          byte[] bytes = hllPlus.getBytes();
          assertTrue(bytes.length <= maxByteSize,
              String.format("p: %d, sp: %d, numValues: %d, serialized size: %d exceeds max byte size: %d", p, sp,
                  numValues, bytes.length, maxByteSize));
          assertEquals(HyperLogLogPlusUtils.maxByteSize(bytes), maxByteSize);
        }
      }
    }
  }

  @Test
  public void testMaxByteSizeLegacyFormat() {
    for (int sp : new int[]{0, 20}) {
      byte[] legacyHeader = ByteBuffer.allocate(2 * Integer.BYTES).putInt(12).putInt(sp).array();
      assertEquals(HyperLogLogPlusUtils.maxByteSize(legacyHeader), HyperLogLogPlusUtils.maxByteSize(12, sp));
    }
  }

  @Test
  public void testValidateSp() {
    HyperLogLogPlusUtils.validateSp(0);
    HyperLogLogPlusUtils.validateSp(25);
    assertThrows(IllegalArgumentException.class, () -> HyperLogLogPlusUtils.validateSp(26));
    assertThrows(IllegalArgumentException.class, () -> HyperLogLogPlusUtils.validateSp(32));
  }
}
