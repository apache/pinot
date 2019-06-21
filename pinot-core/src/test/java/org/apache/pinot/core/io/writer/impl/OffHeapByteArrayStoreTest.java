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

package org.apache.pinot.core.io.writer.impl;

import java.util.Random;
import org.apache.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class OffHeapByteArrayStoreTest {
  private static Random RANDOM = new Random();
  private PinotDataBufferMemoryManager _memoryManager;

  @BeforeClass
  public void setUp() {
    _memoryManager = new DirectMemoryManager(OffHeapStringStoreTest.class.getName());
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _memoryManager.close();
  }

  // Test that we can add a byte array of max length possible.
  @Test
  public void maxValueTest() {
    final int valueSize = 500;
    final int bufferSize = 500 + Integer.BYTES;

    OffHeapByteArrayStore store;

    store = new OffHeapByteArrayStore(bufferSize, _memoryManager, "test");

    // We should be able to add one array of max value
    byte[] value = generateRandomValue(valueSize);
    int index = store.add(value);
    byte[] valueToCompare = store.get(index);
    Assert.assertEquals(valueToCompare, value);

    // And no more.
    value = new byte[] {1};
    index = store.add(value);
    Assert.assertEquals(-1, index);
  }

  // Test that we can add the max possible size with multiple values.
  @Test
  public void multipleValuesTest() {
    // Add N values that fit exactly into the store. Can't add N+1th value, for different values of N
    for (int iterations = 0; iterations < 100; iterations++) {
      final int sumOfLengthsOfValues = 50000;
      final int maxNumValues = 20 + RANDOM.nextInt(30) + 20;
      final OffHeapByteArrayStore store =
          new OffHeapByteArrayStore((long) sumOfLengthsOfValues + maxNumValues * Integer.BYTES, _memoryManager, "test");

      Assert.assertTrue(sumOfLengthsOfValues / maxNumValues > 1);
      int totalLen = 0;
      int index = 0;
      for (int i = 0; i < maxNumValues - 1; i++) {
        int len = RANDOM.nextInt(sumOfLengthsOfValues / maxNumValues) + 1;
        byte[] value = generateRandomValue(len);
        int thisIndex = store.add(value);
        Assert.assertEquals(thisIndex, index++);

        totalLen += len;
      }

      Assert.assertTrue(totalLen < sumOfLengthsOfValues);
      byte[] value = generateRandomValue(sumOfLengthsOfValues - totalLen);
      int thisIndex = store.add(value);
      Assert.assertEquals(thisIndex, index);

      value = new byte[]{23};
      Assert.assertEquals(store.add(value), -1);
    }
  }

  // Generates a random byte array of given size.
  private byte[] generateRandomValue(final int len) {
    byte[] bytes = new byte[len];
    for (int i = 0; i < len; i++) {
      bytes[i] = (byte) (RANDOM.nextInt(92) + 32);
    }
    return bytes;
  }
}
