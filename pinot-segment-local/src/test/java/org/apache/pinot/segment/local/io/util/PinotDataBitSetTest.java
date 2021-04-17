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
package org.apache.pinot.segment.local.io.util;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Random;
import org.apache.pinot.segment.local.segment.memory.PinotDataBuffer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class PinotDataBitSetTest {
  private static final Random RANDOM = new Random();
  private static final int NUM_ITERATIONS = 1000;

  @Test
  public void testGetNumBitsPerValue() {
    // Should return 1 for max value 0
    assertEquals(PinotDataBitSet.getNumBitsPerValue(0), 1);

    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int maxValue = RANDOM.nextInt(Integer.MAX_VALUE) + 1;
      int actual = PinotDataBitSet.getNumBitsPerValue(maxValue);
      int expected = 0;
      while (maxValue > 0) {
        maxValue >>>= 1;
        expected++;
      }
      assertEquals(actual, expected);
    }
  }

  @Test
  public void testReadWriteInt() throws IOException {
    int numBitsPerValue = RANDOM.nextInt(10) + 1;
    int maxAllowedValue = 1 << numBitsPerValue;
    int numValues = 100;
    int[] values = new int[numValues];
    int dataBufferSize = (numValues * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE;

    try (PinotDataBuffer dataBuffer = getEmptyDataBuffer(dataBufferSize);
        PinotDataBitSet dataBitSet = new PinotDataBitSet(dataBuffer)) {
      for (int i = 0; i < numValues; i++) {
        int value = RANDOM.nextInt(maxAllowedValue);
        values[i] = value;
        dataBitSet.writeInt(i, numBitsPerValue, value);
      }

      // Test single value read
      for (int i = 0; i < NUM_ITERATIONS; i++) {
        int index = RANDOM.nextInt(numValues);
        assertEquals(dataBitSet.readInt(index, numBitsPerValue), values[index]);
      }

      // Test batch read
      int[] buffer = new int[10];
      for (int i = 0; i < NUM_ITERATIONS; i++) {
        int startIndex = RANDOM.nextInt(numValues - 10);
        int numValuesToRead = RANDOM.nextInt(10) + 1;
        dataBitSet.readInt(startIndex, numBitsPerValue, numValuesToRead, buffer);
        for (int j = 0; j < numValuesToRead; j++) {
          assertEquals(buffer[j], values[startIndex + j]);
        }
      }

      // Test batch write
      int startIndex = 0;
      while (startIndex < numValues) {
        int numValuesToWrite = Math.min(RANDOM.nextInt(10) + 1, numValues - startIndex);
        System.arraycopy(values, startIndex, buffer, 0, numValuesToWrite);
        dataBitSet.writeInt(startIndex, numBitsPerValue, numValuesToWrite, buffer);
        startIndex += numValuesToWrite;
      }
      for (int i = 0; i < NUM_ITERATIONS; i++) {
        int index = RANDOM.nextInt(numValues);
        assertEquals(dataBitSet.readInt(index, numBitsPerValue), values[index]);
      }
    }
  }

  @Test
  public void testSetUnsetBit() throws IOException {
    int dataBufferSize = RANDOM.nextInt(100) + 1;
    boolean[] bits = new boolean[dataBufferSize * Byte.SIZE];
    try (PinotDataBuffer dataBuffer = getEmptyDataBuffer(dataBufferSize);
        PinotDataBitSet dataBitSet = new PinotDataBitSet(dataBuffer)) {
      for (int i = 0; i < NUM_ITERATIONS; i++) {
        int bitOffset = RANDOM.nextInt(dataBufferSize * Byte.SIZE);
        if (bits[bitOffset]) {
          assertEquals(dataBitSet.readInt(bitOffset, 1), 1);
        } else {
          assertEquals(dataBitSet.readInt(bitOffset, 1), 0);
        }
        if (RANDOM.nextBoolean()) {
          dataBitSet.setBit(bitOffset);
          bits[bitOffset] = true;
          assertEquals(dataBitSet.readInt(bitOffset, 1), 1);
        } else {
          dataBitSet.unsetBit(bitOffset);
          bits[bitOffset] = false;
          assertEquals(dataBitSet.readInt(bitOffset, 1), 0);
        }
      }
    }
  }

  @Test
  public void testGetNextSetBitOffset() throws IOException {
    int[] setBitOffsets = new int[NUM_ITERATIONS];
    int bitOffset = RANDOM.nextInt(10);
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      setBitOffsets[i] = bitOffset;
      bitOffset += RANDOM.nextInt(10) + 1;
    }
    int dataBufferSize = setBitOffsets[NUM_ITERATIONS - 1] / Byte.SIZE + 1;
    try (PinotDataBuffer dataBuffer = getEmptyDataBuffer(dataBufferSize);
        PinotDataBitSet dataBitSet = new PinotDataBitSet(dataBuffer)) {
      for (int i = 0; i < NUM_ITERATIONS; i++) {
        dataBitSet.setBit(setBitOffsets[i]);
      }

      // Test next set bit offset
      for (int i = 0; i < NUM_ITERATIONS - 1; i++) {
        assertEquals(dataBitSet.getNextSetBitOffset(
            setBitOffsets[i] + RANDOM.nextInt(setBitOffsets[i + 1] - setBitOffsets[i]) + 1), setBitOffsets[i + 1]);
      }

      // Test next nth set bit offset
      for (int i = 0; i < NUM_ITERATIONS - 100; i++) {
        int n = RANDOM.nextInt(100) + 1;
        assertEquals(
            dataBitSet.getNextNthSetBitOffset(
                setBitOffsets[i] + RANDOM.nextInt(setBitOffsets[i + 1] - setBitOffsets[i]) + 1, n),
            setBitOffsets[i + n]);
      }
    }
  }

  private PinotDataBuffer getEmptyDataBuffer(int size) {
    PinotDataBuffer dataBuffer = PinotDataBuffer.allocateDirect(size, ByteOrder.BIG_ENDIAN, null);
    for (int i = 0; i < size; i++) {
      dataBuffer.readFrom(0, new byte[size]);
    }
    return dataBuffer;
  }
}
