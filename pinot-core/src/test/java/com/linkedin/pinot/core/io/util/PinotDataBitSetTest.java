/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.io.util;

import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PinotDataBitSetTest {
  private static final Random RANDOM = new Random();
  private static final int NUM_ITERATIONS = 1000;

  @Test
  public void testReadWriteInt() {
    int numBitsPerValue = RANDOM.nextInt(10) + 1;
    int maxAllowedValue = 1 << numBitsPerValue;
    int numValues = 100;
    int[] values = new int[numValues];
    int dataBufferSize = (numValues * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE;

    try (PinotDataBitSet dataBitSet = new PinotDataBitSet(PinotDataBuffer.allocateDirect(dataBufferSize))) {
      for (int i = 0; i < numValues; i++) {
        int value = RANDOM.nextInt(maxAllowedValue);
        values[i] = value;
        dataBitSet.writeInt(i, numBitsPerValue, value);
      }

      // Test single value read
      for (int i = 0; i < NUM_ITERATIONS; i++) {
        int index = RANDOM.nextInt(numValues);
        Assert.assertEquals(dataBitSet.readInt(index, numBitsPerValue), values[index]);
      }

      // Test batch read
      int[] buffer = new int[10];
      for (int i = 0; i < NUM_ITERATIONS; i++) {
        int startIndex = RANDOM.nextInt(numValues - 10);
        int numValuesToRead = RANDOM.nextInt(10) + 1;
        dataBitSet.readInt(startIndex, numBitsPerValue, numValuesToRead, buffer);
        for (int j = 0; j < numValuesToRead; j++) {
          Assert.assertEquals(buffer[j], values[startIndex + j]);
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
        Assert.assertEquals(dataBitSet.readInt(index, numBitsPerValue), values[index]);
      }
    }
  }

  @Test
  public void testSetUnsetBit() {
    int dataBufferSize = RANDOM.nextInt(100) + 1;
    boolean bits[] = new boolean[dataBufferSize * Byte.SIZE];
    try (PinotDataBitSet dataBitSet = new PinotDataBitSet(PinotDataBuffer.allocateDirect(dataBufferSize))) {
      for (int i = 0; i < NUM_ITERATIONS; i++) {
        int bitOffset = RANDOM.nextInt(dataBufferSize * Byte.SIZE);
        if (bits[bitOffset]) {
          Assert.assertEquals(dataBitSet.readInt(bitOffset, 1), 1);
        } else {
          Assert.assertEquals(dataBitSet.readInt(bitOffset, 1), 0);
        }
        if (RANDOM.nextBoolean()) {
          dataBitSet.setBit(bitOffset);
          bits[bitOffset] = true;
          Assert.assertEquals(dataBitSet.readInt(bitOffset, 1), 1);
        } else {
          dataBitSet.unsetBit(bitOffset);
          bits[bitOffset] = false;
          Assert.assertEquals(dataBitSet.readInt(bitOffset, 1), 0);
        }
      }
    }
  }

  @Test
  public void testGetNextSetBitOffset() {
    int[] setBitOffsets = new int[NUM_ITERATIONS];
    int bitOffset = RANDOM.nextInt(10);
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      setBitOffsets[i] = bitOffset;
      bitOffset += RANDOM.nextInt(10) + 1;
    }
    int dataBufferSize = setBitOffsets[NUM_ITERATIONS - 1] / Byte.SIZE + 1;
    try (PinotDataBitSet dataBitSet = new PinotDataBitSet(PinotDataBuffer.allocateDirect(dataBufferSize))) {
      for (int i = 0; i < NUM_ITERATIONS; i++) {
        dataBitSet.setBit(setBitOffsets[i]);
      }

      // Test next set bit offset
      for (int i = 0; i < NUM_ITERATIONS - 1; i++) {
        Assert.assertEquals(dataBitSet.getNextSetBitOffset(
            setBitOffsets[i] + RANDOM.nextInt(setBitOffsets[i + 1] - setBitOffsets[i]) + 1), setBitOffsets[i + 1]);
      }

      // Test next nth set bit offset
      for (int i = 0; i < NUM_ITERATIONS - 100; i++) {
        int n = RANDOM.nextInt(100) + 1;
        Assert.assertEquals(dataBitSet.getNextNthSetBitOffset(
            setBitOffsets[i] + RANDOM.nextInt(setBitOffsets[i + 1] - setBitOffsets[i]) + 1, n), setBitOffsets[i + n]);
      }
    }
  }
}
