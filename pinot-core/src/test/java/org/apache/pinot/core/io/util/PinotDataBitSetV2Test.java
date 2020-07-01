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
package org.apache.pinot.core.io.util;

import java.nio.ByteOrder;
import java.util.Random;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test for new implementation of {@link PinotBitSet}
 * that supports vectorized operations for power of 2
 * encodings.
 */
public class PinotDataBitSetV2Test {

  private void batchRead(PinotDataBitSetV2 bitset, int startDocId, int batchLength, int[] unpacked,
      int[] forwardIndex) {
    bitset.readInt(startDocId, batchLength, unpacked);
    for (int i = 0; i < batchLength; i++) {
      Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
    }
  }

  @Test
  public void testBit2Encoded() throws Exception {
    int cardinality = 3;
    int rows = 100000;
    int[] forwardIndex = new int[rows];
    Random random = new Random();

    for (int i = 0; i < rows; i++) {
      forwardIndex[i] = random.nextInt(cardinality);
    }

    int numBitsPerValue = PinotDataBitSet.getNumBitsPerValue(cardinality - 1);
    int bitPackedBufferSize = (rows * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE;
    try (PinotDataBuffer dataBuffer = getBuffer(bitPackedBufferSize);
        PinotDataBitSetV2 bitSet = getEmptyBitSet(dataBuffer, bitPackedBufferSize, numBitsPerValue)) {
      Assert.assertEquals(2, numBitsPerValue);
      Assert.assertTrue(bitSet instanceof PinotDataBitSetV2.Bit2Encoded);

      for (int i = 0; i < rows; i++) {
        bitSet.writeInt(i, forwardIndex[i]);
      }

      // test single read API for sequential consecutive
      for (int i = 0; i < rows; i++) {
        int unpacked = bitSet.readInt(i);
        Assert.assertEquals(forwardIndex[i], unpacked);
      }

      // for each batch:
      // 3 aligned reads at 4-byte boundary to unpack 16 integers after each read -- 48 integers unpacked
      // followed by reading the next byte to unpack 2 integers from first 4 bits
      int batchLength = 50;
      int[] unpacked = new int[batchLength];
      int startDocId;
      for (startDocId = 0; startDocId < rows; startDocId += 50) {
        bitSet.readInt(startDocId, batchLength, unpacked);
        for (int i = 0; i < batchLength; i++) {
          Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
        }
      }

      // 3 aligned reads at 4-byte boundary to unpack 16 integers after each read -- 48 integers unpacked
      // followed by reading the next 2 bytes to unpack 8 integers
      batchLength = 56;
      unpacked = new int[batchLength];
      startDocId = 1;
      batchRead(bitSet, startDocId, batchLength, unpacked, forwardIndex);

      // 3 aligned reads at 4-byte boundary to unpack 16 integers after each read -- 48 integers unpacked
      // followed by reading the next 2 bytes to unpack 8 integers
      // followed by reading the next byte to unpack 4 integers
      batchLength = 60;
      unpacked = new int[batchLength];
      startDocId = 20;
      batchRead(bitSet, startDocId, batchLength, unpacked, forwardIndex);

      // 3 aligned reads at 4-byte boundary to unpack 16 integers after each read -- 48 integers unpacked
      // followed by reading the next 2 bytes to unpack 8 integers
      // followed by reading the next byte to unpack 4 integers
      // followed by reading the next byte to unpack 1 integer from first 2 bits
      batchLength = 61;
      unpacked = new int[batchLength];
      startDocId = 20;
      batchRead(bitSet, startDocId, batchLength, unpacked, forwardIndex);

      // 3 aligned reads at 4-byte boundary to unpack 16 integers after each read -- 48 integers unpacked
      // followed by reading the next 2 bytes to unpack 8 integers
      // followed by reading the next byte to unpack 4 integers
      // followed by reading the next byte to unpack 2 integers from first 4 bits
      batchLength = 62;
      unpacked = new int[batchLength];
      startDocId = 20;
      batchRead(bitSet, startDocId, batchLength, unpacked, forwardIndex);

      // 3 aligned reads at 4-byte boundary to unpack 16 integers after each read -- 48 integers unpacked
      // followed by reading the next 2 bytes to unpack 8 integers
      // followed by reading the next byte to unpack 4 integers
      // followed by reading the next byte to unpack 6 integers from first 6 bits
      batchLength = 63;
      unpacked = new int[batchLength];
      startDocId = 20;
      batchRead(bitSet, startDocId, batchLength, unpacked, forwardIndex);

      // unaligned read on the first byte to unpack 3 integers
      // followed by 3 aligned reads at byte boundary to unpack 4 integers after each read -- 12 integers unpacked
      // followed by reading the next byte to unpack 2 integer from first 4 bits
      // 3 + 12 + 2  = 17 unpacked integers
      batchLength = 17;
      unpacked = new int[batchLength];
      startDocId = 1;
      bitSet.readInt(startDocId, batchLength, unpacked);
      for (int i = 0; i < batchLength; i++) {
        Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
      }

      // unaligned read on the first byte to unpack 3 integers (bits 2 to 7)
      // followed by 3 aligned reads at 4-byte boundary to unpack 16 integers after each read -- 48 integers unpacked
      // followed by 1 aligned read at byte boundary to unpack 4 integers -- 4 integers unpacked
      // followed by reading the next byte to unpack 3 integers from first 6 bits
      // 3 + 48 + 4 + 3 = 58 unpacked integers
      batchLength = 58;
      unpacked = new int[batchLength];
      startDocId = 1;
      bitSet.readInt(startDocId, batchLength, unpacked);
      for (int i = 0; i < batchLength; i++) {
        Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
      }

      // for each batch:
      // unaligned read on the first byte to unpack 2 integers (bits 4 to 7)
      // followed by 3 aligned reads at 4-byte boundary to unpack 16 integers after each read -- 48 integers unpacked
      // followed by 2 aligned reads at byte boundary to unpack 4 integers after each read -- 8 integers unpacked
      // 3 + 48 + 8 = 58 unpacked integers
      startDocId = 2;
      unpacked = new int[batchLength];
      bitSet.readInt(startDocId, batchLength, unpacked);
      for (int i = 0; i < batchLength; i++) {
        Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
      }

      // for each batch:
      // unaligned read on the first byte to unpack 1 integers (bits 6 to 7)
      // followed by 3 aligned reads at 4-byte boundary to unpack 16 integers after each read -- 48 integers unpacked
      // followed by 2 aligned reads at byte boundary to unpack 4 integers after each read -- 8 integers unpacked
      // followed by reading the next byte to unpack 1 integer from first 2 bits
      // 1 + 48 + 8 + 1 = 58 unpacked integers
      startDocId = 3;
      unpacked = new int[batchLength];
      bitSet.readInt(startDocId, batchLength, unpacked);
      for (int i = 0; i < batchLength; i++) {
        Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
      }

      // test bulk API for sequential but not necessarily consecutive
      testBulkSequentialWithGaps(dataBuffer, forwardIndex, rows, 2);
    }
  }

  @Test
  public void testBit4Encoded() throws Exception {
    int cardinality = 11;
    int rows = 100000;
    int[] forwardIndex = new int[rows];
    Random random = new Random();

    for (int i = 0; i < rows; i++) {
      forwardIndex[i] = random.nextInt(cardinality);
    }

    int numBitsPerValue = PinotDataBitSet.getNumBitsPerValue(cardinality - 1);
    int bitPackedBufferSize = (rows * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE;
    try (PinotDataBuffer dataBuffer = getBuffer(bitPackedBufferSize);
        PinotDataBitSetV2 bitSet = getEmptyBitSet(dataBuffer, bitPackedBufferSize, numBitsPerValue)) {
      Assert.assertEquals(4, numBitsPerValue);
      Assert.assertTrue(bitSet instanceof PinotDataBitSetV2.Bit4Encoded);

      for (int i = 0; i < rows; i++) {
        bitSet.writeInt(i, forwardIndex[i]);
      }

      // test single read API for sequential consecutive
      for (int i = 0; i < rows; i++) {
        int unpacked = bitSet.readInt(i);
        Assert.assertEquals(forwardIndex[i], unpacked);
      }

      // test array API for sequential consecutive

      // for each batch: do a combination of aligned and unaligned reads
      // 6 aligned reads at 4-byte boundary to unpack 8 integers after each read -- 48 integers unpacked
      // followed by reading the next byte to unpack 2 integers
      int batchLength = 50;
      int[] unpacked = new int[batchLength];
      int startDocId;
      for (startDocId = 0; startDocId < rows; startDocId += batchLength) {
        bitSet.readInt(startDocId, batchLength, unpacked);
        for (int i = 0; i < batchLength; i++) {
          Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
        }
      }

      // 12 aligned reads at 4-byte boundary to unpack 8 integers after each read -- 96 integers unpacked
      batchLength = 96;
      unpacked = new int[batchLength];
      startDocId = 19;
      bitSet.readInt(startDocId, batchLength, unpacked);
      for (int i = 0; i < batchLength; i++) {
        Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
      }

      // only a single unaligned read from the middle of a byte (44th bit)
      batchLength = 1;
      startDocId = 21;
      bitSet.readInt(startDocId, batchLength, unpacked);
      Assert.assertEquals(forwardIndex[startDocId], unpacked[0]);

      // unaligned read within a byte to unpack an integer from bits 4 to 7
      // followed by 2 aligned reads at 4-byte boundary to unpack 8 integers after each read -- unpacked 16 integers
      // followed by 1 aligned read at 2-byte boundary to unpack 4 integers
      // followed by 1 aligned read at byte boundary to unpack 2 integers
      // followed by reading the next byte to unpack integer from first 4 bits
      // 1 + 16 + 4 + 2 + 1 = 24 unpacked integers
      startDocId = 1;
      batchLength = 24;
      unpacked = new int[batchLength];
      bitSet.readInt(startDocId, batchLength, unpacked);
      for (int i = 0; i < batchLength; i++) {
        Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
      }

      // unaligned read within a byte to unpack an integer from bits 4 to 7
      // 1 aligned read at 2-byte boundary to unpack 4 integers
      // followed by 1 aligned read at byte boundary to unpack 2 integers
      // followed by reading the next byte to unpack integer from first 4 bits
      // 1 + 4 + 2 + 1 = 8 unpacked integers
      startDocId = 1;
      batchLength = 8;
      unpacked = new int[batchLength];
      bitSet.readInt(startDocId, batchLength, unpacked);
      for (int i = 0; i < batchLength; i++) {
        Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
      }

      // 1 aligned read at 2-byte boundary to unpack 4 integers
      // followed by 1 aligned read at byte boundary to unpack 2 integers
      // followed by reading the next byte to unpack integer from first 4 bits
      // 4 + 2 + 1 = 7 unpacked integers
      startDocId = 4;
      batchLength = 7;
      unpacked = new int[batchLength];
      bitSet.readInt(startDocId, batchLength, unpacked);
      for (int i = 0; i < batchLength; i++) {
        Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
      }

      // test bulk API for sequential but not necessarily consecutive
      testBulkSequentialWithGaps(dataBuffer, forwardIndex, rows, 4);
    }
  }

  @Test
  public void testBit8Encoded() throws Exception {
    int cardinality = 190;
    int rows = 100000;
    int[] forwardIndex = new int[rows];
    Random random = new Random();

    for (int i = 0; i < rows; i++) {
      forwardIndex[i] = random.nextInt(cardinality);
    }

    int numBitsPerValue = PinotDataBitSet.getNumBitsPerValue(cardinality - 1);
    int bitPackedBufferSize = (rows * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE;
    try (PinotDataBuffer dataBuffer = getBuffer(bitPackedBufferSize);
        PinotDataBitSetV2 bitSet = getEmptyBitSet(dataBuffer, bitPackedBufferSize, numBitsPerValue)) {
      Assert.assertEquals(8, numBitsPerValue);
      Assert.assertTrue(bitSet instanceof PinotDataBitSetV2.Bit8Encoded);

      for (int i = 0; i < rows; i++) {
        bitSet.writeInt(i, forwardIndex[i]);
      }

      // test single read API for sequential consecutive
      for (int i = 0; i < rows; i++) {
        int unpacked = bitSet.readInt(i);
        Assert.assertEquals(forwardIndex[i], unpacked);
      }

      // test array API for sequential consecutive

      // for each batch:
      // 12 aligned reads at 4-byte boundary to unpack 4 integers after each read -- 48 integers unpacked
      // followed by reading the next 2 bytes to unpack 2 integers
      int batchLength = 50;
      int[] unpacked = new int[batchLength];
      int startDocId;
      for (startDocId = 0; startDocId < rows; startDocId += batchLength) {
        bitSet.readInt(startDocId, batchLength, unpacked);
        for (int i = 0; i < batchLength; i++) {
          Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
        }
      }

      // for each batch:
      // 24 aligned reads at 4-byte boundary to unpack 4 integers after each read -- 96 integers unpacked
      batchLength = 96;
      unpacked = new int[batchLength];
      startDocId = 7;
      bitSet.readInt(startDocId, batchLength, unpacked);
      for (int i = 0; i < batchLength; i++) {
        Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
      }

      // unaligned spill over
      startDocId = 19;
      batchLength = 3;
      bitSet.readInt(startDocId, batchLength, unpacked);
      Assert.assertEquals(forwardIndex[startDocId], unpacked[0]);

      // test bulk API for sequential but not necessarily consecutive
      testBulkSequentialWithGaps(dataBuffer, forwardIndex, rows, 8);
    }
  }

  @Test
  public void testBit16Encoded() throws Exception {
    int cardinality = 40000;
    int rows = 100000;
    int[] forwardIndex = new int[rows];
    Random random = new Random();

    for (int i = 0; i < rows; i++) {
      forwardIndex[i] = random.nextInt(cardinality);
    }

    int numBitsPerValue = PinotDataBitSet.getNumBitsPerValue(cardinality - 1);
    int bitPackedBufferSize = (rows * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE;
    try (PinotDataBuffer dataBuffer = getBuffer(bitPackedBufferSize);
        PinotDataBitSetV2 bitSet = getEmptyBitSet(dataBuffer, bitPackedBufferSize, numBitsPerValue)) {
      Assert.assertEquals(16, numBitsPerValue);
      Assert.assertTrue(bitSet instanceof PinotDataBitSetV2.Bit16Encoded);

      for (int i = 0; i < rows; i++) {
        bitSet.writeInt(i, forwardIndex[i]);
      }

      // test single read API for sequential consecutive
      for (int i = 0; i < rows; i++) {
        int unpacked = bitSet.readInt(i);
        Assert.assertEquals(forwardIndex[i], unpacked);
      }

      // test array API for sequential consecutive

      // for each batch:
      // 25 aligned reads at 4-byte boundary to unpack 2 integers after each read -- 50 integers unpacked
      int batchLength = 50;
      int[] unpacked = new int[batchLength];
      int startDocId;
      for (startDocId = 0; startDocId < rows; startDocId += batchLength) {
        bitSet.readInt(startDocId, batchLength, unpacked);
        for (int i = 0; i < batchLength; i++) {
          Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
        }
      }

      // 25 aligned reads at 4-byte boundary to unpack 2 integers after each read -- 50 integers unpacked
      // followed by unpacking 1 integer from the next 2 bytes
      batchLength = 51;
      startDocId = 3;
      unpacked = new int[batchLength];
      bitSet.readInt(startDocId, batchLength, unpacked);
      for (int i = 0; i < batchLength; i++) {
        Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
      }

      // unaligned spill over
      startDocId = 7;
      batchLength = 1;
      bitSet.readInt(startDocId, batchLength, unpacked);
      Assert.assertEquals(forwardIndex[startDocId], unpacked[0]);

      // test array API for sequential but not necessarily consecutive
      testBulkSequentialWithGaps(dataBuffer, forwardIndex, rows, 16);
    }
  }

  @Test
  public void testBit32Encoded() throws Exception {
    int cardinality = 100000;
    int rows = 100000;
    int[] forwardIndex = new int[rows];
    Random random = new Random();

    for (int i = 0; i < rows; i++) {
      forwardIndex[i] = random.nextInt(cardinality);
    }

    int numBitsPerValue = 32;
    int bitPackedBufferSize = (rows * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE;
    try (PinotDataBuffer dataBuffer = getBuffer(bitPackedBufferSize);
        PinotDataBitSetV2 bitSet = getEmptyBitSet(dataBuffer, bitPackedBufferSize, numBitsPerValue)) {
      Assert.assertTrue(bitSet instanceof PinotDataBitSetV2.RawInt);

      for (int i = 0; i < rows; i++) {
        bitSet.writeInt(i, forwardIndex[i]);
      }

      // test single read API for sequential consecutive
      for (int i = 0; i < rows; i++) {
        int unpacked = bitSet.readInt(i);
        Assert.assertEquals(forwardIndex[i], unpacked);
      }

      // test array API for sequential consecutive
      int batchLength = 50;
      int[] unpacked = new int[batchLength];
      int startDocId;
      for (startDocId = 0; startDocId < rows; startDocId += batchLength) {
        bitSet.readInt(startDocId, batchLength, unpacked);
        for (int i = 0; i < batchLength; i++) {
          Assert.assertEquals(forwardIndex[startDocId + i], unpacked[i]);
        }
      }

      // test array API for sequential but not necessarily consecutive
      testBulkSequentialWithGaps(dataBuffer, forwardIndex, rows, 32);
    }
  }

  private void testBulkSequentialWithGaps(PinotDataBuffer dataBuffer, int[] forwardIndex, int rows, int numBits) {
    testBulkSequentialWithGaps(dataBuffer, 1, 50, -1, forwardIndex, rows, numBits);
    testBulkSequentialWithGaps(dataBuffer, 3, 57, 4, forwardIndex, rows, numBits);
    testBulkSequentialWithGaps(dataBuffer, 5, 109, 9, forwardIndex, rows, numBits);
    testBulkSequentialWithGaps(dataBuffer, 7, 1000, 4, forwardIndex, rows, numBits);
    testBulkSequentialWithGaps(dataBuffer, 17, 233, 19, forwardIndex, rows, numBits);
    testBulkSequentialWithGaps(dataBuffer, 1, 1, 19, forwardIndex, rows, numBits);
  }

  private void testBulkSequentialWithGaps(PinotDataBuffer dataBuffer, int gaps,
      int batchLength, int startDocId, int[] forwardIndex, int rows, int numBits) {
    int docId = startDocId;
    int[] docIds = new int[batchLength];
    Random random = new Random();
    for (int i = 0; i < batchLength; i++) {
      docId = docId + 1 + random.nextInt(gaps);
      docIds[i] = docId;
    }
    int[] unpacked = new int[batchLength];
    FixedBitIntReaderWriterV2 bitReader = new FixedBitIntReaderWriterV2(dataBuffer, rows, numBits);
    bitReader.readValues(docIds, 0, batchLength, unpacked, 0);
    for (int i = 0; i < batchLength; i++) {
      Assert.assertEquals(forwardIndex[docIds[i]], unpacked[i]);
    }
  }

  private PinotDataBuffer getBuffer(int size) {
    return PinotDataBuffer.allocateDirect(size, ByteOrder.BIG_ENDIAN, null);
  }

  private PinotDataBitSetV2 getEmptyBitSet(PinotDataBuffer bitPackedBuffer, int size, int numBitsPerValue) {
    for (int i = 0; i < size; i++) {
      bitPackedBuffer.readFrom(0, new byte[size]);
    }
    return (PinotDataBitSetV2)PinotDataBitSetFactory.createBitSet(bitPackedBuffer, numBitsPerValue);
  }
}
