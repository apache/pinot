/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.index.writer;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.index.writer.impl.v1.FixedBitMultiValueWriter;
import com.linkedin.pinot.core.util.CustomBitSet;


public class FixedBitSkipListSCMVWriterTest {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(FixedBitSkipListSCMVWriterTest.class);

  @Test
  public void testSingleColMultiValue() throws Exception {
    int maxBits = 2;
    while (maxBits < 32) {
      LOGGER.debug("START test maxBit:" + maxBits);
      File file = new File("test_single_col_multi_value_writer.dat");
      file.delete();
      int rows = 100;
      int[][] data = new int[rows][];
      int maxValue = (int) Math.pow(2, maxBits);
      Random r = new Random();
      int totalNumValues = 0;
      for (int i = 0; i < rows; i++) {
        int numValues = r.nextInt(100) + 1;
        data[i] = new int[numValues];
        for (int j = 0; j < numValues; j++) {
          data[i][j] = r.nextInt(maxValue);
        }
        totalNumValues += numValues;
      }
      FixedBitMultiValueWriter writer = new FixedBitMultiValueWriter(file, rows, totalNumValues, maxBits);
      CustomBitSet bitSet = CustomBitSet.withBitLength(totalNumValues * maxBits);
      int numChunks = writer.getNumChunks();
      int[] chunkOffsets = new int[numChunks];
      int chunkId = 0;
      int offset = 0;
      int index = 0;
      for (int i = 0; i < rows; i++) {
        writer.setIntArray(i, data[i]);
        if (i % writer.getRowsPerChunk() == 0) {
          chunkOffsets[chunkId] = offset;
          chunkId = chunkId + 1;
        }
        offset += data[i].length;
        for (int j = 0; j < data[i].length; j++) {
          int value = data[i][j];
          for (int bitPos = maxBits - 1; bitPos >= 0; bitPos--) {
            if ((value & (1 << bitPos)) != 0) {
              bitSet.setBit(index * maxBits + (maxBits - bitPos - 1));
            }
          }
          index = index + 1;
        }
      }
      writer.close();
      LOGGER.trace("chunkOffsets: {}", Arrays.toString(chunkOffsets));
      //start validating the file
      RandomAccessFile raf = new RandomAccessFile(file, "r");

      Assert.assertEquals(raf.length(), writer.getTotalSize());
      DataInputStream dis = new DataInputStream(new FileInputStream(file));
      for (int i = 0; i < numChunks; i++) {
        Assert.assertEquals(dis.readInt(), chunkOffsets[i]);
      }
      int numBytesForBitmap = (totalNumValues + 7) / 8;
      Assert.assertEquals(writer.getBitsetSize(), numBytesForBitmap);
      byte[] bitsetBytes = new byte[numBytesForBitmap];
      dis.read(bitsetBytes);
      CustomBitSet customBit = CustomBitSet.withByteBuffer(numBytesForBitmap, ByteBuffer.wrap(bitsetBytes));
      offset = 0;
      LOGGER.trace(customBit.toString());

      for (int i = 0; i < rows; i++) {
        Assert.assertTrue(customBit.isBitSet(offset));
        offset += data[i].length;
      }
      byte[] byteArray = bitSet.toByteArray();

      LOGGER.trace("raf.length():" + raf.length());
      LOGGER.trace("getTotalSize:" + writer.getTotalSize());
      LOGGER.trace("getRawDataSize:" + writer.getRawDataSize());
      LOGGER.trace("getBitsetSize:" + writer.getBitsetSize());
      LOGGER.trace("getChunkOffsetHeaderSize:" + writer.getChunkOffsetHeaderSize());

      int dataLength = (int) (writer.getTotalSize() - writer.getChunkOffsetHeaderSize() - numBytesForBitmap);
      byte[] rawData = new byte[dataLength];
      // read the data segment that starts after the header.
      dis.read(rawData);
      Assert.assertEquals(rawData.length, byteArray.length);
      Assert.assertEquals(rawData, byteArray);
      raf.close();
      dis.close();
      file.delete();
      LOGGER.debug("END test maxBit:" + maxBits);
      maxBits = maxBits + 1;
      bitSet.close();
      customBit.close();
    }
  }
}
