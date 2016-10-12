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
package com.linkedin.pinot.index.writer;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.io.writer.impl.v1.FixedByteSkipListMultiValueWriter;
import com.linkedin.pinot.core.util.CustomBitSet;


public class FixedByteSkipListSCMVWriterTest {
  @Test
  public void testSingleColMultiValue() throws Exception {

    File file = new File("test_single_col_multi_value_writer.dat");
    file.delete();
    int rows = 100;
    int[][] data = new int[rows][];
    Random r = new Random();
    int totalNumValues = 0;
    for (int i = 0; i < rows; i++) {
      int numValues = r.nextInt(100) + 1;
      data[i] = new int[numValues];
      for (int j = 0; j < numValues; j++) {
        data[i][j] = r.nextInt();
      }
      totalNumValues += numValues;
    }

    FixedByteSkipListMultiValueWriter writer = new FixedByteSkipListMultiValueWriter(file, rows, totalNumValues, 4);
    int numChunks = writer.getNumChunks();
    int[] chunkOffsets = new int[numChunks];
    int chunkId = 0;
    int offset = 0;
    for (int i = 0; i < rows; i++) {
      writer.setIntArray(i, data[i]);
      if (i % writer.getDocsPerChunk() == 0) {
        chunkOffsets[chunkId] = offset;
        chunkId = chunkId + 1;
      }
      offset += data[i].length;
    }
    writer.close();

    DataInputStream dis = new DataInputStream(new FileInputStream(file));
    for (int i = 0; i < numChunks; i++) {
      Assert.assertEquals(dis.readInt(), chunkOffsets[i]);
    }
    int numBytesForBitmap = (totalNumValues + 7) / 8;
    byte[] bitsetBytes = new byte[numBytesForBitmap];
    dis.read(bitsetBytes);
    CustomBitSet customBit = CustomBitSet.withByteBuffer(numBytesForBitmap, ByteBuffer.wrap(bitsetBytes));
    offset = 0;
//    System.out.println(customBit);
//    RandomAccessFile raf = new RandomAccessFile(file, "r");
//    System.out.println("totalNumValues:" + totalNumValues);
//    System.out.println("totalDocs:" + rows);
//    System.out.println("raf.length():" + raf.length());
//    System.out.println("numChunks:" + numChunks);
//    System.out.println("getTotalSize:" + writer.getTotalSize());
//    System.out.println("getRawDataSize:" + writer.getRawDataSize());
//    System.out.println("getBitsetSize:" + writer.getBitsetSize());
//    System.out.println("getChunkOffsetHeaderSize:" + writer.getChunkOffsetHeaderSize());
    for (int i = 0; i < rows; i++) {
      Assert.assertTrue(customBit.isBitSet(offset));
      for (int j = 0; j < data[i].length; j++) {
        Assert.assertEquals(dis.readInt(), data[i][j]);
      }
      offset += data[i].length;
    }
    dis.close();
    file.delete();
//    raf.close();
    customBit.close();
  }
}
