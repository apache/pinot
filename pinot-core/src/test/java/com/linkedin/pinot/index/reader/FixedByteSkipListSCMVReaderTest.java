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
package com.linkedin.pinot.index.reader;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.File;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.io.reader.impl.v1.FixedByteMultiValueReader;
import com.linkedin.pinot.core.io.writer.impl.v1.FixedByteSkipListMultiValueWriter;


public class FixedByteSkipListSCMVReaderTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(FixedByteSkipListSCMVReaderTest.class);

  @Test
  public void testSingleColMultiValue() throws Exception {
    String fileName = "test_single_col.dat";
    File f = new File(fileName);
    f.delete();
    int numDocs = 100;
    int maxMultiValues = 100;
    int[][] data = new int[numDocs][];
    Random r = new Random();
    int totalNumValues = 0;
    int[] startOffsets = new int[numDocs];
    int[] lengths = new int[numDocs];

    for (int i = 0; i < data.length; i++) {
      int numValues = r.nextInt(maxMultiValues) + 1;
      data[i] = new int[numValues];
      for (int j = 0; j < numValues; j++) {
        data[i][j] = r.nextInt();
      }
      startOffsets[i] = totalNumValues;
      lengths[i] = numValues;
      totalNumValues = totalNumValues + numValues;
    }

    for (int i = 0; i < data.length; i++) {
      LOGGER.trace("(" + i + "," + startOffsets[i] + "," + lengths[i] + ")");
      LOGGER.trace(",");
    }
    LOGGER.trace(Arrays.toString(startOffsets));
    LOGGER.trace(Arrays.toString(lengths));
    FixedByteSkipListMultiValueWriter writer = new FixedByteSkipListMultiValueWriter(f, numDocs, totalNumValues, Integer.SIZE / 8);

    for (int i = 0; i < data.length; i++) {
      writer.setIntArray(i, data[i]);
    }
    writer.close();

    int[] readValues = new int[maxMultiValues];
    PinotDataBuffer heapBuffer = PinotDataBuffer.fromFile(f, ReadMode.heap, FileChannel.MapMode.READ_ONLY, "heap-testing");
    FixedByteMultiValueReader heapReader = new FixedByteMultiValueReader(heapBuffer, numDocs, totalNumValues, Integer.SIZE / 8);
    for (int i = 0; i < data.length; i++) {
      int numValues = heapReader.getIntArray(i, readValues);
      Assert.assertEquals(numValues, data[i].length);
      for (int j = 0; j < numValues; j++) {
        Assert.assertEquals(readValues[j], data[i][j], "");
      }
    }

    heapReader.close();
    heapBuffer.close();


    PinotDataBuffer mmapBuffer = PinotDataBuffer.fromFile(f, ReadMode.mmap, FileChannel.MapMode.READ_ONLY, "mmap-testing");
    FixedByteMultiValueReader mmapReader = new FixedByteMultiValueReader(mmapBuffer, numDocs, totalNumValues, Integer.SIZE / 8);
    for (int i = 0; i < data.length; i++) {
      int numValues = mmapReader.getIntArray(i, readValues);
      Assert.assertEquals(numValues, data[i].length);
      for (int j = 0; j < numValues; j++) {
        Assert.assertEquals(readValues[j], data[i][j], "");
      }
    }

    mmapReader.close();
    mmapBuffer.close();

    f.delete();
  }
}
