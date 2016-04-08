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
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.io.reader.impl.FixedBitSingleValueMultiColReader;
import com.linkedin.pinot.core.io.writer.impl.FixedBitSingleValueMultiColWriter;


public class FixedBitRowColDataWriterReaderTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(FixedBitRowColDataWriterReaderTest.class);
  @Test
  public void testSingleColUnsigned() throws Exception {
    int maxBits = 1;
    while (maxBits < 32) {
      LOGGER.debug("START test maxBits:" + maxBits);
      final String fileName = getClass().getName() + "_single_col_fixed_bit_"
          + maxBits + ".dat";
      final File file = new File(fileName);
      file.delete();
      final int rows = 100;
      final int cols = 1;
      final int[] columnSizesInBits = new int[] { maxBits };
      final FixedBitSingleValueMultiColWriter writer = new FixedBitSingleValueMultiColWriter(
          file, rows, cols, columnSizesInBits, new boolean[] { true });
      final int[] data = new int[rows];
      final Random r = new Random();
      writer.open();
      final int maxValue = (int) Math.pow(2, maxBits);
      for (int i = 0; i < rows; i++) {
        data[i] = r.nextInt(maxValue) * ((Math.random() > .5) ? 1 : -1);
        writer.setInt(i, 0, data[i]);
      }
      writer.close();

      // Test heap mode
      PinotDataBuffer heapBuffer = PinotDataBuffer.fromFile(file, ReadMode.heap,
          FileChannel.MapMode.READ_ONLY, "testing");
      FixedBitSingleValueMultiColReader heapReader = new FixedBitSingleValueMultiColReader(heapBuffer, rows, cols,
          columnSizesInBits, new boolean[] { true });

      for (int i = 0; i < rows; i++) {
        Assert.assertEquals(heapReader.getInt(i, 0), data[i]);
      }
      heapReader.close();
      heapBuffer.close();

      // Test mmap mode
      PinotDataBuffer mmapBuffer = PinotDataBuffer.fromFile(file, ReadMode.mmap, FileChannel.MapMode.READ_ONLY, "mmap-testing");
      FixedBitSingleValueMultiColReader mmapReader =
          new FixedBitSingleValueMultiColReader(mmapBuffer, rows, cols, columnSizesInBits, new boolean[] { true });
      for (int i = 0; i < rows; i++) {
        Assert.assertEquals(mmapReader.getInt(i, 0), data[i]);
      }

      mmapReader.close();
      mmapBuffer.close();

      maxBits = maxBits + 1;
      file.delete();
    }
  }
}
