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

package com.linkedin.pinot.core.segment.store;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PinotDataBufferTest {
  private static Logger LOGGER = LoggerFactory.getLogger(PinotDataBufferTest.class);
  static Random random = new Random(System.currentTimeMillis());

  int[] generateNumbers(int count, int max) {
    int[] data = new int[count];
    if (max < 0) {
      max = Integer.MAX_VALUE;
    }

    for (int i = 0; i < count; i++) {
      data[i] = random.nextInt(max);
    }
    return data;
  }

  @Test
  public void testReadWriteFile()
      throws IOException {
    final int[] data = generateNumbers(1_000_000, -1);
    File file = new File(this.getClass().getName() + ".data");
    file.deleteOnExit();

    int size = data.length * 4;
    PinotDataBuffer buffer = PinotDataBuffer.fromFile(file, 0, size, ReadMode.mmap,
        FileChannel.MapMode.READ_WRITE, "testing");
    int pos = 0;
    for (int val : data) {
      buffer.putInt(pos * 4, val);
      ++pos;
    }
    buffer.close();

    PinotDataBuffer readBuf = PinotDataBuffer.fromFile(file, ReadMode.mmap, FileChannel.MapMode.READ_WRITE, "testing");
    pos = 0;
    for (int i = 0; i < data.length; ++i, ++pos) {
      Assert.assertEquals(data[i], readBuf.getInt(pos * 4));
    }
    readBuf.close();

    PinotDataBuffer heapReadBuf = PinotDataBuffer.fromFile(file, ReadMode.heap, FileChannel.MapMode.READ_ONLY, "testing");
    pos = 0;
    for (int i = 0; i < data.length; ++i, ++pos) {
      Assert.assertEquals(data[i], heapReadBuf.getInt(pos * 4));
    }
    heapReadBuf.close();
  }


}
