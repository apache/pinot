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

package com.linkedin.pinot.core.segment.memory;

import com.linkedin.pinot.common.segment.ReadMode;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HugeByteBufferTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(HugeByteBufferTest.class);

  @Test(enabled = false)
  public void testLoadGet() {
    HugeByteBuffer buffer = HugeByteBuffer.allocateDirect(3L * PinotDataBufferTest.ONE_GB);
    PinotDataBufferTest.loadVerifyAllTypes(buffer);
    buffer.close();

  }

  @Test(enabled = false)
  public void testBulkGetPut() {
    HugeByteBuffer buffer = HugeByteBuffer.allocateDirect(3L * PinotDataBufferTest.ONE_GB);

    byte[] data = new byte[PinotDataBufferTest.ONE_GB];
    ByteBuffer db = ByteBuffer.wrap(data);
    Random random = new Random();
    int elemCount = data.length / 4;
    for (int i = 0; i < elemCount; i++) {
      db.putInt(random.nextInt());
    }
    long offset = 1800 * 1024 * 1024;
    buffer.readFrom(data, offset);
    for (int i = 0; i < elemCount; i++) {
      Assert.assertEquals(buffer.getInt(offset), db.getInt(i * 4));
      offset += 4;
    }
    buffer.close();
  }

  @Test(enabled = false)
  public void testRead()
      throws IOException {
    HugeByteBuffer buffer = HugeByteBuffer.allocateDirect(3L * PinotDataBufferTest.ONE_GB);
    buffer.putInt(PinotDataBufferTest.ONE_GB - 2, 0x12345678);

    LOGGER.debug(String.valueOf(buffer.getShort(PinotDataBufferTest.ONE_GB)));
    LOGGER.debug(Integer.toHexString(buffer.getInt(PinotDataBufferTest.ONE_GB - 2)));
    File tempFilePath = Files.createTempFile(HugeByteBufferTest.class.getName() + "-test", ".tmp").toFile();
    tempFilePath.deleteOnExit();

    HugeByteBuffer mapBuffer =
        (HugeByteBuffer) PinotDataBuffer.fromFile(tempFilePath, 0, 3L * PinotDataBufferTest.ONE_GB, ReadMode.mmap,
            FileChannel.MapMode.READ_WRITE, "allocContext");
    mapBuffer.putInt(PinotDataBufferTest.ONE_GB - 2, 0x12345678);
    LOGGER.debug(Integer.toHexString(buffer.getShort(PinotDataBufferTest.ONE_GB - 2)));
    LOGGER.debug(Integer.toHexString(buffer.getInt(PinotDataBufferTest.ONE_GB - 2)));
  }
}
