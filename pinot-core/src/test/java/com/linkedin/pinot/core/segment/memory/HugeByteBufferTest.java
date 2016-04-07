package com.linkedin.pinot.core.segment.memory;

import java.nio.ByteBuffer;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;


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
public class HugeByteBufferTest {

  @Test
  public void testLoadGet() {
    HugeByteBuffer buffer = HugeByteBuffer.allocateDirect(3L * PinotDataBufferTest.ONE_GB);
    PinotDataBufferTest.loadVerifyAllTypes(buffer);
    buffer.close();

  }

  @Test
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

}