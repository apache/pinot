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

package com.linkedin.pinot.core.indexsegment.utils;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import com.linkedin.pinot.core.io.readerwriter.RealtimeIndexOffHeapMemoryManager;
import com.linkedin.pinot.core.io.writer.impl.MmapMemoryManager;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;


public class MmapMemoryManagerTest {

  private String _tmpDir;

  @BeforeSuite
  public void setUp() {
    _tmpDir = System.getProperty("java.io.tmpdir") + "/" + MmapMemoryManagerTest.class.getSimpleName();
    File dir = new File(_tmpDir);
    FileUtils.deleteQuietly(dir);
    dir.mkdir();
    dir.deleteOnExit();
  }

  @AfterSuite
  public void tearDown() {
    new File(_tmpDir).delete();
  }

  @Test
  public void testLargeBlocks() throws Exception {
    final String segmentName = "someSegment";
    RealtimeIndexOffHeapMemoryManager memoryManager = new MmapMemoryManager(_tmpDir, segmentName);
    final long s1 = 1024*1024*1024;
    final long s2 = 1000;
    final String col1 = "col1";
    final String col2 = "col2";
    final byte value = 34;
    PinotDataBuffer buf1 = memoryManager.allocate(s1, col1);

    // Verify that we can write to and read from the buffer
    ByteBuffer b1 = buf1.toDirectByteBuffer(0, (int) s1);
    b1.putLong(0, s1);
    Assert.assertEquals(b1.getLong(0), s1);
    b1.put((int) s1 - 1, value);
    Assert.assertEquals(b1.get((int)s1-1), value);

    PinotDataBuffer buf2 = memoryManager.allocate(s2, col2);
    ByteBuffer b2 = buf2.toDirectByteBuffer(0, (int) s2);
    b2.putLong(0, s2);
    Assert.assertEquals(b2.getLong(0), s2);

    File dir = new File(_tmpDir);
    File[] files = dir.listFiles();
    Assert.assertEquals(files.length, 2);

    Arrays.sort(files, new Comparator<File>() {
      @Override
      public int compare(File o1, File o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });

    String fileName = files[0].getName();
    Assert.assertTrue(fileName.contains(segmentName));

    fileName = files[1].getName();
    Assert.assertTrue(fileName.contains(segmentName));

    buf1.close();
    buf2.close();

    memoryManager.close();

    Assert.assertEquals(new File(_tmpDir).listFiles().length, 0);
  }

  @Test
  public void testSmallBlocksForSameColumn() throws Exception {
    final String segmentName = "someSegment";
    RealtimeIndexOffHeapMemoryManager memoryManager = new MmapMemoryManager(_tmpDir, segmentName);
    final long s1 = 500;
    final long s2 = 1000;
    final String col1 = "col1";
    PinotDataBuffer buf1 = memoryManager.allocate(s1, col1);

    PinotDataBuffer buf2 = memoryManager.allocate(s2, col1);

    ByteBuffer b1 = buf1.toDirectByteBuffer(0, (int) s1);
    b1.putLong(0, s1);

    ByteBuffer b2 = buf2.toDirectByteBuffer(0, (int) s2);
    b2.putLong(0, s2);

    Assert.assertEquals(b1.getLong(0), s1);
    Assert.assertEquals(b2.getLong(0), s2);

    File dir = new File(_tmpDir);
    Assert.assertEquals(dir.listFiles().length, 1);

    buf1.close();
    buf2.close();

    memoryManager.close();

    Assert.assertEquals(dir.listFiles().length, 0);
  }

  @Test
  public void testCornerConditions() throws Exception {
    final String segmentName = "someSegment";
    RealtimeIndexOffHeapMemoryManager memoryManager = new MmapMemoryManager(_tmpDir, segmentName);
    final long s1 = 512*1024*1024 - 1;
    final long s2 = 1;
    final long s3 = 100*1024*1024;
    final String colName = "col";
    final byte v1 = 56;
    final byte v2 = 11;
    final byte v3 = 32;

    PinotDataBuffer b1 = memoryManager.allocate(s1, colName);
    ByteBuffer bb1 = b1.toDirectByteBuffer(0, (int) s1);
    bb1.put((int)s1-1, v1);
    PinotDataBuffer b2 = memoryManager.allocate(s2, colName);
    ByteBuffer bb2 = b2.toDirectByteBuffer(0, (int) s2);
    bb2.put((int)s2-1, v2);
    PinotDataBuffer b3 = memoryManager.allocate(s3, colName);
    ByteBuffer bb3 = b3.toDirectByteBuffer(0, (int) s3);
    bb3.put(0, v3);
    Assert.assertEquals(bb1.get((int)s1-1), v1);
    Assert.assertEquals(bb2.get((int)s2-1), v2);
    Assert.assertEquals(bb3.get(0), v3);
  }
}
