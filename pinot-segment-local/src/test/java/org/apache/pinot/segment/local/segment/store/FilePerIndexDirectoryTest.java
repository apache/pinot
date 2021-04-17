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
package org.apache.pinot.segment.local.segment.store;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.local.segment.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class FilePerIndexDirectoryTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), FilePerIndexDirectoryTest.class.toString());

  private SegmentMetadataImpl segmentMetadata;

  static final long ONE_KB = 1024L;
  static final long ONE_MB = ONE_KB * ONE_KB;
  static final long ONE_GB = ONE_MB * ONE_KB;

  @BeforeMethod
  public void setUp() throws IOException {
    TestUtils.ensureDirectoriesExistAndEmpty(TEMP_DIR);
    segmentMetadata = ColumnIndexDirectoryTestHelper.writeMetadata(SegmentVersion.v1);
  }

  @AfterMethod
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testEmptyDirectory() throws Exception {
    Assert.assertEquals(0, TEMP_DIR.list().length, TEMP_DIR.list().toString());
    try (FilePerIndexDirectory fpiDir = new FilePerIndexDirectory(TEMP_DIR, segmentMetadata, ReadMode.heap);
        PinotDataBuffer buffer = fpiDir.newBuffer("col1", ColumnIndexType.DICTIONARY, 1024)) {
      Assert.assertEquals(1, TEMP_DIR.list().length, TEMP_DIR.list().toString());

      buffer.putLong(0, 0xbadfadL);
      buffer.putInt(8, 51);
      // something at random location
      buffer.putInt(101, 55);
    }

    Assert.assertEquals(1, TEMP_DIR.list().length);

    try (FilePerIndexDirectory colDir = new FilePerIndexDirectory(TEMP_DIR, segmentMetadata, ReadMode.mmap);
        PinotDataBuffer readBuffer = colDir.getBuffer("col1", ColumnIndexType.DICTIONARY)) {
      Assert.assertEquals(readBuffer.getLong(0), 0xbadfadL);
      Assert.assertEquals(readBuffer.getInt(8), 51);
      Assert.assertEquals(readBuffer.getInt(101), 55);
    }
  }

  @Test
  public void testMmapLargeBuffer() throws Exception {
    testMultipleRW(ReadMode.mmap, 6, 3L * ONE_MB);
  }

  @Test
  public void testLargeRWDirectBuffer() throws Exception {
    testMultipleRW(ReadMode.heap, 6, 3L * ONE_MB);
  }

  @Test
  public void testReadModeChange() throws Exception {
    // first verify it all works for one mode
    testMultipleRW(ReadMode.heap, 6, 100 * ONE_MB);
    try (ColumnIndexDirectory columnDirectory = new FilePerIndexDirectory(TEMP_DIR, segmentMetadata, ReadMode.mmap)) {
      ColumnIndexDirectoryTestHelper.verifyMultipleReads(columnDirectory, "foo", 6);
    }
  }

  private void testMultipleRW(ReadMode readMode, int numIter, long size) throws Exception {
    try (FilePerIndexDirectory columnDirectory = new FilePerIndexDirectory(TEMP_DIR, segmentMetadata, readMode)) {
      ColumnIndexDirectoryTestHelper.performMultipleWrites(columnDirectory, "foo", size, numIter);
    }
    // now read and validate data
    try (FilePerIndexDirectory columnDirectory = new FilePerIndexDirectory(TEMP_DIR, segmentMetadata, readMode)) {
      ColumnIndexDirectoryTestHelper.verifyMultipleReads(columnDirectory, "foo", numIter);
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testWriteExisting() throws Exception {
    try (FilePerIndexDirectory columnDirectory = new FilePerIndexDirectory(TEMP_DIR, segmentMetadata, ReadMode.mmap)) {
      columnDirectory.newBuffer("column1", ColumnIndexType.DICTIONARY, 1024);
    }
    try (FilePerIndexDirectory columnDirectory = new FilePerIndexDirectory(TEMP_DIR, segmentMetadata, ReadMode.mmap)) {
      columnDirectory.newBuffer("column1", ColumnIndexType.DICTIONARY, 1024);
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testMissingIndex() throws IOException {
    try (FilePerIndexDirectory fpiDirectory = new FilePerIndexDirectory(TEMP_DIR, segmentMetadata, ReadMode.mmap)) {
      fpiDirectory.getBuffer("noSuchColumn", ColumnIndexType.DICTIONARY);
    }
  }

  @Test
  public void testHasIndex() throws IOException {
    try (FilePerIndexDirectory fpiDirectory = new FilePerIndexDirectory(TEMP_DIR, segmentMetadata, ReadMode.mmap)) {
      PinotDataBuffer buffer = fpiDirectory.newBuffer("foo", ColumnIndexType.DICTIONARY, 1024);
      buffer.putInt(0, 100);
      Assert.assertTrue(fpiDirectory.hasIndexFor("foo", ColumnIndexType.DICTIONARY));
    }
  }

  @Test
  public void testRemoveIndex() throws IOException {
    try (FilePerIndexDirectory fpi = new FilePerIndexDirectory(TEMP_DIR, segmentMetadata, ReadMode.mmap)) {
      fpi.newBuffer("col1", ColumnIndexType.FORWARD_INDEX, 1024);
      fpi.newBuffer("col2", ColumnIndexType.DICTIONARY, 100);
      Assert.assertTrue(fpi.getFileFor("col1", ColumnIndexType.FORWARD_INDEX).exists());
      Assert.assertTrue(fpi.getFileFor("col2", ColumnIndexType.DICTIONARY).exists());
      Assert.assertTrue(fpi.isIndexRemovalSupported());
      fpi.removeIndex("col1", ColumnIndexType.FORWARD_INDEX);
      Assert.assertFalse(fpi.getFileFor("col1", ColumnIndexType.FORWARD_INDEX).exists());
    }
  }
}
