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
import java.util.Arrays;
import java.util.HashSet;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.ColumnIndexDirectory;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.util.TestUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SingleFileIndexDirectoryTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), SingleFileIndexDirectoryTest.class.toString());

  private SegmentMetadataImpl segmentMetadata;
  static ColumnIndexType[] indexTypes;
  static final long ONE_KB = 1024L;
  static final long ONE_MB = ONE_KB * ONE_KB;
  static final long ONE_GB = ONE_MB * ONE_KB;

  static {
    indexTypes = ColumnIndexType.values();
  }

  @BeforeMethod
  public void setUp()
      throws IOException {
    TestUtils.ensureDirectoriesExistAndEmpty(TEMP_DIR);
    writeMetadata();
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  void writeMetadata() {
    SegmentMetadataImpl meta = Mockito.mock(SegmentMetadataImpl.class);
    Mockito.when(meta.getVersion()).thenReturn(SegmentVersion.v3.toString());
    segmentMetadata = meta;
  }

  @Test
  public void testWithEmptyDir()
      throws Exception {
    // segmentDir does not have anything to begin with
    Assert.assertEquals(TEMP_DIR.list().length, 0);
    SingleFileIndexDirectory columnDirectory = new SingleFileIndexDirectory(TEMP_DIR, segmentMetadata, ReadMode.mmap);
    PinotDataBuffer writtenBuffer = columnDirectory.newBuffer("foo", ColumnIndexType.DICTIONARY, 1024);
    String data = "This is a test string";
    final byte[] dataBytes = data.getBytes();
    int pos = 0;
    for (byte b : dataBytes) {
      writtenBuffer.putByte(pos++, b);
    }
    writtenBuffer.close();

    Mockito.when(segmentMetadata.getAllColumns()).thenReturn(new HashSet<String>(Arrays.asList("foo")));
    try (SingleFileIndexDirectory directoryReader = new SingleFileIndexDirectory(TEMP_DIR, segmentMetadata,
        ReadMode.mmap); PinotDataBuffer readBuffer = directoryReader.getBuffer("foo", ColumnIndexType.DICTIONARY)) {
      Assert.assertEquals(1024, readBuffer.size());
      int length = dataBytes.length;
      for (int i = 0; i < length; i++) {
        byte b = readBuffer.getByte(i);
        Assert.assertEquals(dataBytes[i], b);
      }
    }
  }

  @Test
  public void testMmapLargeBuffer()
      throws Exception {
    testMultipleRW(ReadMode.mmap, 6, 4L * ONE_MB);
  }

  @Test
  public void testLargeRWDirectBuffer()
      throws Exception {
    testMultipleRW(ReadMode.heap, 6, 3L * ONE_MB);
  }

  @Test
  public void testModeChange()
      throws Exception {
    // first verify it all works for one mode
    long size = 2L * ONE_MB;
    testMultipleRW(ReadMode.heap, 6, size);
    try (
        ColumnIndexDirectory columnDirectory = new SingleFileIndexDirectory(TEMP_DIR, segmentMetadata, ReadMode.mmap)) {
      ColumnIndexDirectoryTestHelper.verifyMultipleReads(columnDirectory, "foo", 6);
    }
  }

  private void testMultipleRW(ReadMode readMode, int numIter, long size)
      throws Exception {
    try (SingleFileIndexDirectory columnDirectory = new SingleFileIndexDirectory(TEMP_DIR, segmentMetadata, readMode)) {
      ColumnIndexDirectoryTestHelper.performMultipleWrites(columnDirectory, "foo", size, numIter);
    }

    // now read and validate data
    try (ColumnIndexDirectory columnDirectory = new SingleFileIndexDirectory(TEMP_DIR, segmentMetadata, readMode)) {
      ColumnIndexDirectoryTestHelper.verifyMultipleReads(columnDirectory, "foo", numIter);
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testWriteExisting()
      throws Exception {
    try (SingleFileIndexDirectory columnDirectory = new SingleFileIndexDirectory(TEMP_DIR, segmentMetadata,
        ReadMode.mmap)) {
      columnDirectory.newBuffer("column1", ColumnIndexType.DICTIONARY, 1024);
    }
    try (SingleFileIndexDirectory columnDirectory = new SingleFileIndexDirectory(TEMP_DIR, segmentMetadata,
        ReadMode.mmap)) {
      columnDirectory.newBuffer("column1", ColumnIndexType.DICTIONARY, 1024);
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testMissingIndex()
      throws IOException, ConfigurationException {
    try (SingleFileIndexDirectory columnDirectory = new SingleFileIndexDirectory(TEMP_DIR, segmentMetadata,
        ReadMode.mmap)) {
      columnDirectory.getBuffer("column1", ColumnIndexType.DICTIONARY);
    }
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testRemoveIndex()
      throws IOException, ConfigurationException {
    try (SingleFileIndexDirectory sfd = new SingleFileIndexDirectory(TEMP_DIR, segmentMetadata, ReadMode.mmap)) {
      sfd.newBuffer("col1", ColumnIndexType.DICTIONARY, 1024);
      Assert.assertFalse(sfd.isIndexRemovalSupported());
      sfd.removeIndex("col1", ColumnIndexType.DICTIONARY);
    }
  }
}
