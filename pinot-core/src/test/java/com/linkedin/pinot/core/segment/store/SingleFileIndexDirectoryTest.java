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
package com.linkedin.pinot.core.segment.store;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class SingleFileIndexDirectoryTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SingleFileIndexDirectoryTest.class);
  private static final File TEST_DIRECTORY = new File(SingleFileIndexDirectoryTest.class.toString());

  private File segmentDir;
  private SegmentMetadataImpl segmentMetadata;
  static ColumnIndexType[] indexTypes;
  static final long ONE_KB = 1024L;
  static final long ONE_MB = ONE_KB * ONE_KB;
  static final long ONE_GB = ONE_MB * ONE_KB;

  static {
    indexTypes = ColumnIndexType.values();
  }

  @BeforeMethod
  public void setUpTest()
      throws IOException, ConfigurationException {
    segmentDir = new File(TEST_DIRECTORY, "segmentDirectory");

    if (segmentDir.exists()) {
      FileUtils.deleteQuietly(segmentDir);
    }
    segmentDir.mkdirs();
    segmentDir.deleteOnExit();
    writeMetadata();
  }

  @AfterMethod
  public void tearDownTest()
      throws IOException {
  }

  @AfterClass
  public void tearDownClass() {
    FileUtils.deleteQuietly(TEST_DIRECTORY);
  }

  void writeMetadata() throws ConfigurationException {
    SegmentMetadataImpl meta = mock(SegmentMetadataImpl.class);
    when(meta.getVersion()).thenReturn(SegmentVersion.v3.toString());
    segmentMetadata = meta;
  }

  @Test
  public void testWithEmptyDir()
      throws Exception {
    // segmentDir does not have anything to begin with
    Assert.assertEquals(segmentDir.list().length, 0);
    SingleFileIndexDirectory columnDirectory =
        new SingleFileIndexDirectory(segmentDir, segmentMetadata, ReadMode.mmap);
    PinotDataBuffer writtenBuffer = columnDirectory.newDictionaryBuffer("foo", 1024);
    String data = new String("This is a test string");
    final byte[] dataBytes = data.getBytes();
    int pos = 0;
    for (byte b : dataBytes) {
      writtenBuffer.putByte(pos++, b);
    }
    writtenBuffer.close();

    when(segmentMetadata.getAllColumns()).thenReturn(
        new HashSet<String>(Arrays.asList("foo"))
    );
    try (SingleFileIndexDirectory directoryReader =
        new SingleFileIndexDirectory(segmentDir, segmentMetadata, ReadMode.mmap);
        PinotDataBuffer readBuffer = directoryReader.getDictionaryBufferFor("foo")) {
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
    ColumnIndexDirectory columnDirectory = null;
    try {
      columnDirectory = new SingleFileIndexDirectory(segmentDir, segmentMetadata, ReadMode.mmap);
      ColumnIndexDirectoryTestHelper.verifyMultipleReads(columnDirectory, "foo", 6);
    } finally {
      if (columnDirectory != null) {
        columnDirectory.close();
      }
    }

  }

  private void testMultipleRW(ReadMode readMode, int numIter, long size)
      throws Exception {

    try (SingleFileIndexDirectory columnDirectory = new SingleFileIndexDirectory(segmentDir, segmentMetadata, readMode)) {
      ColumnIndexDirectoryTestHelper.performMultipleWrites(columnDirectory, "foo", size, numIter);
    }

    // now read and validate data
    try(ColumnIndexDirectory columnDirectory =
          new SingleFileIndexDirectory(segmentDir, segmentMetadata, readMode) ) {
      ColumnIndexDirectoryTestHelper.verifyMultipleReads(columnDirectory, "foo", numIter);
    }
  }


  @Test(expectedExceptions = RuntimeException.class)
  public void testWriteExisting()
      throws Exception {
    {
      try ( SingleFileIndexDirectory columnDirectory = new SingleFileIndexDirectory(segmentDir, segmentMetadata, ReadMode.mmap);
          PinotDataBuffer buffer = columnDirectory.newDictionaryBuffer("column1", 1024)) {
      }
    }
    {
      try (SingleFileIndexDirectory columnDirectory =
          new SingleFileIndexDirectory(segmentDir, segmentMetadata, ReadMode.mmap);
          PinotDataBuffer repeatBuffer = columnDirectory.newDictionaryBuffer("column1", 1024)) {

      }
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testMissingIndex()
      throws IOException, ConfigurationException {
    try (SingleFileIndexDirectory columnDirectory =
        new SingleFileIndexDirectory(segmentDir, segmentMetadata, ReadMode.mmap)) {
      try (PinotDataBuffer buffer = columnDirectory.getDictionaryBufferFor("column1")) {

      }
    }
  }

  @Test(expectedExceptions =  UnsupportedOperationException.class)
  public void testRemoveIndex()
      throws IOException, ConfigurationException {
    try (SingleFileIndexDirectory sfd = new SingleFileIndexDirectory(segmentDir, segmentMetadata, ReadMode.mmap)) {
      try (PinotDataBuffer buffer = sfd.newDictionaryBuffer("col1", 1024)) { }
      Assert.assertFalse(sfd.isIndexRemovalSupported());
      sfd.removeIndex("col1", ColumnIndexType.DICTIONARY);
    }
  }


}
