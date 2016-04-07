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
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.File;
import java.io.IOException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class FilePerIndexDirectoryTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SingleFileIndexDirectoryTest.class);
  private static final File TEST_DIRECTORY = new File(FilePerIndexDirectoryTest.class.toString());

  private File segmentDir;
  private SegmentMetadataImpl segmentMetadata;

  static final int ONE_KB = 1024;
  static final int ONE_MB = ONE_KB * ONE_KB;
  static final int ONE_GB = ONE_MB * ONE_KB;

  @BeforeMethod
  public void setUpTest()
      throws IOException, ConfigurationException {
    segmentDir = new File(TEST_DIRECTORY, "segmentDirectory");

    if (segmentDir.exists()) {
      FileUtils.deleteQuietly(segmentDir);
    }
    if (segmentDir.exists()) {
      throw new RuntimeException("directory exists");
    }
    segmentDir.mkdirs();
    segmentMetadata = ColumnIndexDirectoryTestHelper.writeMetadata(SegmentVersion.v1);

  }

  @AfterMethod
  public void tearDownTest()
      throws IOException {
    FileUtils.deleteQuietly(segmentDir);
    Assert.assertFalse(segmentDir.exists());
  }

  @Test
  public void testEmptyDirectory()
      throws Exception {
    FilePerIndexDirectory fpiDir = new FilePerIndexDirectory(segmentDir, segmentMetadata, ReadMode.heap);
    Assert.assertEquals(0, segmentDir.list().length);
    PinotDataBuffer buffer = fpiDir.newDictionaryBuffer("col1", 1024);
    buffer.putLong(0, 0xbadfadL);
    buffer.putInt(8, 51);
    // something at random location
    buffer.putInt(101, 55);


    buffer.close();
    fpiDir.close();

    Assert.assertEquals(1, segmentDir.list().length);

    FilePerIndexDirectory colDir = new FilePerIndexDirectory(segmentDir, segmentMetadata, ReadMode.mmap);
    PinotDataBuffer readBuffer = colDir.getDictionaryBufferFor("col1");
    Assert.assertEquals(readBuffer.getLong(0), 0xbadfadL);
    Assert.assertEquals(readBuffer.getInt(8), 51);
    Assert.assertEquals(readBuffer.getInt(101), 55);
  }

  @Test
  public void testMmapLargeBuffer()
      throws Exception {
    testMultipleRW(ReadMode.mmap, 6, 100 * ONE_MB);
  }

  @Test
  public void testLargeRWDirectBuffer()
      throws Exception {
    testMultipleRW(ReadMode.heap, 6, 10 * ONE_MB);
  }

  @Test
  public void testReadModeChange()
      throws Exception {
    // first verify it all works for one mode
    testMultipleRW(ReadMode.heap, 6, 10 * ONE_MB);
    ColumnIndexDirectory columnDirectory = null;
    try {
      columnDirectory = new FilePerIndexDirectory(segmentDir, segmentMetadata, ReadMode.mmap);
      ColumnIndexDirectoryTestHelper.verifyMultipleReads(columnDirectory, "foo", 6);
    } finally {
      if (columnDirectory != null) {
        columnDirectory.close();
      }
    }

  }

  private void testMultipleRW(ReadMode readMode, int numIter, int size)
      throws Exception {
    //final int ONE_GB = 1024 * 1024 * 1024;
    // for testing
    FilePerIndexDirectory columnDirectory = null;
    try {
      columnDirectory = new FilePerIndexDirectory(segmentDir, segmentMetadata, readMode);
      ColumnIndexDirectoryTestHelper.performMultipleWrites(columnDirectory, "foo", size, numIter);
    } finally {
      if (columnDirectory != null) {
        columnDirectory.close();
        columnDirectory = null;
      }
    }
    System.out.println(segmentDir.getAbsoluteFile());
    // now read and validate data
    try {
      columnDirectory = new FilePerIndexDirectory(segmentDir, segmentMetadata, readMode);
      ColumnIndexDirectoryTestHelper.verifyMultipleReads(columnDirectory, "foo", numIter);
    } finally {
      if (columnDirectory != null) {
        columnDirectory.close();
      }
    }
  }


  @Test(expectedExceptions = RuntimeException.class)
  public void testWriteExisting()
      throws Exception {
    {
      FilePerIndexDirectory columnDirectory = new FilePerIndexDirectory(segmentDir, segmentMetadata, ReadMode.mmap);
      try (PinotDataBuffer buffer = columnDirectory.newDictionaryBuffer("column1", 1024)) {
      }
      columnDirectory.close();
    }
    {
      FilePerIndexDirectory columnDirectory =
          new FilePerIndexDirectory(segmentDir, segmentMetadata, ReadMode.mmap);
      try (PinotDataBuffer repeatBuffer = columnDirectory.newDictionaryBuffer("column1", 1024)) {

      }
    }
  }
  @Test (expectedExceptions = IOException.class)
  public void testMissingIndex()
      throws IOException {
    FilePerIndexDirectory fpiDirectory =
        new FilePerIndexDirectory(segmentDir, segmentMetadata, ReadMode.mmap);
    PinotDataBuffer buffer = fpiDirectory.getDictionaryBufferFor("noSuchColumn");
  }

}
