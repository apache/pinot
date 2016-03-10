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
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class SegmentLocalFSDirectoryTest {
  private static Logger LOGGER = LoggerFactory.getLogger(SegmentLocalFSDirectoryTest.class);

  private static final File TEST_DIRECTORY = new File(SingleFileIndexDirectoryTest.class.toString());
  SegmentLocalFSDirectory segmentDirectory;
  SegmentMetadataImpl metadata;
  @BeforeClass
  public void setUp() {
    FileUtils.deleteQuietly(TEST_DIRECTORY);
    TEST_DIRECTORY.mkdirs();
    metadata = ColumnIndexDirectoryTestHelper.writeMetadata(SegmentVersion.v1);
    segmentDirectory = new SegmentLocalFSDirectory(TEST_DIRECTORY, metadata, ReadMode.mmap);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEST_DIRECTORY);
  }



  @Test
  public void testMultipleReadersNoWriter()
      throws Exception {
    SegmentLocalFSDirectory.Reader reader = segmentDirectory.createReader();
    Assert.assertNotNull(reader);
    SegmentLocalFSDirectory.Reader reader1 = segmentDirectory.createReader();
    Assert.assertNotNull(reader1);

    SegmentLocalFSDirectory.Writer writer = segmentDirectory.createWriter();
    Assert.assertNull(writer);
    reader.close();
    reader1.close();
  }

  @Test
  public void testExclusiveWrite()
      throws java.lang.Exception {
    SegmentLocalFSDirectory.Writer writer = segmentDirectory.createWriter();
    Assert.assertNotNull(writer);

    SegmentLocalFSDirectory.Reader reader2 = segmentDirectory.createReader();
    Assert.assertNull(reader2);

    SegmentLocalFSDirectory.Writer writer1 = segmentDirectory.createWriter();
    Assert.assertNull(writer1);
    writer.close();

    reader2 = segmentDirectory.createReader();
    Assert.assertNotNull(reader2);
    reader2.close();
  }

  private void loadData(PinotDataBuffer buffer) {
    int limit = (int) (buffer.size() / 4);
    for (int i = 0; i < limit; ++i) {
      buffer.putInt(i*4, 10000 + i);
    }
  }

  private void verifyData(PinotDataBuffer newDataBuffer) {
    int limit = (int)newDataBuffer.size() / 4;
    for (int i = 0; i < limit; i++) {
      Assert.assertEquals(newDataBuffer.getInt(i * 4), 10000 + i, "Failed to match at index: " + i);
    }

  }

  @Test
  public void testWriteAndReadBackData()
      throws java.lang.Exception {
    SegmentLocalFSDirectory.Writer writer = segmentDirectory.createWriter();
    Assert.assertNotNull(writer);
    PinotDataBuffer buffer = writer.newIndexFor("newColumn", ColumnIndexType.FORWARD_INDEX, 1024);
    loadData(buffer);
    writer.saveAndClose();

    SegmentDirectory.Reader reader = segmentDirectory.createReader();
    Assert.assertNotNull(reader);
    PinotDataBuffer newDataBuffer = reader.getIndexFor("newColumn", ColumnIndexType.FORWARD_INDEX);
    verifyData(newDataBuffer);
  }


}
