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
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.configuration.ConfigurationException;
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
  public void tearDown()
      throws Exception {
    segmentDirectory.close();
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
    try (SegmentLocalFSDirectory.Writer writer = segmentDirectory.createWriter() ) {
      Assert.assertNotNull(writer);
      PinotDataBuffer buffer = writer.newIndexFor("newColumn", ColumnIndexType.FORWARD_INDEX, 1024);
      loadData(buffer);
      writer.saveAndClose();
    }
    try (SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      Assert.assertNotNull(reader);
      PinotDataBuffer newDataBuffer = reader.getIndexFor("newColumn", ColumnIndexType.FORWARD_INDEX);
      verifyData(newDataBuffer);
    }
  }

  @Test
  public void testStarTree()
      throws IOException, ConfigurationException {
    Assert.assertFalse(segmentDirectory.hasStarTree());
    FileUtils.touch(new File(segmentDirectory.getPath().toFile(), V1Constants.STAR_TREE_INDEX_FILE));
    String data = "This is star tree";
    try (SegmentDirectory.Writer writer = segmentDirectory.createWriter();
        OutputStream starTreeOstream = writer.starTreeOutputStream() ) {
      starTreeOstream.write(data.getBytes());
    }
    try (SegmentDirectory.Reader reader = segmentDirectory.createReader();
        InputStream starTreeIstream = reader.getStarTreeStream()) {
      byte[] fileDataBytes = new byte[data.length()];
        starTreeIstream.read(fileDataBytes, 0, fileDataBytes.length);
      String fileData = new String(fileDataBytes);
      Assert.assertEquals(fileData, data);
    }
    try (SegmentDirectory.Writer writer = segmentDirectory.createWriter()) {
      writer.removeStarTree();
      Assert.assertFalse(segmentDirectory.hasStarTree());
    }
  }

  @Test
  public void testDirectorySize()
      throws IOException {
    // this test verifies that the segment size is returned correctly even if v3/ subdir
    // does not exist. We have not good way to test all the conditions since the
    // format converters are higher level modules that can not be used in this package
    // So, we do what we can do best....HACK HACK HACK
    File sizeTestDirectory = null;
    try {
       sizeTestDirectory = new File(SegmentLocalFSDirectoryTest.class.getName() + "-size_test");
      if (sizeTestDirectory.exists()) {
        FileUtils.deleteQuietly(sizeTestDirectory);
      }
      FileUtils.copyDirectoryToDirectory(segmentDirectory.getPath().toFile(), sizeTestDirectory);
      SegmentDirectory sizeSegment = SegmentLocalFSDirectory.createFromLocalFS(sizeTestDirectory, metadata, ReadMode.mmap);
      Assert.assertEquals(sizeSegment.getDiskSizeBytes(), segmentDirectory.getDiskSizeBytes());

      Assert.assertFalse(SegmentDirectoryPaths.segmentDirectoryFor(sizeTestDirectory, SegmentVersion.v3).exists());
      File v3SizeDir = new File(sizeTestDirectory, SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
      // the destination is not exactly v3 but does not matter
      FileUtils.copyDirectoryToDirectory(segmentDirectory.getPath().toFile(), v3SizeDir);
      SegmentDirectory sizeV3Segment = SegmentDirectory.createFromLocalFS(v3SizeDir, metadata, ReadMode.mmap);
      Assert.assertEquals(sizeSegment.getDiskSizeBytes(), sizeV3Segment.getDiskSizeBytes());

      // now drop v3
      FileUtils.deleteQuietly(v3SizeDir);
      v3SizeDir.mkdirs();
      // sizes still match because we get the size from the parent...
      Assert.assertEquals(sizeSegment.getDiskSizeBytes(), sizeV3Segment.getDiskSizeBytes());
    } finally {
      if (sizeTestDirectory != null) {
        FileUtils.deleteQuietly(sizeTestDirectory);
      }
    }
  }

}
