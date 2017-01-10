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
package com.linkedin.pinot.core.startree;

import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Test for Star Tree OnHeap-OffHeap converter.
 */
public class TestStarTreeConverter {
  private static final String TMP_DIR = System.getProperty("java.io.tmpdir");
  private static final String SEGMENT_DIR_NAME = TMP_DIR + File.separator + "StarTreeFormatConverter";
  private static final String SEGMENT_NAME = "starTreeSegment";

  private IndexSegment _segment;
  private File _indexDir;

  /**
   * Build the star tree index
   * @throws Exception
   */
  @BeforeClass
  public void setup()
      throws Exception {
    StarTreeIndexTestSegmentHelper.buildSegment(SEGMENT_DIR_NAME, SEGMENT_NAME, false);
    _segment = StarTreeIndexTestSegmentHelper.loadSegment(SEGMENT_DIR_NAME, SEGMENT_NAME);
    _indexDir = new File(new File(SEGMENT_DIR_NAME, SEGMENT_NAME), SegmentVersion.v3.toString());
  }

  /**
   * This test builds a star-tree in on-heap format, and then performs multiple
   * format conversions, and asserts that all conversions work as expected.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   */
  @Test
  public void testConverter()
      throws IOException, ClassNotFoundException {

    // Convert from ON_HEAP to ON_HEAP, this should be no-op.
    StarTreeSerDe.convertStarTreeFormatIfNeeded(_indexDir, StarTreeFormatVersion.ON_HEAP);
    assertStarTreeVersion(_indexDir, StarTreeFormatVersion.ON_HEAP);

    // Convert from ON_HEAP to OFF_HEAP.
    StarTreeSerDe.convertStarTreeFormatIfNeeded(_indexDir, StarTreeFormatVersion.OFF_HEAP);
    assertStarTreeVersion(_indexDir, StarTreeFormatVersion.OFF_HEAP);

    // Convert from OFF_HEAP to OFF_HEAP, this should be no-op
    StarTreeSerDe.convertStarTreeFormatIfNeeded(_indexDir, StarTreeFormatVersion.OFF_HEAP);
    assertStarTreeVersion(_indexDir, StarTreeFormatVersion.OFF_HEAP);

    // Convert from OFF_HEAP to ON_HEAP.
    StarTreeSerDe.convertStarTreeFormatIfNeeded(_indexDir, StarTreeFormatVersion.ON_HEAP);
    assertStarTreeVersion(_indexDir, StarTreeFormatVersion.ON_HEAP);
  }

  private void assertStarTreeVersion(File indexDir, StarTreeFormatVersion expectedVersion)
      throws IOException {
    File starTreeFile = new File(_indexDir, V1Constants.STAR_TREE_INDEX_FILE);
    Assert.assertEquals(StarTreeSerDe.getStarTreeVersion(starTreeFile), expectedVersion);
  }

  /**
   * Cleanup any temporary files and directories.
   * @throws IOException
   */
  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(new File(SEGMENT_DIR_NAME));
  }
}
