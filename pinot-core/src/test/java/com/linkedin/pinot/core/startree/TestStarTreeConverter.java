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
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Test for Star Tree v1-v2 converter.
 */
public class TestStarTreeConverter {
  private static final String TMP_DIR = System.getProperty("java.io.tmpdir");
  private static final String SEGMENT_DIR_NAME = TMP_DIR + File.separator + "StarTreeFormatConverter";
  private static final String SEGMENT_NAME = "starTreeSegment";

  private IndexSegment _segment;
  private StarTreeInterf _starTreeV1;
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
    _starTreeV1 = _segment.getStarTree();
    _indexDir = new File(SEGMENT_DIR_NAME, SEGMENT_NAME);

  }

  /**
   * This test builds a star-tree in v1 format, and then performs multiple
   * format conversions, and asserts that all conversions work as expected.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   */
  @Test
  public void testConverter()
      throws IOException, ClassNotFoundException {

    // Convert from V1 to V1, this should be no-op.
    StarTreeSerDe.convertStarTreeFormatIfNeeded(_indexDir, StarTreeFormatVersion.V1);
    assertStarTreeVersion(_indexDir, StarTreeFormatVersion.V1);

    // Convert from V1 to V2.
    StarTreeSerDe.convertStarTreeFormatIfNeeded(_indexDir, StarTreeFormatVersion.V2);
    assertStarTreeVersion(_indexDir, StarTreeFormatVersion.V2);

    // Convert from V2 to V2, this should be no-op
    StarTreeSerDe.convertStarTreeFormatIfNeeded(_indexDir, StarTreeFormatVersion.V2);
    assertStarTreeVersion(_indexDir, StarTreeFormatVersion.V2);

    // Convert from V2 to V1.
    StarTreeSerDe.convertStarTreeFormatIfNeeded(_indexDir, StarTreeFormatVersion.V1);
    assertStarTreeVersion(_indexDir, StarTreeFormatVersion.V1);
  }

  private void assertStarTreeVersion(File indexDir, StarTreeFormatVersion expectedVersion)
      throws IOException {
    File starTreeFile = new File(indexDir, V1Constants.STAR_TREE_INDEX_FILE);
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
