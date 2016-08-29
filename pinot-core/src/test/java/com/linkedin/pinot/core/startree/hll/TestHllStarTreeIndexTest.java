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
package com.linkedin.pinot.core.startree.hll;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.startree.StarTreeIndexTestSegmentHelper;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

/**
 * This test generates a Star-Tree segment with random data, and ensures that
 * aggregation results computed using star-tree index operator are the same as
 * aggregation results computed by scanning raw docs.
 */
public class TestHllStarTreeIndexTest extends BaseHllStarTreeIndexTest {

  private static final String SEGMENT_NAME = "starTreeSegment";
  private static final String SEGMENT_DIR_NAME = "/tmp/star-tree-index";

  private IndexSegment _segment;
  private Schema _schema;

  @BeforeSuite
  void setup() throws Exception {
    _schema = StarTreeIndexTestSegmentHelper.buildSegmentWithHll(SEGMENT_DIR_NAME, SEGMENT_NAME, HLL_CONFIG);
    _segment = StarTreeIndexTestSegmentHelper.loadSegment(SEGMENT_DIR_NAME, SEGMENT_NAME);
  }

  @AfterSuite
  void tearDown() throws IOException {
    FileUtils.deleteDirectory(new File(SEGMENT_DIR_NAME));
  }

  /**
   * This test ensures that the aggregation result computed using the star-tree index operator
   * is the same as computed by scanning raw-docs, for a hard-coded set of queries.
   *
   * @throws Exception
   */
  @Test
  public void test() throws Exception {
    testHardCodedQueries(_segment, _schema);
  }
}
