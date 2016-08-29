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

import com.google.common.collect.HashBiMap;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;


/**
 * Tests for StarTreeV2 class.
 */
public class TestStarTreeV2 extends BaseSumStarTreeIndexTest {

  private static final String SEGMENT_NAME = "starTreeSegment";
  private static final String SEGMENT_V2_NAME = "starTreeV2Segment";
  private static final String SEGMENT_DIR_NAME = "/tmp/star-tree-index";
  private static final String SEGMENT_V2_DIR_NAME = "/tmp/star-tree-v2-index";
  private static final String STAR_TREE_V2_FILE_NAME = "starTreeV2";

  private IndexSegment _segment;
  private IndexSegment _segmentV2;
  private StarTreeInterf _starTreeV1;
  private File _starTreeV2File;
  private Schema _schema;

  /**
   * Build the star tree.
   * @throws Exception
   */
  @BeforeSuite
  void setup()
      throws Exception {
    StarTreeIndexTestSegmentHelper.buildSegment(SEGMENT_DIR_NAME, SEGMENT_NAME, false);
    _segment = StarTreeIndexTestSegmentHelper.loadSegment(SEGMENT_DIR_NAME, SEGMENT_NAME);
    _starTreeV1 = _segment.getStarTree();

    _starTreeV2File = new File(SEGMENT_DIR_NAME, STAR_TREE_V2_FILE_NAME);
    StarTreeSerDe.writeTreeV2(_starTreeV1, _starTreeV2File);

    // Build the StarTreeV2 segment
    _schema = StarTreeIndexTestSegmentHelper.buildSegment(SEGMENT_V2_DIR_NAME, SEGMENT_V2_NAME, true);
    _segmentV2 = StarTreeIndexTestSegmentHelper.loadSegment(SEGMENT_V2_DIR_NAME, SEGMENT_V2_NAME);
  }

  /**
   * Cleanup any temporary files and directories.
   * @throws IOException
   */
  @AfterSuite
  void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(new File(SEGMENT_DIR_NAME));
    FileUtils.deleteDirectory(new File(SEGMENT_V2_DIR_NAME));
  }

  /**
   * This test ensures that the StarTreeV2 in heap mode has the exact same
   * contents as the original implementation of star tree.
   *
   * @throws Exception
   */
  @Test
  public void testHeapMode()
      throws IOException {
    StarTreeV2 starTreeV2 = new StarTreeV2(_starTreeV2File, ReadMode.heap);
    compareMetadata(_starTreeV1, starTreeV2);
    compareTrees(_starTreeV1.getRoot(), starTreeV2.getRoot());
  }

  /**
   * This test ensures that the StarTreeV2 in mmap mode has the exact same
   * contents as the original implementation of star tree.
   *
   * @throws Exception
   */
  @Test
  public void testMmapMode()
      throws IOException {
    StarTreeV2 starTreeV2 = new StarTreeV2(_starTreeV2File, ReadMode.mmap);

    compareMetadata(_starTreeV1, starTreeV2);
    compareTrees(_starTreeV1.getRoot(), starTreeV2.getRoot());
  }

  /**
   * This test runs a set of queries on the StarTreeV2 segment, and ensures
   * correctness of query results.
   *
   */
  @Test
  public void testQueries() {
    testHardCodedQueries(_segmentV2, _schema);
  }

  /**
   * Compare metadata information of the two trees:
   * - Number of nodes
   * - Dimension name to index map
   *
   * @param starTreeV1
   * @param starTreeV2
   */
  private void compareMetadata(StarTreeInterf starTreeV1, StarTreeInterf starTreeV2) {
    Assert.assertEquals(starTreeV2.getNumNodes(), _starTreeV1.getNumNodes(), "Number of nodes mist-match");

    HashBiMap<String, Integer> dimensionNameToIndexMap1 = starTreeV1.getDimensionNameToIndexMap();
    HashBiMap<String, Integer> dimensionNameToIndexMap2 = starTreeV2.getDimensionNameToIndexMap();
    Assert.assertEquals(dimensionNameToIndexMap2.size(), dimensionNameToIndexMap1.size(),
        "Dimension name index map size mis-match");

    for (Map.Entry<String, Integer> entry : dimensionNameToIndexMap1.entrySet()) {
      String key = entry.getKey();
      Integer value = entry.getValue();

      Assert.assertTrue(dimensionNameToIndexMap2.containsKey(key), "Missing dimension " + key);
      Assert.assertEquals(dimensionNameToIndexMap2.get(key), value, "Dimension index mist-match");
    }
  }

  /**
   * Helper method to compare two star trees:
   * - Compares all the values for the two nodes.
   * - Ensures that either both or neither nodes have a star child.
   * - All children of V1 can be found by iterating over children of V2 and vice-versa.
   * - Performs DFS invoking the same test for all node pairs.
   * @param rootV1
   * @param rootV2
   */
  private void compareTrees(StarTreeIndexNodeInterf rootV1, StarTreeIndexNodeInterf rootV2) {
    // Compare the nodes.
    compareNodes(rootV1, rootV2);

    // Assert star child either exists in both or does not exist at all.
    StarTreeIndexNodeInterf star1 = rootV1.getChildForDimensionValue(StarTreeIndexNodeInterf.ALL);
    StarTreeIndexNodeInterf star2 = rootV2.getChildForDimensionValue(StarTreeIndexNodeInterf.ALL);
    Assert.assertEquals((star2 == null), (star1 == null));

    // Assert both nodes have same number of children.
    Assert.assertEquals(rootV1.getNumChildren(), rootV1.getNumChildren(), "Mis-match in number of children");

    // Iterate over children of V2 and assert they are the same as children of V1.
    int numChildren = 0;
    Iterator<? extends StarTreeIndexNodeInterf> childrenIterator = rootV2.getChildrenIterator();
    while (childrenIterator.hasNext()) {
      StarTreeIndexNodeInterf childV2 = childrenIterator.next();
      StarTreeIndexNodeInterf childV1 = rootV1.getChildForDimensionValue(childV2.getDimensionValue());

      Assert.assertNotNull(childV1);
      compareNodes(childV1, childV2);
      numChildren++;
    }
    Assert.assertEquals(rootV2.getNumChildren(), numChildren, "Mis-match in number of children");

    // Now iterate over children of V1 and assert they are the same as children of V2.
    childrenIterator = rootV1.getChildrenIterator();
    while (childrenIterator.hasNext()) {
      StarTreeIndexNodeInterf childV1 = childrenIterator.next();
      StarTreeIndexNodeInterf childV2 = rootV2.getChildForDimensionValue(childV1.getDimensionValue());
      Assert.assertNotNull(childV2);
      compareTrees(childV1, childV2);
    }
  }

  /**
   * Helper method to compare and assert that all values for the two nodes match.
   * @param nodeV1
   * @param nodeV2
   */
  private void compareNodes(StarTreeIndexNodeInterf nodeV1, StarTreeIndexNodeInterf nodeV2) {
    Assert.assertEquals(nodeV2.getDimensionName(), nodeV1.getDimensionName(), "Dimension name mis-match");
    Assert.assertEquals(nodeV2.getDimensionValue(), nodeV1.getDimensionValue(), "Dimension value mis-match");
    Assert.assertEquals(nodeV2.getStartDocumentId(), nodeV1.getStartDocumentId(), "StartDocumentId mis-match");
    Assert.assertEquals(nodeV2.getEndDocumentId(), nodeV2.getEndDocumentId(), "EndDocumentId mis-match");
    Assert.assertEquals(nodeV2.getAggregatedDocumentId(), nodeV1.getAggregatedDocumentId(),
        "AggregatedDocumentId mis-match");
    Assert.assertEquals(nodeV2.getNumChildren(), nodeV1.getNumChildren(), "Number of children mis-match");
    Assert.assertEquals(nodeV2.isLeaf(), nodeV1.isLeaf(), "IsLeaf mist-match");
  }
}
