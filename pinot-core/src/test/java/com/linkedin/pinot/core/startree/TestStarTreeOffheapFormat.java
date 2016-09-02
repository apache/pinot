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
 * Tests for StarTreeOffHeap class.
 */
public class TestStarTreeOffheapFormat extends BaseSumStarTreeIndexTest {

  private static final String SEGMENT_NAME = "starTreeSegment";
  private static final String SEGMENT_OFF_HEAP_NAME = "starTreeOffHeapSegment";
  private static final String SEGMENT_DIR_NAME = "/tmp/star-tree-index";
  private static final String SEGMENT_OFF_HEAP_DIR_NAME = "/tmp/star-tree-off-heap-index";
  private static final String STAR_TREE_OFF_HEAP_FILE_NAME = "starTreeOffHeap";

  private IndexSegment _segment;
  private IndexSegment _segmentOffHeap;
  private StarTreeInterf _starTreeOnHeap;
  private File _starTreeOffHeapFile;
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
    _starTreeOnHeap = _segment.getStarTree();

    _starTreeOffHeapFile = new File(SEGMENT_DIR_NAME, STAR_TREE_OFF_HEAP_FILE_NAME);
    StarTreeSerDe.writeTreeOffHeapFormat(_starTreeOnHeap, _starTreeOffHeapFile);

    // Build the StarTreeOffHeap segment
    _schema = StarTreeIndexTestSegmentHelper.buildSegment(SEGMENT_OFF_HEAP_DIR_NAME, SEGMENT_OFF_HEAP_NAME, true);
    _segmentOffHeap = StarTreeIndexTestSegmentHelper.loadSegment(SEGMENT_OFF_HEAP_DIR_NAME, SEGMENT_OFF_HEAP_NAME);
  }

  /**
   * Cleanup any temporary files and directories.
   * @throws IOException
   */
  @AfterSuite
  void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(new File(SEGMENT_DIR_NAME));
    FileUtils.deleteDirectory(new File(SEGMENT_OFF_HEAP_DIR_NAME));
  }

  /**
   * This test ensures that the StarTreeOffHeap in heap mode has the exact same
   * contents as the original implementation of star tree.
   *
   * @throws Exception
   */
  @Test
  public void testHeapMode()
      throws IOException {
    StarTreeOffHeap starTreeOffHeap = new StarTreeOffHeap(_starTreeOffHeapFile, ReadMode.heap);
    compareMetadata(_starTreeOnHeap, starTreeOffHeap);
    compareTrees(_starTreeOnHeap.getRoot(), starTreeOffHeap.getRoot());
  }

  /**
   * This test ensures that the StarTreeOffHeap in mmap mode has the exact same
   * contents as the original implementation of star tree.
   *
   * @throws Exception
   */
  @Test
  public void testMmapMode()
      throws IOException {
    StarTreeOffHeap starTreeOffHeap = new StarTreeOffHeap(_starTreeOffHeapFile, ReadMode.mmap);

    compareMetadata(_starTreeOnHeap, starTreeOffHeap);
    compareTrees(_starTreeOnHeap.getRoot(), starTreeOffHeap.getRoot());
  }

  /**
   * This test runs a set of queries on the StarTreeOffHeap segment, and ensures
   * correctness of query results.
   *
   */
  @Test
  public void testQueries() {
    testHardCodedQueries(_segmentOffHeap, _schema);
  }

  /**
   * Compare metadata information of the two trees:
   * - Number of nodes
   * - Dimension name to index map
   *
   * @param starTreeOnHeap
   * @param starTreeOffHeap
   */
  private void compareMetadata(StarTreeInterf starTreeOnHeap, StarTreeInterf starTreeOffHeap) {
    Assert.assertEquals(starTreeOffHeap.getNumNodes(), _starTreeOnHeap.getNumNodes(), "Number of nodes mist-match");

    HashBiMap<String, Integer> dimensionNameToIndexMap1 = starTreeOnHeap.getDimensionNameToIndexMap();
    HashBiMap<String, Integer> dimensionNameToIndexMap2 = starTreeOffHeap.getDimensionNameToIndexMap();
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
   * - All children of on-heap version can be found by iterating over children of off-heap version and vice-versa.
   * - Performs DFS invoking the same test for all node pairs.
   * @param rootOnheap
   * @param rootOffHeap
   */
  private void compareTrees(StarTreeIndexNodeInterf rootOnheap, StarTreeIndexNodeInterf rootOffHeap) {
    // Compare the nodes.
    compareNodes(rootOnheap, rootOffHeap);

    // Assert star child either exists in both or does not exist at all.
    StarTreeIndexNodeInterf star1 = rootOnheap.getChildForDimensionValue(StarTreeIndexNodeInterf.ALL);
    StarTreeIndexNodeInterf star2 = rootOffHeap.getChildForDimensionValue(StarTreeIndexNodeInterf.ALL);
    Assert.assertEquals((star2 == null), (star1 == null));

    // Assert both nodes have same number of children.
    Assert.assertEquals(rootOnheap.getNumChildren(), rootOnheap.getNumChildren(), "Mis-match in number of children");

    // Iterate over children of off-heap version and assert they are the same as children of on-heap version.
    int numChildren = 0;
    Iterator<? extends StarTreeIndexNodeInterf> childrenIterator = rootOffHeap.getChildrenIterator();
    while (childrenIterator.hasNext()) {
      StarTreeIndexNodeInterf childOffHeap = childrenIterator.next();
      StarTreeIndexNodeInterf childOnHeap = rootOnheap.getChildForDimensionValue(childOffHeap.getDimensionValue());

      Assert.assertNotNull(childOnHeap);
      compareNodes(childOnHeap, childOffHeap);
      numChildren++;
    }
    Assert.assertEquals(rootOffHeap.getNumChildren(), numChildren, "Mis-match in number of children");

    // Now iterate over children of on-heap version and assert they are the same as children of off-heap version.
    childrenIterator = rootOnheap.getChildrenIterator();
    while (childrenIterator.hasNext()) {
      StarTreeIndexNodeInterf childOnHeap = childrenIterator.next();
      StarTreeIndexNodeInterf childOffHeap = rootOffHeap.getChildForDimensionValue(childOnHeap.getDimensionValue());
      Assert.assertNotNull(childOffHeap);
      compareTrees(childOnHeap, childOffHeap);
    }
  }

  /**
   * Helper method to compare and assert that all values for the two nodes match.
   * @param nodeOnHeap
   * @param nodeOffHeap
   */
  private void compareNodes(StarTreeIndexNodeInterf nodeOnHeap, StarTreeIndexNodeInterf nodeOffHeap) {
    Assert.assertEquals(nodeOffHeap.getDimensionName(), nodeOnHeap.getDimensionName(), "Dimension name mis-match");
    Assert.assertEquals(nodeOffHeap.getDimensionValue(), nodeOnHeap.getDimensionValue(), "Dimension value mis-match");
    Assert.assertEquals(nodeOffHeap.getStartDocumentId(), nodeOnHeap.getStartDocumentId(), "StartDocumentId mis-match");
    Assert.assertEquals(nodeOffHeap.getEndDocumentId(), nodeOffHeap.getEndDocumentId(), "EndDocumentId mis-match");
    Assert.assertEquals(nodeOffHeap.getAggregatedDocumentId(), nodeOnHeap.getAggregatedDocumentId(),
        "AggregatedDocumentId mis-match");
    Assert.assertEquals(nodeOffHeap.getNumChildren(), nodeOnHeap.getNumChildren(), "Number of children mis-match");
    Assert.assertEquals(nodeOffHeap.isLeaf(), nodeOnHeap.isLeaf(), "IsLeaf mist-match");
  }
}
