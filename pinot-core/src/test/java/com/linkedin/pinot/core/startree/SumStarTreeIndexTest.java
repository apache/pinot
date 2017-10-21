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

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * This test generates a Star-Tree segment with random data, and ensures that SUM results computed using star-tree index
 * operator are the same as the results computed by scanning raw docs.
 */
public class SumStarTreeIndexTest extends BaseStarTreeIndexTest {
  private static final String DATA_DIR = System.getProperty("java.io.tmpdir") + File.separator + "SumStarTreeIndexTest";
  private static final String SEGMENT_NAME = "starTreeSegment";
  private static final String[] HARD_CODED_QUERIES = new String[]{
      "SELECT SUM(m1) FROM T",
      "SELECT SUM(m1) FROM T WHERE d1 = 'd1-v1'",
      "SELECT SUM(m1) FROM T WHERE d1 <> 'd1-v1'",
      "SELECT SUM(m1) FROM T WHERE d1 BETWEEN 'd1-v1' AND 'd1-v3'",
      "SELECT SUM(m1) FROM T WHERE d1 IN ('d1-v1', 'd1-v2')",
      "SELECT SUM(m1) FROM T WHERE d1 IN ('d1-v1', 'd1-v2') AND d2 NOT IN ('d2-v1')",
      "SELECT SUM(m1) FROM T GROUP BY d1",
      "SELECT SUM(m1) FROM T GROUP BY d1, d2",
      "SELECT SUM(m1) FROM T WHERE d1 = 'd1-v2' GROUP BY d1",
      "SELECT SUM(m1) FROM T WHERE d1 BETWEEN 'd1-v1' AND 'd1-v3' GROUP BY d2",
      "SELECT SUM(m1) FROM T WHERE d1 = 'd1-v2' GROUP BY d2, d3",
      "SELECT SUM(m1) FROM T WHERE d1 <> 'd1-v1' GROUP BY d2",
      "SELECT SUM(m1) FROM T WHERE d1 IN ('d1-v1', 'd1-v2') GROUP BY d2",
      "SELECT SUM(m1) FROM T WHERE d1 IN ('d1-v1', 'd1-v2') AND d2 NOT IN ('d2-v1') GROUP BY d3",
      "SELECT SUM(m1) FROM T WHERE d1 IN ('d1-v1', 'd1-v2') AND d2 NOT IN ('d2-v1') GROUP BY d3, d4"
  };

  private File _onHeapIndexDir;
  private File _offHeapIndexDir;

  @Override
  protected String[] getHardCodedQueries() {
    return HARD_CODED_QUERIES;
  }

  @Override
  protected List<String> getMetricColumns() {
    // Test against all metric columns
    return _segment.getSegmentMetadata().getSchema().getMetricNames();
  }

  @Override
  protected Map<List<Integer>, List<Double>> compute(Operator filterOperator) {
    filterOperator.open();
    BlockDocIdIterator docIdIterator = filterOperator.nextBlock().getBlockDocIdSet().iterator();

    Map<List<Integer>, List<Double>> results = new HashMap<>();
    int docId;
    while ((docId = docIdIterator.next()) != Constants.EOF) {
      // Array of dictionary Ids (zero-length array for non-group-by query)
      List<Integer> groupKeys = new ArrayList<>(_numGroupByColumns);
      for (int i = 0; i < _numGroupByColumns; i++) {
        _groupByValIterators[i].skipTo(docId);
        groupKeys.add(_groupByValIterators[i].nextIntVal());
      }

      List<Double> sums = results.get(groupKeys);
      if (sums == null) {
        sums = new ArrayList<>(_numMetricColumns);
        for (int i = 0; i < _numMetricColumns; i++) {
          sums.add(0.0);
        }
        results.put(groupKeys, sums);
      }
      for (int i = 0; i < _numMetricColumns; i++) {
        _metricValIterators[i].skipTo(docId);
        int dictId = _metricValIterators[i].nextIntVal();
        sums.set(i, sums.get(i) + _metricDictionaries[i].getDoubleValue(dictId));
      }
    }
    filterOperator.close();

    return results;
  }

  @BeforeClass
  void setUp() throws Exception {
    String onHeapDir = DATA_DIR + File.separator + "onHeap";
    StarTreeIndexTestSegmentHelper.buildSegment(onHeapDir, SEGMENT_NAME, false);
    _onHeapIndexDir = new File(onHeapDir, SEGMENT_NAME);
    String offHeapDir = DATA_DIR + File.separator + "offHeap";
    FileUtils.copyDirectory(new File(onHeapDir), new File(offHeapDir));
    _offHeapIndexDir = new File(offHeapDir, SEGMENT_NAME);
    StarTreeSerDe.convertStarTreeFormatIfNeeded(_offHeapIndexDir, StarTreeFormatVersion.OFF_HEAP);
  }

  @Test
  public void testQueries() throws Exception {
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setStarTreeVersion(StarTreeFormatVersion.ON_HEAP);
    _segment = ColumnarSegmentLoader.load(_onHeapIndexDir, indexLoadingConfig);
    Assert.assertEquals(_segment.getStarTree().getVersion(), StarTreeFormatVersion.ON_HEAP);
    testHardCodedQueries();

    indexLoadingConfig.setStarTreeVersion(StarTreeFormatVersion.OFF_HEAP);
    indexLoadingConfig.setReadMode(ReadMode.heap);
    _segment = ColumnarSegmentLoader.load(_offHeapIndexDir, indexLoadingConfig);
    Assert.assertEquals(_segment.getStarTree().getVersion(), StarTreeFormatVersion.OFF_HEAP);
    testHardCodedQueries();

    indexLoadingConfig.setReadMode(ReadMode.mmap);
    _segment = ColumnarSegmentLoader.load(_offHeapIndexDir, indexLoadingConfig);
    Assert.assertEquals(_segment.getStarTree().getVersion(), StarTreeFormatVersion.OFF_HEAP);
    testHardCodedQueries();
  }

  @Test
  public void testOffHeapStarTree() throws Exception {
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setStarTreeVersion(StarTreeFormatVersion.ON_HEAP);
    _segment = ColumnarSegmentLoader.load(_onHeapIndexDir, indexLoadingConfig);
    StarTreeInterf onHeapStarTree = _segment.getStarTree();

    // Test against off-heap star-tree loaded in heap mode
    indexLoadingConfig.setStarTreeVersion(StarTreeFormatVersion.OFF_HEAP);
    indexLoadingConfig.setReadMode(ReadMode.heap);
    _segment = ColumnarSegmentLoader.load(_offHeapIndexDir, indexLoadingConfig);
    StarTreeInterf offHeapStarTree = _segment.getStarTree();
    compareMetadata(onHeapStarTree, offHeapStarTree);
    compareTrees(onHeapStarTree.getRoot(), offHeapStarTree.getRoot());

    // Test against off-heap star-tree loaded in mmap mode
    indexLoadingConfig.setReadMode(ReadMode.mmap);
    _segment = ColumnarSegmentLoader.load(_offHeapIndexDir, indexLoadingConfig);
    offHeapStarTree = _segment.getStarTree();
    compareMetadata(onHeapStarTree, offHeapStarTree);
    compareTrees(onHeapStarTree.getRoot(), offHeapStarTree.getRoot());
  }

  /**
   * Helper method to compare metadata information of the two star-trees:
   * <ul>
   *   <li>Number of nodes</li>
   *   <li>Dimension name to index map</li>
   * </ul>
   */
  private void compareMetadata(StarTreeInterf onHeapStarTree, StarTreeInterf offHeapStarTree) {
    Assert.assertEquals(offHeapStarTree.getNumNodes(), onHeapStarTree.getNumNodes(), "Number of nodes mis-match");
    Assert.assertEquals(offHeapStarTree.getDimensionNames(), onHeapStarTree.getDimensionNames(),
        "Dimension name to index map mis-match");
  }

  /**
   * Helper method to compare two star-trees.
   */
  private void compareTrees(StarTreeIndexNodeInterf onHeapRoot, StarTreeIndexNodeInterf offHeapRoot) {
    // Compare the root notes
    compareNodes(onHeapRoot, offHeapRoot);

    // Iterate over children of on-heap root
    Iterator<? extends StarTreeIndexNodeInterf> onHeapChildrenIterator = onHeapRoot.getChildrenIterator();
    while (onHeapChildrenIterator.hasNext()) {
      StarTreeIndexNodeInterf onHeapChild = onHeapChildrenIterator.next();
      StarTreeIndexNodeInterf offHeapChild = offHeapRoot.getChildForDimensionValue(onHeapChild.getDimensionValue());
      compareNodes(onHeapChild, offHeapChild);
    }
  }

  /**
   * Helper method to compare two nodes.
   */
  private void compareNodes(StarTreeIndexNodeInterf onHeapNode, StarTreeIndexNodeInterf offHeapNode) {
    Assert.assertEquals(offHeapNode.getDimensionName(), onHeapNode.getDimensionName(), "Dimension name mis-match");
    Assert.assertEquals(offHeapNode.getDimensionValue(), onHeapNode.getDimensionValue(), "Dimension value mis-match");
    Assert.assertEquals(offHeapNode.getStartDocumentId(), onHeapNode.getStartDocumentId(), "StartDocumentId mis-match");
    Assert.assertEquals(offHeapNode.getEndDocumentId(), offHeapNode.getEndDocumentId(), "EndDocumentId mis-match");
    Assert.assertEquals(offHeapNode.getAggregatedDocumentId(), onHeapNode.getAggregatedDocumentId(),
        "AggregatedDocumentId mis-match");
    Assert.assertEquals(offHeapNode.getNumChildren(), onHeapNode.getNumChildren(), "Number of children mis-match");
    Assert.assertEquals(offHeapNode.isLeaf(), onHeapNode.isLeaf(), "IsLeaf mist-match");
  }

  @AfterClass
  void tearDown() {
    FileUtils.deleteQuietly(new File(DATA_DIR));
  }
}
