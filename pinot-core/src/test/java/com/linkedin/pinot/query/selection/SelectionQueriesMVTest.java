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
package com.linkedin.pinot.query.selection;

import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import java.io.File;
import java.io.Serializable;
import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;


/**
 * The <code>SelectionQueriesMVTest</code> class extends the {@link BaseSelectionQueriesTest} class to provide tests
 * on multi-value data set.
 */
public class SelectionQueriesMVTest extends BaseSelectionQueriesTest {
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "SelectionQueriesMVTest");

  private IndexSegment _indexSegment;
  private Map<String, BaseOperator> _dataSourceMap;
  private Selection _selectionOnlyQuery;
  private Selection _selectionOrderByQuery;

  @BeforeClass
  public void setup() throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    // Get resource file path.
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    Assert.assertNotNull(resource);
    String filePath = resource.getFile();

    // Build the index segment.
    SegmentGeneratorConfig config = SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath),
        INDEX_DIR, "daysSinceEpoch", TimeUnit.DAYS, "testTable");
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();

    // Load the index segment.
    File indexSegmentDir = new File(INDEX_DIR, driver.getSegmentName());
    _indexSegment = ColumnarSegmentLoader.load(indexSegmentDir, ReadMode.heap);

    // Initialize the data source map.
    _dataSourceMap = new HashMap<>();
    _dataSourceMap.put("column1", _indexSegment.getDataSource("column1"));
    _dataSourceMap.put("column2", _indexSegment.getDataSource("column2"));
    _dataSourceMap.put("column6", _indexSegment.getDataSource("column6"));
    _dataSourceMap.put("count", _indexSegment.getDataSource("count"));

    // Build the selection ONLY query.
    _selectionOnlyQuery = new Selection();
    _selectionOnlyQuery.setSelectionColumns(Arrays.asList("column6", "column1", "count"));
    _selectionOnlyQuery.setSize(10);

    // Build the selection ORDER BY query.
    _selectionOrderByQuery = new Selection();
    _selectionOrderByQuery.setSelectionColumns(Arrays.asList("column6", "column1", "count"));
    _selectionOrderByQuery.setSize(10);
    SelectionSort selectionSort1 = new SelectionSort();
    selectionSort1.setColumn("column2");
    SelectionSort selectionSort2 = new SelectionSort();
    selectionSort2.setColumn("column1");
    _selectionOrderByQuery.setSelectionSortSequence(Arrays.asList(selectionSort1, selectionSort2));
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Override
  IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  Map<String, BaseOperator> getDataSourceMap() {
    return _dataSourceMap;
  }

  @Override
  Selection getSelectionOnlyQuery() {
    return _selectionOnlyQuery;
  }

  @Override
  Selection getSelectionOrderByQuery() {
    return _selectionOrderByQuery;
  }

  private static final Object[][] EXPECTED_SELECTION_ONLY_RESULT =
      {
          {890282370, 2147483647, 890662862},
          {890282370, 2147483647, 890662862},
          {890282370, 2147483647, 890662862},
          {890282370, 2147483647, 890662862},
          {890282370, 2147483647, 890662862},
          {890282370, 2147483647, 890662862},
          {890282370, 2147483647, 890662862},
          {890282370, 2147483647, 890662862},
          {890282370, 2147483647, 890662862},
          {972569181, 593959, 890662862}
      };

  private static final Object[][] EXPECTED_SELECTION_ORDER_BY_RESULT =
      {
          {2147279568, 1522220030, 1392, 890662862},
          {2147339302, 1727078583, 1115287, 890662862},
          {2147344388, 796772300, 3312947, 890662862},
          {2147344388, 796772300, 3312947, 890662862},
          {2147393520, 1503666160, 2147483647, 890662862},
          {2147393520, 2065226350, 2147483647, 890662862},
          {2147434110, 241701456, 557935, 890662862},
          {2147434110, 241701456, 557935, 890662862},
          {2147434110, 241701456, 557935, 890662862},
          {2147434110, 241701456, 557935, 890662862}
      };

  @Override
  void verifySelectionOnlyResult(Collection<Serializable[]> selectionOnlyResult) {
    Assert.assertEquals(selectionOnlyResult.size(), 10);

    List<Serializable[]> list = (List<Serializable[]>) selectionOnlyResult;
    for (int i = 0; i < 10; i++) {
      Serializable[] row = list.get(i);
      Assert.assertEquals(row[0], EXPECTED_SELECTION_ONLY_RESULT[i][0]);
      // The second element is a multi-value column with one value inside.
      Assert.assertEquals(((int[])(row[1]))[0], EXPECTED_SELECTION_ONLY_RESULT[i][1]);
      Assert.assertEquals(row[2], EXPECTED_SELECTION_ONLY_RESULT[i][2]);
    }
  }

  @Override
  void verifySelectionOrderByResult(Collection<Serializable[]> selectionOrderByResult) {
    Assert.assertEquals(selectionOrderByResult.size(), 10);

    // Need to cache the result in the priority queue, and add them back in the end.
    PriorityQueue<Serializable[]> priorityQueue = (PriorityQueue<Serializable[]>) selectionOrderByResult;
    List<Serializable[]> list = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Serializable[] row = priorityQueue.poll();
      Assert.assertEquals(row[0], EXPECTED_SELECTION_ORDER_BY_RESULT[i][0]);
      Assert.assertEquals(row[1], EXPECTED_SELECTION_ORDER_BY_RESULT[i][1]);
      // The third element is a multi-value column with one value inside.
      Assert.assertEquals(((int[])row[2])[0], EXPECTED_SELECTION_ORDER_BY_RESULT[i][2]);
      Assert.assertEquals(row[3], EXPECTED_SELECTION_ORDER_BY_RESULT[i][3]);
      list.add(row);
    }

    // Add back the result in the priority queue.
    priorityQueue.addAll(list);
  }

  @Override
  void verifyReducedSelectionOrderByResult(Collection<Serializable[]> reducedSelectionOrderByResult) {
    Assert.assertEquals(reducedSelectionOrderByResult.size(), 10);

    for (Serializable[] row : reducedSelectionOrderByResult) {
      Assert.assertEquals(row[0], EXPECTED_SELECTION_ORDER_BY_RESULT[9][0]);
      Assert.assertEquals(row[1], EXPECTED_SELECTION_ORDER_BY_RESULT[9][1]);
      // The third element is a multi-value column with one value inside.
      Assert.assertEquals(((int[])row[2])[0], EXPECTED_SELECTION_ORDER_BY_RESULT[9][2]);
      Assert.assertEquals(row[3], EXPECTED_SELECTION_ORDER_BY_RESULT[9][3]);
    }
  }
}
