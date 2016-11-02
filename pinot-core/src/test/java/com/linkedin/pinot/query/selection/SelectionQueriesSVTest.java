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
 * The <code>SelectionQueriesSVTest</code> class extends the {@link BaseSelectionQueriesTest} class to provide tests
 * on single-value data set.
 */
public class SelectionQueriesSVTest extends BaseSelectionQueriesTest {
  private static final String AVRO_DATA = "data/test_sample_data.avro";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "SelectionQueriesSVTest");

  private IndexSegment _indexSegment;
  private Map<String, DataSource> _dataSourceMap;
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
        INDEX_DIR, "time_day", TimeUnit.DAYS, "testTable");
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();

    // Load the index segment.
    File indexSegmentDir = new File(INDEX_DIR, driver.getSegmentName());
    _indexSegment = ColumnarSegmentLoader.load(indexSegmentDir, ReadMode.heap);

    // Initialize the data source map.
    _dataSourceMap = new HashMap<>();
    _dataSourceMap.put("column11", _indexSegment.getDataSource("column11"));
    _dataSourceMap.put("column12", _indexSegment.getDataSource("column12"));
    _dataSourceMap.put("column13", _indexSegment.getDataSource("column13"));
    _dataSourceMap.put("met_impressionCount", _indexSegment.getDataSource("met_impressionCount"));

    // Build the selection ONLY query.
    _selectionOnlyQuery = new Selection();
    _selectionOnlyQuery.setSelectionColumns(Arrays.asList("column12", "column11", "met_impressionCount"));
    _selectionOnlyQuery.setSize(10);

    // Build the selection ORDER BY query.
    _selectionOrderByQuery = new Selection();
    _selectionOrderByQuery.setSelectionColumns(Arrays.asList("column12", "column11", "met_impressionCount"));
    _selectionOrderByQuery.setSize(10);
    SelectionSort selectionSort1 = new SelectionSort();
    selectionSort1.setColumn("column13");
    SelectionSort selectionSort2 = new SelectionSort();
    selectionSort2.setColumn("column11");
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
  Map<String, DataSource> getDataSourceMap() {
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
          {"i", "jgn", 4955241829510629137L},
          {"i", "lVH", 6240989492723764727L},
          {"i", "kWZ", 4955241829510629137L},
          {"i", "pm", 6240989492723764727L},
          {"i", "GF", 8637957270245933828L},
          {"i", "kB", 8637957270245933828L},
          {"i", "BQ", 8310347835142446717L},
          {"i", "YO", 4955241829510629137L},
          {"i", "RI", 8310347835142446717L},
          {"i", "RI", 6240989492723764727L}
      };

  private static final Object[][] EXPECTED_SELECTION_ORDER_BY_RESULT =
      {
          {"zz", "i", "k", 6994898124486825607L},
          {"zz", "i", "GF", 4955241829510629137L},
          {"zz", "i", "nt", 6240989492723764727L},
          {"zz", "i", "EF", 8637957270245933828L},
          {"zz", "i", "FL", 6240989492723764727L},
          {"zz", "i", "k", 8637957270245933828L},
          {"zz", "i", "RI", 6240989492723764727L},
          {"zz", "i", "Zi", 4955241829510629137L},
          {"zz", "i", "sp", 6240989492723764727L},
          {"zzWY", "U", "db", 6240989492723764727L}
      };

  @Override
  void verifySelectionOnlyResult(Collection<Serializable[]> selectionOnlyResult) {
    Assert.assertEquals(selectionOnlyResult.size(), 10);

    List<Serializable[]> list = (List<Serializable[]>) selectionOnlyResult;
    for (int i = 0; i < 10; i++) {
      Serializable[] row = list.get(i);
      for (int j = 0; j < 3; j++) {
        Assert.assertEquals(row[j], EXPECTED_SELECTION_ONLY_RESULT[i][j]);
      }
    }
  }

  @Override
  void verifySelectionOrderByResult(Collection<Serializable[]> selectionOrderByResult) {
    Assert.assertEquals(selectionOrderByResult.size(), 10);

    // Need to cache the result in the priority queue, and add them back in the end.
    List<Serializable[]> list = new ArrayList<>();
    PriorityQueue<Serializable[]> priorityQueue = (PriorityQueue<Serializable[]>) selectionOrderByResult;
    for (int i = 0; i < 10; i++) {
      Serializable[] row = priorityQueue.poll();
      for (int j = 0; j < 4; j++) {
        Assert.assertEquals(row[j], EXPECTED_SELECTION_ORDER_BY_RESULT[i][j]);
      }
      list.add(row);
    }

    // Add back the result in the priority queue.
    priorityQueue.addAll(list);
  }

  @Override
  void verifyReducedSelectionOrderByResult(Collection<Serializable[]> reducedSelectionOrderByResult) {
    Assert.assertEquals(reducedSelectionOrderByResult.size(), 10);

    for (Serializable[] row : reducedSelectionOrderByResult) {
      for (int i = 0; i < 4; i++) {
        Assert.assertEquals(row[i], EXPECTED_SELECTION_ORDER_BY_RESULT[9][i]);
      }
    }
  }
}
