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
package com.linkedin.pinot.queries;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.manager.offline.OfflineSegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;


/**
 * The <code>BaseSingleValueQueriesTest</code> class sets up the index segment for the single-value queries test.
 * <p>There are totally 18 columns, 30000 records inside the original Avro file where 11 columns are selected to build
 * the index segment. Selected columns information are as following:
 * <ul>
 *   ColumnName, FieldType, DataType, Cardinality, IsSorted, HasInvertedIndex
 *   <li>column1, METRIC, INT, 6582, F, F</li>
 *   <li>column3, METRIC, INT, 21910, F, F</li>
 *   <li>column5, DIMENSION, STRING, 1, T, F</li>
 *   <li>column6, DIMENSION, INT, 608, F, T</li>
 *   <li>column7, DIMENSION, INT, 146, F, T</li>
 *   <li>column9, DIMENSION, INT, 1737, F, F</li>
 *   <li>column11, DIMENSION, STRING, 5, F, T</li>
 *   <li>column12, DIMENSION, STRING, 5, F, F</li>
 *   <li>column17, METRIC, INT, 24, F, T</li>
 *   <li>column18, METRIC, INT, 1440, F, T</li>
 *   <li>daysSinceEpoch, TIME, INT, 2, T, F</li>
 * </ul>
 */
public abstract class BaseSingleValueQueriesTest extends BaseQueriesTest {
  private static final String AVRO_DATA = "data" + File.separator + "test_data-sv.avro";
  private static final String SEGMENT_NAME = "testTable_126164076_167572854";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "SingleValueQueriesTest");

  // Hard-coded query filter.
  private static final String QUERY_FILTER =
      " WHERE column1 > 100000000"
          + " AND column3 BETWEEN 20000000 AND 1000000000"
          + " AND column5 = 'gFuH'"
          + " AND (column6 < 500000000 OR column11 NOT IN ('t', 'P'))"
          + " AND daysSinceEpoch = 126164076";

  private IndexSegment _indexSegment;
  // Contains 2 identical index segments.
  private List<SegmentDataManager> _segmentDataManagers;

  @BeforeTest
  public void buildSegment()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    // Get resource file path.
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    Assert.assertNotNull(resource);
    String filePath = resource.getFile();

    // Build the segment schema.
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addMetric("column1", FieldSpec.DataType.INT)
        .addMetric("column3", FieldSpec.DataType.INT)
        .addSingleValueDimension("column5", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column6", FieldSpec.DataType.INT)
        .addSingleValueDimension("column7", FieldSpec.DataType.INT)
        .addSingleValueDimension("column9", FieldSpec.DataType.INT)
        .addSingleValueDimension("column11", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column12", FieldSpec.DataType.STRING)
        .addMetric("column17", FieldSpec.DataType.INT)
        .addMetric("column18", FieldSpec.DataType.INT)
        .addTime("daysSinceEpoch", TimeUnit.DAYS, FieldSpec.DataType.INT)
        .build();

    // Create the segment generator config.
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(schema);
    segmentGeneratorConfig.setInputFilePath(filePath);
    segmentGeneratorConfig.setTableName("testTable");
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
    segmentGeneratorConfig.setInvertedIndexCreationColumns(
        Arrays.asList("column6", "column7", "column11", "column17", "column18"));

    // Build the index segment.
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();
  }

  @BeforeClass
  public void loadSegment()
      throws Exception {
    _indexSegment = ColumnarSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.heap);
    _segmentDataManagers = Arrays.<SegmentDataManager>asList(new OfflineSegmentDataManager(_indexSegment),
        new OfflineSegmentDataManager(_indexSegment));
  }

  @AfterClass
  public void destroySegment() {
    _indexSegment.destroy();
  }

  @AfterTest
  public void deleteSegment() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Override
  protected String getFilter() {
    return QUERY_FILTER;
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<SegmentDataManager> getSegmentDataManagers() {
    return _segmentDataManagers;
  }
}
