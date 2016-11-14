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
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.plan.maker.InstancePlanMakerImplV2;
import com.linkedin.pinot.core.plan.maker.PlanMaker;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;


/**
 * The <code>BaseMultiValueQueriesTest</code> class sets up the index segment for the multi-value queries test.
 * <p>There are totally 14 columns, 100000 records inside the original Avro file where 10 columns are selected to build
 * the index segment. Selected columns information are as following:
 * <ul>
 *   ColumnName, FieldType, DataType, Cardinality, IsSorted, HasInvertedIndex, IsMultiValue
 *   <li>column1, METRIC, INT, 51594, F, F, F</li>
 *   <li>column2, METRIC, INT, 42242, F, F, F</li>
 *   <li>column3, DIMENSION, STRING, 5, F, T, F</li>
 *   <li>column5, DIMENSION, STRING, 9, F, F, F</li>
 *   <li>column6, DIMENSION, INT, 18499, F, F, T</li>
 *   <li>column7, DIMENSION, INT, 359, F, T, T</li>
 *   <li>column8, DIMENSION, INT, 850, F, T, F</li>
 *   <li>column9, METRIC, INT, 146, F, T, F</li>
 *   <li>column10, METRIC, INT, 3960, F, F, F</li>
 *   <li>daysSinceEpoch, TIME, INT, 1, T, F, F</li>
 * </ul>
 */
public abstract class BaseMultiValueQueriesTest {
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final String SEGMENT_NAME = "testTable_1756015683_1756015683_";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "MultiValueQueriesTest");
  private static final Pql2Compiler COMPILER = new Pql2Compiler();
  private static final PlanMaker PLAN_MAKER = new InstancePlanMakerImplV2();

  // Hard-coded query filter.
  private static final String QUERY_FILTER =
      " WHERE column1 > 100000000"
          + " AND column2 BETWEEN 20000000 AND 1000000000"
          + " AND column3 <> 'w'"
          + " AND (column6 < 500000 OR column7 NOT IN (225, 407))"
          + " AND daysSinceEpoch = 1756015683";

  private IndexSegment _indexSegment;

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
        .addMetric("column2", FieldSpec.DataType.INT)
        .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column5", FieldSpec.DataType.STRING)
        .addMultiValueDimension("column6", FieldSpec.DataType.INT)
        .addMultiValueDimension("column7", FieldSpec.DataType.INT)
        .addSingleValueDimension("column8", FieldSpec.DataType.INT)
        .addMetric("column9", FieldSpec.DataType.INT)
        .addMetric("column10", FieldSpec.DataType.INT)
        .addTime("daysSinceEpoch", TimeUnit.DAYS, FieldSpec.DataType.INT)
        .build();

    // Create the segment generator config.
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(schema);
    segmentGeneratorConfig.setInputFilePath(filePath);
    segmentGeneratorConfig.setTableName("testTable");
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
    segmentGeneratorConfig.setInvertedIndexCreationColumns(Arrays.asList("column3", "column7", "column8", "column9"));

    // Build the index segment.
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();
  }

  @BeforeClass
  public void loadSegment()
      throws Exception {
    File indexSegmentDir = new File(INDEX_DIR, SEGMENT_NAME);
    _indexSegment = ColumnarSegmentLoader.load(indexSegmentDir, ReadMode.heap);
  }

  @AfterClass
  public void destroySegment() {
    _indexSegment.destroy();
  }

  @AfterTest
  public void deleteSegment() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  /**
   * Run query on the single index segment.
   *
   * @param query PQL query.
   * @return query operator.
   */
  @SuppressWarnings("unchecked")
  protected <T extends Operator> T getOperatorForQuery(String query) {
    return (T) PLAN_MAKER.makeInnerSegmentPlan(_indexSegment, COMPILER.compileToBrokerRequest(query)).run();
  }

  /**
   * Run query with hard-coded filter on the single index segment.
   *
   * @param query PQL query without any filter.
   * @return query operator.
   */
  protected <T extends Operator> T getOperatorForQueryWithFilter(String query) {
    return getOperatorForQuery(query + QUERY_FILTER);
  }
}
