/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.queries;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;


/**
 * The <code>BaseMultiValueQueriesTest</code> class sets up the index segment for the multi-value queries test.
 * <p>There are totally 14 columns, 100000 records inside the original Avro file where 10 columns are selected to build
 * the index segment. Selected columns information are as following:
 * <ul>
 *   ColumnName, FieldType, DataType, Cardinality, IsSorted, HasInvertedIndex, IsMultiValueRaw
 *   <li>column1, METRIC, INT, 51594, F, F, F</li>
 *   <li>column2, METRIC, INT, 42242, F, F, F</li>
 *   <li>column3, DIMENSION, STRING, 5, F, T, F</li>
 *   <li>column4, DIMENSION, STRING, 5, F, F, F</li>
 *   <li>column5, DIMENSION, STRING, 9, F, F, F</li>
 *   <li>column6, DIMENSION, INT, 18499, F, F, T</li>
 *   <li>column7, DIMENSION, INT, 359, F, F, T</li>
 *   <li>column8, DIMENSION, INT, 850, F, T, F</li>
 *   <li>column9, METRIC, INT, 146, F, T, F</li>
 *   <li>column10, METRIC, INT, 3960, F, F, F</li>
 *   <li>daysSinceEpoch, TIME, INT, 1, T, F, F</li>
 * </ul>
 */
public class ScanBasedANDFilterReorderingTest {

  @Test
  public static class MVTest extends BaseQueriesTest {
    protected static final String SUM_QUERY = "SELECT SUM(column1) FROM testTable";
    protected static final String FILTER1 = " WHERE column7 IN (2147483647, 211, 336, 363, 469, 565)"
        + " AND column6 = 2147483647"
        + " AND column3 <> 'L'";
    protected static final String FILTER2 = " WHERE column7 IN (2147483647, 211, 336, 363, 469, 565)"
        + " AND column6 = 3267"
        + " AND column3 <> 'L'";
    protected static final String FILTER3 = FILTER1 + " AND column1 > '50000000'";
    private static final String AVRO_DATA = "data" + File.separator + "test_data-mv.avro";
    private static final String SEGMENT_NAME = "testTable_1756015683_1756015683";
    private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "MultiValueRawQueriesTest");
    private static final String SET_AND_OPTIMIZATION = "SET "
        + CommonConstants.Broker.Request.QueryOptionKey.AND_SCAN_REORDERING + " = 'True';";
    private IndexSegment _indexSegment;
    // Contains 2 identical index segments.
    private List<IndexSegment> _indexSegments;

    @BeforeTest
    public void buildSegment()
        throws Exception {
      FileUtils.deleteQuietly(INDEX_DIR);

      // Get resource file path.
      URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
      assertNotNull(resource);
      String filePath = resource.getFile();

      // Build the segment schema.
      Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable").addMetric("column1", FieldSpec.DataType.INT)
          .addMetric("column2", FieldSpec.DataType.INT).addSingleValueDimension("column3", FieldSpec.DataType.STRING)
          .addSingleValueDimension("column4", FieldSpec.DataType.STRING)
          .addSingleValueDimension("column5", FieldSpec.DataType.STRING)
          .addMultiValueDimension("column6", FieldSpec.DataType.INT)
          .addMultiValueDimension("column7", FieldSpec.DataType.INT)
          .addSingleValueDimension("column8", FieldSpec.DataType.INT).addMetric("column9", FieldSpec.DataType.INT)
          .addMetric("column10", FieldSpec.DataType.INT)
          .addTime(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "daysSinceEpoch"), null).build();
      // The segment generation code in SegmentColumnarIndexCreator will throw
      // exception if start and end time in time column are not in acceptable
      // range. For this test, we first need to fix the input avro data
      // to have the time column values in allowed range. Until then, the check
      // is explicitly disabled
      IngestionConfig ingestionConfig = new IngestionConfig();
      ingestionConfig.setSegmentTimeValueCheck(false);
      ingestionConfig.setRowTimeValueCheck(false);
      TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
          .setTimeColumnName("daysSinceEpoch")
          .setIngestionConfig(ingestionConfig).build();

      // Create the segment generator config.
      SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
      segmentGeneratorConfig.setInputFilePath(filePath);
      segmentGeneratorConfig.setTableName("testTable");
      segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
      segmentGeneratorConfig.setIndexOn(
          StandardIndexes.inverted(), IndexConfig.ENABLED, "column3", "column8", "column9");

      // Build the index segment.
      SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
      driver.init(segmentGeneratorConfig);
      driver.build();
    }

    @BeforeClass
    public void loadSegment()
        throws Exception {
      IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
      indexLoadingConfig.setInvertedIndexColumns(
          new HashSet<>(Arrays.asList("column3", "column8", "column9")));
      ImmutableSegment immutableSegment =
          ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);
      _indexSegment = immutableSegment;
      _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
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
      return FILTER1;
    }

    @Override
    protected IndexSegment getIndexSegment() {
      return _indexSegment;
    }

    @Override
    protected List<IndexSegment> getIndexSegments() {
      return _indexSegments;
    }

    @Test
    public void testScanBasedANDFilterReorderingOptimization1() {
      // Test query with optimization, bitmap + scan
      AggregationOperator aggregationOperator = getOperator(SET_AND_OPTIMIZATION + SUM_QUERY + FILTER1);
      AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
      ExecutionStatistics executionStatistics = aggregationOperator.getExecutionStatistics();
      QueriesTestUtils.testInnerSegmentExecutionStatistics(executionStatistics, 46649L, 154999L,
          46649L, 100000L);
      Assert.assertEquals(((Number) resultsBlock.getResults().get(0)).longValue(), 44224075056091L);

      // Test query without optimization, bitmap + scan
      aggregationOperator = getOperator(SUM_QUERY + FILTER1);
      resultsBlock = aggregationOperator.nextBlock();
      executionStatistics = aggregationOperator.getExecutionStatistics();
      QueriesTestUtils.testInnerSegmentExecutionStatistics(executionStatistics, 46649L, 189513L,
          46649L, 100000L);
      Assert.assertEquals(((Number) resultsBlock.getResults().get(0)).longValue(), 44224075056091L);
    }

    @Test
    public void testScanBasedANDFilterReorderingOptimization2() {
      // Test query with optimization, another bitmap + scan
      AggregationOperator aggregationOperator = getOperator(SET_AND_OPTIMIZATION + SUM_QUERY + FILTER2);
      AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
      ExecutionStatistics executionStatistics = aggregationOperator.getExecutionStatistics();
      QueriesTestUtils.testInnerSegmentExecutionStatistics(executionStatistics, 0, 97458L,
          0, 100000L);
      Assert.assertEquals(((Number) resultsBlock.getResults().get(0)).longValue(), 0);

      // Test query without optimization, another bitmap + scan
      aggregationOperator = getOperator(SUM_QUERY + FILTER2);
      resultsBlock = aggregationOperator.nextBlock();
      executionStatistics = aggregationOperator.getExecutionStatistics();
      QueriesTestUtils.testInnerSegmentExecutionStatistics(executionStatistics, 0, 189513L,
          0, 100000L);
      Assert.assertEquals(((Number) resultsBlock.getResults().get(0)).longValue(), 0);
    }

    @Test
    public void testScanBasedANDFilterReorderingOptimization3() {
      // Test query with optimization, bitmap + scan + range
      AggregationOperator aggregationOperator = getOperator(SET_AND_OPTIMIZATION + SUM_QUERY + FILTER3);
      AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
      ExecutionStatistics executionStatistics = aggregationOperator.getExecutionStatistics();
      QueriesTestUtils.testInnerSegmentExecutionStatistics(executionStatistics, 45681L, 201648L,
          45681L, 100000L);
      Assert.assertEquals(((Number) resultsBlock.getResults().get(0)).longValue(), 44199078145668L);

      // Test query without optimization, bitmap + scan + range
      aggregationOperator = getOperator(SUM_QUERY + FILTER3);
      resultsBlock = aggregationOperator.nextBlock();
      executionStatistics = aggregationOperator.getExecutionStatistics();
      QueriesTestUtils.testInnerSegmentExecutionStatistics(executionStatistics, 45681L, 276352L,
          45681L, 100000L);
      Assert.assertEquals(((Number) resultsBlock.getResults().get(0)).longValue(), 44199078145668L);
    }
  }
}
