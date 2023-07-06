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

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.GroupByOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/**
 * The <code>FastHllQueriesTest</code> class sets up the index segment and create fastHll on 'column17' and 'column18'.
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
@SuppressWarnings("ConstantConditions")
public class FastHllQueriesTest extends BaseQueriesTest {
  private static final String AVRO_DATA_WITH_PRE_GENERATED_HLL_COLUMNS =
      "data" + File.separator + "test_data-sv_hll.avro";
  private static final String SEGMENT_NAME = "testTable_126164076_167572854";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "FastHllQueriesTest");

  private static final String BASE_QUERY = "SELECT FASTHLL(column17_HLL), FASTHLL(column18_HLL) FROM testTable";
  private static final String GROUP_BY = " GROUP BY column11 ORDER BY column11";
  private static final String QUERY_FILTER =
      " WHERE column1 > 100000000 " + "AND column3 BETWEEN 20000000 AND 1000000000 " + "AND column5 = 'gFuH' "
          + "AND (column6 < 500000000 OR column11 NOT IN ('t', 'P')) " + "AND daysSinceEpoch = 126164076";

  private IndexSegment _indexSegment;
  // Contains 2 identical index segments
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    return QUERY_FILTER;
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
  public void testFastHllWithPreGeneratedHllColumns()
      throws Exception {
    buildAndLoadSegment();

    // Test inner segment queries
    // Test base query
    AggregationOperator aggregationOperator = getOperator(BASE_QUERY);
    AggregationResultsBlock aggregationResultsBlock = aggregationOperator.nextBlock();
    ExecutionStatistics executionStatistics = aggregationOperator.getExecutionStatistics();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(executionStatistics, 30000L, 0L, 60000L, 30000L);
    List<Object> aggregationResult = aggregationResultsBlock.getResults();
    assertEquals(((HyperLogLog) aggregationResult.get(0)).cardinality(), 21L);
    assertEquals(((HyperLogLog) aggregationResult.get(1)).cardinality(), 1762L);
    // Test query with filter
    aggregationOperator = getOperatorWithFilter(BASE_QUERY);
    aggregationResultsBlock = aggregationOperator.nextBlock();
    executionStatistics = aggregationOperator.getExecutionStatistics();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(executionStatistics, 6129L, 84134L, 12258L, 30000L);
    aggregationResult = aggregationResultsBlock.getResults();
    assertEquals(((HyperLogLog) aggregationResult.get(0)).cardinality(), 17L);
    assertEquals(((HyperLogLog) aggregationResult.get(1)).cardinality(), 1197L);
    // Test query with group-by
    GroupByOperator groupByOperator = getOperator(BASE_QUERY + GROUP_BY);
    GroupByResultsBlock groupByResultsBlock = groupByOperator.nextBlock();
    executionStatistics = groupByOperator.getExecutionStatistics();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(executionStatistics, 30000L, 0L, 90000L, 30000L);
    AggregationGroupByResult aggregationGroupByResult = groupByResultsBlock.getAggregationGroupByResult();
    GroupKeyGenerator.GroupKey firstGroupKey = aggregationGroupByResult.getGroupKeyIterator().next();
    assertEquals(firstGroupKey._keys[0], "");
    assertEquals(((HyperLogLog) aggregationGroupByResult.getResultForGroupId(0, firstGroupKey._groupId)).cardinality(),
        21L);
    assertEquals(((HyperLogLog) aggregationGroupByResult.getResultForGroupId(1, firstGroupKey._groupId)).cardinality(),
        691L);

    // Test inter segments base query
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(BASE_QUERY), 120000L, 0L, 240000L, 120000L,
        new Object[]{21L, 1762L});
    // Test inter segments query with filter
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponseWithFilter(BASE_QUERY), 24516L, 336536L, 49032L, 120000L,
        new Object[]{17L, 1197L});
    // Test inter segments query with group-by
    List<Object[]> expectedRows =
        Arrays.asList(new Object[]{21L, 691L}, new Object[]{21L, 1762L}, new Object[]{11L, 27L},
            new Object[]{21L, 1397L}, new Object[]{21L, 1532L});
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(BASE_QUERY + GROUP_BY), 120000L, 0L, 360000L, 120000L,
        expectedRows);

    deleteSegment();
  }

  private void buildAndLoadSegment()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    // Get resource file path
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA_WITH_PRE_GENERATED_HLL_COLUMNS);
    assertNotNull(resource);
    String filePath = resource.getFile();

    // Build the segment schema
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable").addMetric("column1", FieldSpec.DataType.INT)
        .addMetric("column3", FieldSpec.DataType.INT).addSingleValueDimension("column5", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column6", FieldSpec.DataType.INT)
        .addSingleValueDimension("column7", FieldSpec.DataType.INT)
        .addSingleValueDimension("column9", FieldSpec.DataType.INT)
        .addSingleValueDimension("column11", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column12", FieldSpec.DataType.STRING).addMetric("column17", FieldSpec.DataType.INT)
        .addMetric("column18", FieldSpec.DataType.INT)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "daysSinceEpoch"), null)
        .addSingleValueDimension("column17_HLL", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column18_HLL", FieldSpec.DataType.STRING).build();

    // The segment generation code in SegmentColumnarIndexCreator will throw
    // exception if start and end time in time column are not in acceptable
    // range. For this test, we first need to fix the input avro data
    // to have the time column values in allowed range. Until then, the check
    // is explicitly disabled
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setSegmentTimeValueCheck(false);
    ingestionConfig.setRowTimeValueCheck(false);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setTimeColumnName("daysSinceEpoch")
            .setIngestionConfig(ingestionConfig).build();

    // Create the segment generator config
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setInputFilePath(filePath);
    segmentGeneratorConfig.setTableName("testTable");
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());

    // Build the index segment
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.heap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  private void deleteSegment() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }
}
