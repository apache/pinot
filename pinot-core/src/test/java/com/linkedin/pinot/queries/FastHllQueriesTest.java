/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.response.broker.BrokerResponseNative;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.manager.SegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegment;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import com.linkedin.pinot.core.operator.ExecutionStatistics;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.operator.query.AggregationGroupByOperator;
import com.linkedin.pinot.core.operator.query.AggregationOperator;
import com.linkedin.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.startree.hll.HllConfig;
import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


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
  private static final String AVRO_DATA_WITHOUT_PRE_GENERATED_HLL_COLUMNS =
      "data" + File.separator + "test_data-sv.avro";
  private static final String AVRO_DATA_WITH_PRE_GENERATED_HLL_COLUMNS =
      "data" + File.separator + "test_data-sv_hll.avro";
  private static final String SEGMENT_NAME = "testTable_126164076_167572854";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "FastHllQueriesTest");
  private static final int HLL_LOG2M = 6;

  private static final String BASE_QUERY = "SELECT FASTHLL(column17_HLL), FASTHLL(column18_HLL) FROM testTable";
  private static final String GROUP_BY = " group by column11";
  private static final String QUERY_FILTER =
      " WHERE column1 > 100000000" + " AND column3 BETWEEN 20000000 AND 1000000000" + " AND column5 = 'gFuH'"
          + " AND (column6 < 500000000 OR column11 NOT IN ('t', 'P'))" + " AND daysSinceEpoch = 126164076";

  private IndexSegment _indexSegment;
  // Contains 2 identical index segments
  private List<SegmentDataManager> _segmentDataManagers;

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

  @Test
  public void testFastHllWithoutPreGeneratedHllColumns() throws Exception {
    buildAndLoadSegment(false);

    // Test inner segment queries
    // Test base query
    AggregationOperator aggregationOperator = getOperatorForQuery(BASE_QUERY);
    IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
    ExecutionStatistics executionStatistics = aggregationOperator.getExecutionStatistics();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(executionStatistics, 1L, 0L, 2L, 30000L);
    List<Object> aggregationResult = resultsBlock.getAggregationResult();
    Assert.assertEquals(((HyperLogLog) aggregationResult.get(0)).cardinality(), 21L);
    Assert.assertEquals(((HyperLogLog) aggregationResult.get(1)).cardinality(), 1762L);
    // Test query with filter
    aggregationOperator = getOperatorForQueryWithFilter(BASE_QUERY);
    resultsBlock = aggregationOperator.nextBlock();
    executionStatistics = aggregationOperator.getExecutionStatistics();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(executionStatistics, 6129L, 112472L, 12258L, 30000L);
    aggregationResult = resultsBlock.getAggregationResult();
    Assert.assertEquals(((HyperLogLog) aggregationResult.get(0)).cardinality(), 17L);
    Assert.assertEquals(((HyperLogLog) aggregationResult.get(1)).cardinality(), 1197L);
    // Test query with group-by
    AggregationGroupByOperator aggregationGroupByOperator = getOperatorForQuery(BASE_QUERY + GROUP_BY);
    resultsBlock = aggregationGroupByOperator.nextBlock();
    executionStatistics = aggregationGroupByOperator.getExecutionStatistics();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(executionStatistics, 4613L, 0L, 13839L, 30000L);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    GroupKeyGenerator.GroupKey firstGroupKey = aggregationGroupByResult.getGroupKeyIterator().next();
    Assert.assertEquals(firstGroupKey._stringKey, "");
    Assert.assertEquals(((HyperLogLog) aggregationGroupByResult.getResultForKey(firstGroupKey, 0)).cardinality(), 21L);
    Assert.assertEquals(((HyperLogLog) aggregationGroupByResult.getResultForKey(firstGroupKey, 1)).cardinality(), 691L);

    // Test inter segments base query
    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(BASE_QUERY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 4L, 0L, 8L, 120000L, new String[]{"21", "1762"});
    // Test inter segments query with filter
    brokerResponse = getBrokerResponseForQueryWithFilter(BASE_QUERY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 449888L, 49032L, 120000L,
        new String[]{"17", "1197"});
    // Test inter segments query with group-by
    brokerResponse = getBrokerResponseForQuery(BASE_QUERY + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 18452L, 0L, 55356L, 120000L,
        new String[]{"21", "1762"});

    deleteSegment();
  }

  @Test
  public void testFastHllWithPreGeneratedHllColumns() throws Exception {
    buildAndLoadSegment(true);

    // Test inner segment queries
    // Test base query
    AggregationOperator aggregationOperator = getOperatorForQuery(BASE_QUERY);
    IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
    ExecutionStatistics executionStatistics = aggregationOperator.getExecutionStatistics();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(executionStatistics, 30000L, 0L, 60000L, 30000L);
    List<Object> aggregationResult = resultsBlock.getAggregationResult();
    Assert.assertEquals(((HyperLogLog) aggregationResult.get(0)).cardinality(), 21L);
    Assert.assertEquals(((HyperLogLog) aggregationResult.get(1)).cardinality(), 1762L);
    // Test query with filter
    aggregationOperator = getOperatorForQueryWithFilter(BASE_QUERY);
    resultsBlock = aggregationOperator.nextBlock();
    executionStatistics = aggregationOperator.getExecutionStatistics();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(executionStatistics, 6129L, 84134L, 12258L, 30000L);
    aggregationResult = resultsBlock.getAggregationResult();
    Assert.assertEquals(((HyperLogLog) aggregationResult.get(0)).cardinality(), 17L);
    Assert.assertEquals(((HyperLogLog) aggregationResult.get(1)).cardinality(), 1197L);
    // Test query with group-by
    AggregationGroupByOperator aggregationGroupByOperator = getOperatorForQuery(BASE_QUERY + GROUP_BY);
    resultsBlock = aggregationGroupByOperator.nextBlock();
    executionStatistics = aggregationGroupByOperator.getExecutionStatistics();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(executionStatistics, 30000L, 0L, 90000L, 30000L);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    GroupKeyGenerator.GroupKey firstGroupKey = aggregationGroupByResult.getGroupKeyIterator().next();
    Assert.assertEquals(firstGroupKey._stringKey, "");
    Assert.assertEquals(((HyperLogLog) aggregationGroupByResult.getResultForKey(firstGroupKey, 0)).cardinality(), 21L);
    Assert.assertEquals(((HyperLogLog) aggregationGroupByResult.getResultForKey(firstGroupKey, 1)).cardinality(), 691L);

    // Test inter segments base query
    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(BASE_QUERY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"21", "1762"});
    // Test inter segments query with filter
    brokerResponse = getBrokerResponseForQueryWithFilter(BASE_QUERY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"17", "1197"});
    // Test inter segments query with group-by
    brokerResponse = getBrokerResponseForQuery(BASE_QUERY + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"21", "1762"});

    deleteSegment();
  }

  private void buildAndLoadSegment(boolean hasPreGeneratedHllColumns) throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    // Get resource file path
    URL resource;
    if (hasPreGeneratedHllColumns) {
      resource = getClass().getClassLoader().getResource(AVRO_DATA_WITH_PRE_GENERATED_HLL_COLUMNS);
    } else {
      resource = getClass().getClassLoader().getResource(AVRO_DATA_WITHOUT_PRE_GENERATED_HLL_COLUMNS);
    }
    Assert.assertNotNull(resource);
    String filePath = resource.getFile();

    // Build the segment schema
    Schema.SchemaBuilder schemaBuilder = new Schema.SchemaBuilder().setSchemaName("testTable")
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
        .addTime("daysSinceEpoch", TimeUnit.DAYS, FieldSpec.DataType.INT);
    if (hasPreGeneratedHllColumns) {
      schemaBuilder.addSingleValueDimension("column17_HLL", FieldSpec.DataType.STRING)
          .addSingleValueDimension("column18_HLL", FieldSpec.DataType.STRING);
    }

    // Create the segment generator config
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(schemaBuilder.build());
    segmentGeneratorConfig.setInputFilePath(filePath);
    segmentGeneratorConfig.setTableName("testTable");
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
    segmentGeneratorConfig.setInvertedIndexCreationColumns(
        Arrays.asList("column6", "column7", "column11", "column17", "column18"));
    if (hasPreGeneratedHllColumns) {
      segmentGeneratorConfig.setHllConfig(new HllConfig(HLL_LOG2M));
    } else {
      segmentGeneratorConfig.enableStarTreeIndex(null);
      // Intentionally use the non-default suffix
      segmentGeneratorConfig.setHllConfig(
          new HllConfig(HLL_LOG2M, new HashSet<>(Arrays.asList("column17", "column18")), "_HLL"));
    }

    // Build the index segment
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.heap);
    _indexSegment = immutableSegment;
    _segmentDataManagers =
        Arrays.asList(new ImmutableSegmentDataManager(immutableSegment), new ImmutableSegmentDataManager(immutableSegment));
  }

  private void deleteSegment() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }
}
