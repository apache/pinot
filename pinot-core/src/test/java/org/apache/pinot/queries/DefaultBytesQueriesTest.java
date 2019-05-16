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
import com.tdunning.math.stats.TDigest;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.data.DimensionFieldSpec;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.MetricFieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.GroupByResult;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.data.readers.RecordReader;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.AggregationGroupByOperator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.query.aggregation.function.customobject.AvgPair;
import org.apache.pinot.core.query.aggregation.function.customobject.MinMaxRangePair;
import org.apache.pinot.core.query.aggregation.function.customobject.QuantileDigest;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Tests for default bytes (zero-length byte array) values.
 *
 * <p>Aggregation function that supports bytes values:
 * <ul>
 *   <li>AVG</li>
 *   <li>DISTINCTCOUNTHLL</li>
 *   <li>MINMAXRANGE</li>
 *   <li>PERCENTILEEST</li>
 *   <li>PERCENTILETDIGEST</li>
 * </ul>
 */
public class DefaultBytesQueriesTest extends BaseQueriesTest {
  protected static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "DefaultBytesQueriesTest");
  protected static final String TABLE_NAME = "testTable";
  protected static final String SEGMENT_NAME = "testSegment";

  protected static final int NUM_ROWS = 1000;
  protected static final String BYTES_COLUMN = "bytesColumn";
  protected static final String GROUP_BY_COLUMN = "groupByColumn";
  protected static final String[] GROUPS = new String[]{"G1", "G2", "G3"};
  protected static final long RANDOM_SEED = System.nanoTime();
  protected static final Random RANDOM = new Random(RANDOM_SEED);

  private ImmutableSegment _indexSegment;
  private List<SegmentDataManager> _segmentDataManagers;

  @Override
  protected String getFilter() {
    return ""; // No filtering required for this test.
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<SegmentDataManager> getSegmentDataManagers() {
    return _segmentDataManagers;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    buildSegment();
    _indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _segmentDataManagers =
        Arrays.asList(new ImmutableSegmentDataManager(_indexSegment), new ImmutableSegmentDataManager(_indexSegment));
  }

  private void buildSegment()
      throws Exception {
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      HashMap<String, Object> valueMap = new HashMap<>();

      valueMap.put(BYTES_COLUMN, FieldSpec.DEFAULT_METRIC_NULL_VALUE_OF_BYTES);

      String group = GROUPS[RANDOM.nextInt(GROUPS.length)];
      valueMap.put(GROUP_BY_COLUMN, group);

      GenericRow genericRow = new GenericRow();
      genericRow.init(valueMap);
      rows.add(genericRow);
    }

    Schema schema = new Schema();
    schema.addField(new MetricFieldSpec(BYTES_COLUMN, FieldSpec.DataType.BYTES));
    schema.addField(new DimensionFieldSpec(GROUP_BY_COLUMN, FieldSpec.DataType.STRING, true));

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows, schema)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  @Test
  public void testInnerSegmentAggregation() {
    // For inner segment case, percentile does not affect the intermediate result
    AggregationOperator aggregationOperator = getOperatorForQuery(getAggregationQuery());
    IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
    List<Object> aggregationResult = resultsBlock.getAggregationResult();
    Assert.assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 5);
    // Avg
    AvgPair avgPair = (AvgPair) aggregationResult.get(0);
    assertEquals(avgPair.getSum(), 0.0);
    assertEquals(avgPair.getCount(), 0L);
    // DistinctCountHLL
    HyperLogLog hyperLogLog = (HyperLogLog) aggregationResult.get(1);
    assertEquals(hyperLogLog.cardinality(), 0L);
    // MinMaxRange
    MinMaxRangePair minMaxRangePair = (MinMaxRangePair) aggregationResult.get(2);
    assertEquals(minMaxRangePair.getMax(), Double.NEGATIVE_INFINITY);
    assertEquals(minMaxRangePair.getMin(), Double.POSITIVE_INFINITY);
    // PercentileEst
    QuantileDigest quantileDigest = (QuantileDigest) aggregationResult.get(3);
    assertEquals(quantileDigest.getQuantile(0.5), Long.MIN_VALUE);
    // PercentileTDigest
    TDigest tDigest = (TDigest) aggregationResult.get(4);
    assertTrue(Double.isNaN(tDigest.quantile(0.5)));
  }

  @Test
  public void testInterSegmentAggregation() {
    for (int percentile = 0; percentile <= 100; percentile++) {
      BrokerResponseNative brokerResponse = getBrokerResponseForQuery(getAggregationQuery());
      List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
      Assert.assertNotNull(aggregationResults);
      assertEquals(aggregationResults.size(), 5);
      // Avg
      assertEquals(Double.parseDouble((String) aggregationResults.get(0).getValue()), Double.NEGATIVE_INFINITY);
      // DistinctCountHLL
      assertEquals(Long.parseLong((String) aggregationResults.get(1).getValue()), 0L);
      // MinMaxRange
      assertEquals(Double.parseDouble((String) aggregationResults.get(2).getValue()), Double.NEGATIVE_INFINITY);
      // PercentileEst
      assertEquals(Long.parseLong((String) aggregationResults.get(3).getValue()), Long.MIN_VALUE);
      // PercentileTDigest
      assertTrue(Double.isNaN(Double.parseDouble((String) aggregationResults.get(4).getValue())));
    }
  }

  @Test
  public void testInnerSegmentGroupBy() {
    // For inner segment case, percentile does not affect the intermediate result
    AggregationGroupByOperator groupByOperator = getOperatorForQuery(getGroupByQuery());
    IntermediateResultsBlock resultsBlock = groupByOperator.nextBlock();
    AggregationGroupByResult groupByResult = resultsBlock.getAggregationGroupByResult();
    Assert.assertNotNull(groupByResult);
    Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = groupByResult.getGroupKeyIterator();
    while (groupKeyIterator.hasNext()) {
      GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
      // Avg
      AvgPair avgPair = (AvgPair) groupByResult.getResultForKey(groupKey, 0);
      assertEquals(avgPair.getSum(), 0.0);
      assertEquals(avgPair.getCount(), 0L);
      // DistinctCountHLL
      HyperLogLog hyperLogLog = (HyperLogLog) groupByResult.getResultForKey(groupKey, 1);
      assertEquals(hyperLogLog.cardinality(), 0L);
      // MinMaxRange
      MinMaxRangePair minMaxRangePair = (MinMaxRangePair) groupByResult.getResultForKey(groupKey, 2);
      assertEquals(minMaxRangePair.getMax(), Double.NEGATIVE_INFINITY);
      assertEquals(minMaxRangePair.getMin(), Double.POSITIVE_INFINITY);
      // PercentileEst
      QuantileDigest quantileDigest = (QuantileDigest) groupByResult.getResultForKey(groupKey, 3);
      assertEquals(quantileDigest.getQuantile(0.5), Long.MIN_VALUE);
      // PercentileTDigest
      TDigest tDigest = (TDigest) groupByResult.getResultForKey(groupKey, 4);
      assertTrue(Double.isNaN(tDigest.quantile(0.5)));
    }
  }

  @Test
  public void testInterSegmentGroupBy() {
    for (int percentile = 0; percentile <= 100; percentile++) {
      BrokerResponseNative brokerResponse = getBrokerResponseForQuery(getGroupByQuery());
      List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
      Assert.assertNotNull(aggregationResults);
      assertEquals(aggregationResults.size(), 5);
      // Avg
      List<GroupByResult> groupByResults = aggregationResults.get(0).getGroupByResult();
      assertEquals(groupByResults.size(), 3);
      for (GroupByResult groupByResult : groupByResults) {
        assertEquals(Double.parseDouble((String) groupByResult.getValue()), Double.NEGATIVE_INFINITY);
      }
      // DistinctCountHLL
      groupByResults = aggregationResults.get(1).getGroupByResult();
      assertEquals(groupByResults.size(), 3);
      for (GroupByResult groupByResult : groupByResults) {
        assertEquals(Long.parseLong((String) groupByResult.getValue()), 0L);
      }
      // MinMaxRange
      groupByResults = aggregationResults.get(2).getGroupByResult();
      assertEquals(groupByResults.size(), 3);
      for (GroupByResult groupByResult : groupByResults) {
        assertEquals(Double.parseDouble((String) groupByResult.getValue()), Double.NEGATIVE_INFINITY);
      }
      // PercentileEst
      groupByResults = aggregationResults.get(3).getGroupByResult();
      assertEquals(groupByResults.size(), 3);
      for (GroupByResult groupByResult : groupByResults) {
        assertEquals(Long.parseLong((String) groupByResult.getValue()), Long.MIN_VALUE);
      }
      // PercentileTDigest
      groupByResults = aggregationResults.get(4).getGroupByResult();
      assertEquals(groupByResults.size(), 3);
      for (GroupByResult groupByResult : groupByResults) {
        assertTrue(Double.isNaN(Double.parseDouble((String) groupByResult.getValue())));
      }
    }
  }

  private String getAggregationQuery() {
    return String.format(
        "SELECT AVG(%s), DISTINCTCOUNTHLL(%s), MINMAXRANGE(%s), PERCENTILEEST50(%s), PERCENTILETDIGEST50(%s) FROM %s",
        BYTES_COLUMN, BYTES_COLUMN, BYTES_COLUMN, BYTES_COLUMN, BYTES_COLUMN, TABLE_NAME);
  }

  private String getGroupByQuery() {
    return String.format("%s GROUP BY %s", getAggregationQuery(), GROUP_BY_COLUMN);
  }

  @AfterClass
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }
}
