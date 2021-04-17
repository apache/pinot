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
import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.GroupByResult;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.AggregationGroupByOperator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.segment.local.customobject.AvgPair;
import org.apache.pinot.segment.local.customobject.MinMaxRangePair;
import org.apache.pinot.segment.local.customobject.QuantileDigest;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/**
 * Tests for serialized bytes values.
 *
 * <p>Aggregation function that supports serialized bytes values:
 * <ul>
 *   <li>AVG</li>
 *   <li>DISTINCTCOUNTHLL</li>
 *   <li>MINMAXRANGE</li>
 *   <li>PERCENTILEEST</li>
 *   <li>PERCENTILETDIGEST</li>
 * </ul>
 */
public class SerializedBytesQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "SerializedBytesQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final int NUM_ROWS = 1000;
  private static final int MAX_NUM_VALUES_TO_PRE_AGGREGATE = 10;
  private static final String AVG_COLUMN = "avgColumn";
  private static final String DISTINCT_COUNT_HLL_COLUMN = "distinctCountHLLColumn";
  // Use non-default log2m
  private static final int DISTINCT_COUNT_HLL_LOG2M = 9;
  private static final String MIN_MAX_RANGE_COLUMN = "minMaxRangeColumn";
  private static final String PERCENTILE_EST_COLUMN = "percentileEstColumn";
  // Use non-default max error
  private static final double PERCENTILE_EST_MAX_ERROR = 0.025;
  private static final String PERCENTILE_TDIGEST_COLUMN = "percentileTDigestColumn";
  // Use non-default compression
  private static final double PERCENTILE_TDIGEST_COMPRESSION = 200;
  // Allow 5% quantile error due to the randomness of TDigest merge
  private static final double PERCENTILE_TDIGEST_DELTA = 0.05 * Integer.MAX_VALUE;
  private static final String GROUP_BY_SV_COLUMN = "groupBySVColumn";
  private static final String GROUP_BY_MV_COLUMN = "groupByMVColumn";
  private static final String[] GROUPS = new String[]{"G0", "G1", "G2"};
  private static final int NUM_GROUPS = GROUPS.length;
  private static final long RANDOM_SEED = System.nanoTime();
  private static final Random RANDOM = new Random(RANDOM_SEED);

  private final int[][] _valuesArray = new int[NUM_ROWS][MAX_NUM_VALUES_TO_PRE_AGGREGATE];
  private final AvgPair[] _avgPairs = new AvgPair[NUM_ROWS];
  private final HyperLogLog[] _hyperLogLogs = new HyperLogLog[NUM_ROWS];
  private final MinMaxRangePair[] _minMaxRangePairs = new MinMaxRangePair[NUM_ROWS];
  private final QuantileDigest[] _quantileDigests = new QuantileDigest[NUM_ROWS];
  private final TDigest[] _tDigests = new TDigest[NUM_ROWS];

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    return ""; // No filtering required for this test.
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @BeforeClass
  public void setUp() throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    buildSegment();
    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  private void buildSegment() throws Exception {
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      int numValues = RANDOM.nextInt(MAX_NUM_VALUES_TO_PRE_AGGREGATE) + 1;
      int[] values = new int[numValues];
      for (int j = 0; j < numValues; j++) {
        values[j] = RANDOM.nextInt();
      }
      _valuesArray[i] = values;
      int groupId = i % NUM_GROUPS;

      HashMap<String, Object> valueMap = new HashMap<>();
      valueMap.put(GROUP_BY_SV_COLUMN, GROUPS[groupId]);
      valueMap.put(GROUP_BY_MV_COLUMN, GROUPS);

      double sum = 0.0;
      for (int value : values) {
        sum += value;
      }
      AvgPair avgPair = new AvgPair(sum, numValues);
      _avgPairs[i] = avgPair;
      valueMap.put(AVG_COLUMN, ObjectSerDeUtils.AVG_PAIR_SER_DE.serialize(avgPair));

      HyperLogLog hyperLogLog = new HyperLogLog(DISTINCT_COUNT_HLL_LOG2M);
      for (int value : values) {
        hyperLogLog.offer(value);
      }
      _hyperLogLogs[i] = hyperLogLog;
      valueMap.put(DISTINCT_COUNT_HLL_COLUMN, ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize(hyperLogLog));

      double min = Double.POSITIVE_INFINITY;
      double max = Double.NEGATIVE_INFINITY;
      for (int value : values) {
        if (value < min) {
          min = value;
        }
        if (value > max) {
          max = value;
        }
      }
      MinMaxRangePair minMaxRangePair = new MinMaxRangePair(min, max);
      _minMaxRangePairs[i] = minMaxRangePair;
      valueMap.put(MIN_MAX_RANGE_COLUMN, ObjectSerDeUtils.MIN_MAX_RANGE_PAIR_SER_DE.serialize(minMaxRangePair));

      QuantileDigest quantileDigest = new QuantileDigest(PERCENTILE_EST_MAX_ERROR);
      for (int value : values) {
        quantileDigest.add(value);
      }
      _quantileDigests[i] = quantileDigest;
      valueMap.put(PERCENTILE_EST_COLUMN, ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE.serialize(quantileDigest));

      TDigest tDigest = MergingDigest.createDigest(PERCENTILE_TDIGEST_COMPRESSION);
      for (int value : values) {
        tDigest.add(value);
      }
      _tDigests[i] = tDigest;
      valueMap.put(PERCENTILE_TDIGEST_COLUMN, ObjectSerDeUtils.TDIGEST_SER_DE.serialize(tDigest));

      GenericRow genericRow = new GenericRow();
      genericRow.init(valueMap);
      rows.add(genericRow);
    }

    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension(GROUP_BY_SV_COLUMN, DataType.STRING)
        .addMultiValueDimension(GROUP_BY_MV_COLUMN, DataType.STRING).addMetric(AVG_COLUMN, DataType.BYTES)
        .addMetric(DISTINCT_COUNT_HLL_COLUMN, DataType.BYTES).addMetric(MIN_MAX_RANGE_COLUMN, DataType.BYTES)
        .addMetric(PERCENTILE_EST_COLUMN, DataType.BYTES).addMetric(PERCENTILE_TDIGEST_COLUMN, DataType.BYTES).build();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(RAW_TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  @Test
  public void testInnerSegmentAggregation() throws Exception {
    AggregationOperator aggregationOperator = getOperatorForPqlQuery(getAggregationQuery());
    IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
    List<Object> aggregationResult = resultsBlock.getAggregationResult();
    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 5);

    // Avg
    AvgPair avgPair = (AvgPair) aggregationResult.get(0);
    AvgPair expectedAvgPair = new AvgPair(_avgPairs[0].getSum(), _avgPairs[0].getCount());
    for (int i = 1; i < NUM_ROWS; i++) {
      expectedAvgPair.apply(_avgPairs[i]);
    }
    assertEquals(avgPair.getSum(), expectedAvgPair.getSum());
    assertEquals(avgPair.getCount(), expectedAvgPair.getCount());

    // DistinctCountHLL
    HyperLogLog hyperLogLog = (HyperLogLog) aggregationResult.get(1);
    HyperLogLog expectedHyperLogLog = new HyperLogLog(DISTINCT_COUNT_HLL_LOG2M);
    for (int value : _valuesArray[0]) {
      expectedHyperLogLog.offer(value);
    }
    for (int i = 1; i < NUM_ROWS; i++) {
      expectedHyperLogLog.addAll(_hyperLogLogs[i]);
    }
    assertEquals(hyperLogLog.cardinality(), expectedHyperLogLog.cardinality());

    // MinMaxRange
    MinMaxRangePair minMaxRangePair = (MinMaxRangePair) aggregationResult.get(2);
    MinMaxRangePair expectedMinMaxRangePair =
        new MinMaxRangePair(_minMaxRangePairs[0].getMin(), _minMaxRangePairs[0].getMax());
    for (int i = 1; i < NUM_ROWS; i++) {
      expectedMinMaxRangePair.apply(_minMaxRangePairs[i]);
    }
    assertEquals(minMaxRangePair.getMin(), expectedMinMaxRangePair.getMin());
    assertEquals(minMaxRangePair.getMax(), expectedMinMaxRangePair.getMax());

    // PercentileEst
    QuantileDigest quantileDigest = (QuantileDigest) aggregationResult.get(3);
    QuantileDigest expectedQuantileDigest = new QuantileDigest(PERCENTILE_EST_MAX_ERROR);
    for (int value : _valuesArray[0]) {
      expectedQuantileDigest.add(value);
    }
    for (int i = 1; i < NUM_ROWS; i++) {
      expectedQuantileDigest.merge(_quantileDigests[i]);
    }
    assertEquals(quantileDigest.getQuantile(0.5), expectedQuantileDigest.getQuantile(0.5));

    // PercentileTDigest
    TDigest tDigest = (TDigest) aggregationResult.get(4);
    TDigest expectedTDigest = TDigest.createMergingDigest(PERCENTILE_TDIGEST_COMPRESSION);
    for (int value : _valuesArray[0]) {
      expectedTDigest.add(value);
    }
    for (int i = 1; i < NUM_ROWS; i++) {
      expectedTDigest.add(_tDigests[i]);
    }
    assertEquals(tDigest.quantile(0.5), expectedTDigest.quantile(0.5), PERCENTILE_TDIGEST_DELTA);
  }

  @Test
  public void testInterSegmentAggregation() throws Exception {
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(getAggregationQuery());
    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    assertNotNull(aggregationResults);
    assertEquals(aggregationResults.size(), 5);

    // Simulate the process of server side merge and broker side merge

    // Avg
    AvgPair avgPair1 = new AvgPair(_avgPairs[0].getSum(), _avgPairs[0].getCount());
    AvgPair avgPair2 = new AvgPair(_avgPairs[0].getSum(), _avgPairs[0].getCount());
    for (int i = 1; i < NUM_ROWS; i++) {
      avgPair1.apply(_avgPairs[i]);
      avgPair2.apply(_avgPairs[i]);
    }
    avgPair1.apply(avgPair2);
    avgPair1 = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(ObjectSerDeUtils.AVG_PAIR_SER_DE.serialize(avgPair1));
    avgPair2 = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(ObjectSerDeUtils.AVG_PAIR_SER_DE.serialize(avgPair1));
    avgPair1.apply(avgPair2);
    assertEquals(Double.parseDouble((String) aggregationResults.get(0).getValue()),
        avgPair1.getSum() / avgPair1.getCount(), 1e-5);

    // DistinctCountHLL
    HyperLogLog hyperLogLog1 = new HyperLogLog(DISTINCT_COUNT_HLL_LOG2M);
    HyperLogLog hyperLogLog2 = new HyperLogLog(DISTINCT_COUNT_HLL_LOG2M);
    for (int value : _valuesArray[0]) {
      hyperLogLog1.offer(value);
      hyperLogLog2.offer(value);
    }
    for (int i = 1; i < NUM_ROWS; i++) {
      hyperLogLog1.addAll(_hyperLogLogs[i]);
      hyperLogLog2.addAll(_hyperLogLogs[i]);
    }
    hyperLogLog1.addAll(hyperLogLog2);
    hyperLogLog1 = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE
        .deserialize(ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize(hyperLogLog1));
    hyperLogLog2 = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE
        .deserialize(ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize(hyperLogLog1));
    hyperLogLog1.addAll(hyperLogLog2);
    assertEquals(Long.parseLong((String) aggregationResults.get(1).getValue()), hyperLogLog1.cardinality());

    // MinMaxRange
    MinMaxRangePair minMaxRangePair1 =
        new MinMaxRangePair(_minMaxRangePairs[0].getMin(), _minMaxRangePairs[0].getMax());
    MinMaxRangePair minMaxRangePair2 =
        new MinMaxRangePair(_minMaxRangePairs[0].getMin(), _minMaxRangePairs[0].getMax());
    for (int i = 1; i < NUM_ROWS; i++) {
      minMaxRangePair1.apply(_minMaxRangePairs[i]);
      minMaxRangePair2.apply(_minMaxRangePairs[i]);
    }
    minMaxRangePair1.apply(minMaxRangePair2);
    minMaxRangePair1 = ObjectSerDeUtils.MIN_MAX_RANGE_PAIR_SER_DE
        .deserialize(ObjectSerDeUtils.MIN_MAX_RANGE_PAIR_SER_DE.serialize(minMaxRangePair1));
    minMaxRangePair2 = ObjectSerDeUtils.MIN_MAX_RANGE_PAIR_SER_DE
        .deserialize(ObjectSerDeUtils.MIN_MAX_RANGE_PAIR_SER_DE.serialize(minMaxRangePair1));
    minMaxRangePair1.apply(minMaxRangePair2);
    assertEquals(Double.parseDouble((String) aggregationResults.get(2).getValue()),
        minMaxRangePair1.getMax() - minMaxRangePair1.getMin(), 1e-5);

    // PercentileEst
    QuantileDigest quantileDigest1 = new QuantileDigest(PERCENTILE_EST_MAX_ERROR);
    QuantileDigest quantileDigest2 = new QuantileDigest(PERCENTILE_EST_MAX_ERROR);
    for (int value : _valuesArray[0]) {
      quantileDigest1.add(value);
      quantileDigest2.add(value);
    }
    for (int i = 1; i < NUM_ROWS; i++) {
      quantileDigest1.merge(_quantileDigests[i]);
      quantileDigest2.merge(_quantileDigests[i]);
    }
    quantileDigest1.merge(quantileDigest2);
    quantileDigest1 = ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE
        .deserialize(ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE.serialize(quantileDigest1));
    quantileDigest2 = ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE
        .deserialize(ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE.serialize(quantileDigest1));
    quantileDigest1.merge(quantileDigest2);
    assertEquals(Long.parseLong((String) aggregationResults.get(3).getValue()), quantileDigest1.getQuantile(0.5));

    // PercentileTDigest
    TDigest tDigest1 = TDigest.createMergingDigest(PERCENTILE_TDIGEST_COMPRESSION);
    TDigest tDigest2 = TDigest.createMergingDigest(PERCENTILE_TDIGEST_COMPRESSION);
    for (int value : _valuesArray[0]) {
      tDigest1.add(value);
      tDigest2.add(value);
    }
    for (int i = 1; i < NUM_ROWS; i++) {
      tDigest1.add(_tDigests[i]);
      tDigest2.add(_tDigests[i]);
    }
    tDigest1.add(tDigest2);
    tDigest1 = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(ObjectSerDeUtils.TDIGEST_SER_DE.serialize(tDigest1));
    tDigest2 = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(ObjectSerDeUtils.TDIGEST_SER_DE.serialize(tDigest1));
    tDigest1.add(tDigest2);
    assertEquals(Double.parseDouble((String) aggregationResults.get(4).getValue()), tDigest1.quantile(0.5),
        PERCENTILE_TDIGEST_DELTA);
  }

  @Test
  public void testInnerSegmentSVGroupBy() throws Exception {
    AggregationGroupByOperator groupByOperator = getOperatorForPqlQuery(getSVGroupByQuery());
    IntermediateResultsBlock resultsBlock = groupByOperator.nextBlock();
    AggregationGroupByResult groupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(groupByResult);

    Iterator<GroupKeyGenerator.StringGroupKey> groupKeyIterator = groupByResult.getStringGroupKeyIterator();
    while (groupKeyIterator.hasNext()) {
      GroupKeyGenerator.StringGroupKey groupKey = groupKeyIterator.next();
      int groupId = Integer.parseInt(groupKey._stringKey.substring(1));

      // Avg
      AvgPair avgPair = (AvgPair) groupByResult.getResultForKey(groupKey, 0);
      AvgPair expectedAvgPair = new AvgPair(_avgPairs[groupId].getSum(), _avgPairs[groupId].getCount());
      for (int i = groupId + NUM_GROUPS; i < NUM_ROWS; i += NUM_GROUPS) {
        expectedAvgPair.apply(_avgPairs[i]);
      }
      assertEquals(avgPair.getSum(), expectedAvgPair.getSum());
      assertEquals(avgPair.getCount(), expectedAvgPair.getCount());

      // DistinctCountHLL
      HyperLogLog hyperLogLog = (HyperLogLog) groupByResult.getResultForKey(groupKey, 1);
      HyperLogLog expectedHyperLogLog = new HyperLogLog(DISTINCT_COUNT_HLL_LOG2M);
      for (int value : _valuesArray[groupId]) {
        expectedHyperLogLog.offer(value);
      }
      for (int i = groupId + NUM_GROUPS; i < NUM_ROWS; i += NUM_GROUPS) {
        expectedHyperLogLog.addAll(_hyperLogLogs[i]);
      }
      assertEquals(hyperLogLog.cardinality(), expectedHyperLogLog.cardinality());

      // MinMaxRange
      MinMaxRangePair minMaxRangePair = (MinMaxRangePair) groupByResult.getResultForKey(groupKey, 2);
      MinMaxRangePair expectedMinMaxRangePair =
          new MinMaxRangePair(_minMaxRangePairs[groupId].getMin(), _minMaxRangePairs[groupId].getMax());
      for (int i = groupId + NUM_GROUPS; i < NUM_ROWS; i += NUM_GROUPS) {
        expectedMinMaxRangePair.apply(_minMaxRangePairs[i]);
      }
      assertEquals(minMaxRangePair.getMin(), expectedMinMaxRangePair.getMin());
      assertEquals(minMaxRangePair.getMax(), expectedMinMaxRangePair.getMax());

      // PercentileEst
      QuantileDigest quantileDigest = (QuantileDigest) groupByResult.getResultForKey(groupKey, 3);
      QuantileDigest expectedQuantileDigest = new QuantileDigest(PERCENTILE_EST_MAX_ERROR);
      for (int value : _valuesArray[groupId]) {
        expectedQuantileDigest.add(value);
      }
      for (int i = groupId + NUM_GROUPS; i < NUM_ROWS; i += NUM_GROUPS) {
        expectedQuantileDigest.merge(_quantileDigests[i]);
      }
      assertEquals(quantileDigest.getQuantile(0.5), expectedQuantileDigest.getQuantile(0.5));

      // PercentileTDigest
      TDigest tDigest = (TDigest) groupByResult.getResultForKey(groupKey, 4);
      TDigest expectedTDigest = TDigest.createMergingDigest(PERCENTILE_TDIGEST_COMPRESSION);
      for (int value : _valuesArray[groupId]) {
        expectedTDigest.add(value);
      }
      for (int i = groupId + NUM_GROUPS; i < NUM_ROWS; i += NUM_GROUPS) {
        expectedTDigest.add(_tDigests[i]);
      }
      assertEquals(tDigest.quantile(0.5), expectedTDigest.quantile(0.5), PERCENTILE_TDIGEST_DELTA);
    }
  }

  @Test
  public void testInterSegmentSVGroupBy() throws Exception {
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(getSVGroupByQuery());
    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    assertNotNull(aggregationResults);
    assertEquals(aggregationResults.size(), 5);

    // Simulate the process of server side merge and broker side merge

    // Avg
    List<GroupByResult> groupByResults = aggregationResults.get(0).getGroupByResult();
    assertEquals(groupByResults.size(), 3);
    for (GroupByResult groupByResult : groupByResults) {
      int groupId = Integer.parseInt(groupByResult.getGroup().get(0).substring(1));

      AvgPair avgPair1 = new AvgPair(_avgPairs[groupId].getSum(), _avgPairs[groupId].getCount());
      AvgPair avgPair2 = new AvgPair(_avgPairs[groupId].getSum(), _avgPairs[groupId].getCount());
      for (int i = groupId + NUM_GROUPS; i < NUM_ROWS; i += NUM_GROUPS) {
        avgPair1.apply(_avgPairs[i]);
        avgPair2.apply(_avgPairs[i]);
      }
      avgPair1.apply(avgPair2);
      avgPair1 = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(ObjectSerDeUtils.AVG_PAIR_SER_DE.serialize(avgPair1));
      avgPair2 = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(ObjectSerDeUtils.AVG_PAIR_SER_DE.serialize(avgPair1));
      avgPair1.apply(avgPair2);
      assertEquals(Double.parseDouble((String) groupByResult.getValue()), avgPair1.getSum() / avgPair1.getCount(),
          1e-5);
    }

    // DistinctCountHLL
    groupByResults = aggregationResults.get(1).getGroupByResult();
    assertEquals(groupByResults.size(), 3);
    for (GroupByResult groupByResult : groupByResults) {
      int groupId = Integer.parseInt(groupByResult.getGroup().get(0).substring(1));

      HyperLogLog hyperLogLog1 = new HyperLogLog(DISTINCT_COUNT_HLL_LOG2M);
      HyperLogLog hyperLogLog2 = new HyperLogLog(DISTINCT_COUNT_HLL_LOG2M);
      for (int value : _valuesArray[groupId]) {
        hyperLogLog1.offer(value);
        hyperLogLog2.offer(value);
      }
      for (int i = groupId + NUM_GROUPS; i < NUM_ROWS; i += NUM_GROUPS) {
        hyperLogLog1.addAll(_hyperLogLogs[i]);
        hyperLogLog2.addAll(_hyperLogLogs[i]);
      }
      hyperLogLog1.addAll(hyperLogLog2);
      hyperLogLog1 = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE
          .deserialize(ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize(hyperLogLog1));
      hyperLogLog2 = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE
          .deserialize(ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize(hyperLogLog1));
      hyperLogLog1.addAll(hyperLogLog2);
      assertEquals(Long.parseLong((String) groupByResult.getValue()), hyperLogLog1.cardinality());
    }

    // MinMaxRange
    groupByResults = aggregationResults.get(2).getGroupByResult();
    assertEquals(groupByResults.size(), 3);
    for (GroupByResult groupByResult : groupByResults) {
      int groupId = Integer.parseInt(groupByResult.getGroup().get(0).substring(1));

      MinMaxRangePair minMaxRangePair1 =
          new MinMaxRangePair(_minMaxRangePairs[groupId].getMin(), _minMaxRangePairs[groupId].getMax());
      MinMaxRangePair minMaxRangePair2 =
          new MinMaxRangePair(_minMaxRangePairs[groupId].getMin(), _minMaxRangePairs[groupId].getMax());
      for (int i = groupId + NUM_GROUPS; i < NUM_ROWS; i += NUM_GROUPS) {
        minMaxRangePair1.apply(_minMaxRangePairs[i]);
        minMaxRangePair2.apply(_minMaxRangePairs[i]);
      }
      minMaxRangePair1.apply(minMaxRangePair2);
      minMaxRangePair1 = ObjectSerDeUtils.MIN_MAX_RANGE_PAIR_SER_DE
          .deserialize(ObjectSerDeUtils.MIN_MAX_RANGE_PAIR_SER_DE.serialize(minMaxRangePair1));
      minMaxRangePair2 = ObjectSerDeUtils.MIN_MAX_RANGE_PAIR_SER_DE
          .deserialize(ObjectSerDeUtils.MIN_MAX_RANGE_PAIR_SER_DE.serialize(minMaxRangePair1));
      minMaxRangePair1.apply(minMaxRangePair2);
      assertEquals(Double.parseDouble((String) groupByResult.getValue()),
          minMaxRangePair1.getMax() - minMaxRangePair1.getMin(), 1e-5);
    }

    // PercentileEst
    groupByResults = aggregationResults.get(3).getGroupByResult();
    assertEquals(groupByResults.size(), 3);
    for (GroupByResult groupByResult : groupByResults) {
      int groupId = Integer.parseInt(groupByResult.getGroup().get(0).substring(1));

      QuantileDigest quantileDigest1 = new QuantileDigest(PERCENTILE_EST_MAX_ERROR);
      QuantileDigest quantileDigest2 = new QuantileDigest(PERCENTILE_EST_MAX_ERROR);
      for (int value : _valuesArray[groupId]) {
        quantileDigest1.add(value);
        quantileDigest2.add(value);
      }
      for (int i = groupId + NUM_GROUPS; i < NUM_ROWS; i += NUM_GROUPS) {
        quantileDigest1.merge(_quantileDigests[i]);
        quantileDigest2.merge(_quantileDigests[i]);
      }
      quantileDigest1.merge(quantileDigest2);
      quantileDigest1 = ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE
          .deserialize(ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE.serialize(quantileDigest1));
      quantileDigest2 = ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE
          .deserialize(ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE.serialize(quantileDigest1));
      quantileDigest1.merge(quantileDigest2);
      assertEquals(Long.parseLong((String) groupByResult.getValue()), quantileDigest1.getQuantile(0.5));
    }

    // PercentileTDigest
    groupByResults = aggregationResults.get(4).getGroupByResult();
    assertEquals(groupByResults.size(), 3);
    for (GroupByResult groupByResult : groupByResults) {
      int groupId = Integer.parseInt(groupByResult.getGroup().get(0).substring(1));

      TDigest tDigest1 = TDigest.createMergingDigest(PERCENTILE_TDIGEST_COMPRESSION);
      TDigest tDigest2 = TDigest.createMergingDigest(PERCENTILE_TDIGEST_COMPRESSION);
      for (int value : _valuesArray[groupId]) {
        tDigest1.add(value);
        tDigest2.add(value);
      }
      for (int i = groupId + NUM_GROUPS; i < NUM_ROWS; i += NUM_GROUPS) {
        tDigest1.add(_tDigests[i]);
        tDigest2.add(_tDigests[i]);
      }
      tDigest1.add(tDigest2);
      tDigest1 = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(ObjectSerDeUtils.TDIGEST_SER_DE.serialize(tDigest1));
      tDigest2 = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(ObjectSerDeUtils.TDIGEST_SER_DE.serialize(tDigest1));
      tDigest1.add(tDigest2);
      assertEquals(Double.parseDouble((String) groupByResult.getValue()), tDigest1.quantile(0.5),
          PERCENTILE_TDIGEST_DELTA);
    }
  }

  @Test
  public void testInnerSegmentMVGroupBy() throws Exception {
    AggregationGroupByOperator groupByOperator = getOperatorForPqlQuery(getMVGroupByQuery());
    IntermediateResultsBlock resultsBlock = groupByOperator.nextBlock();
    AggregationGroupByResult groupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(groupByResult);

    // Avg
    AvgPair expectedAvgPair = new AvgPair(_avgPairs[0].getSum(), _avgPairs[0].getCount());
    for (int i = 1; i < NUM_ROWS; i++) {
      expectedAvgPair.apply(_avgPairs[i]);
    }

    // DistinctCountHLL
    HyperLogLog expectedHyperLogLog = new HyperLogLog(DISTINCT_COUNT_HLL_LOG2M);
    for (int value : _valuesArray[0]) {
      expectedHyperLogLog.offer(value);
    }
    for (int i = 1; i < NUM_ROWS; i++) {
      expectedHyperLogLog.addAll(_hyperLogLogs[i]);
    }

    // MinMaxRange
    MinMaxRangePair expectedMinMaxRangePair =
        new MinMaxRangePair(_minMaxRangePairs[0].getMin(), _minMaxRangePairs[0].getMax());
    for (int i = 1; i < NUM_ROWS; i++) {
      expectedMinMaxRangePair.apply(_minMaxRangePairs[i]);
    }

    // PercentileEst
    QuantileDigest expectedQuantileDigest = new QuantileDigest(PERCENTILE_EST_MAX_ERROR);
    for (int value : _valuesArray[0]) {
      expectedQuantileDigest.add(value);
    }
    for (int i = 1; i < NUM_ROWS; i++) {
      expectedQuantileDigest.merge(_quantileDigests[i]);
    }

    // PercentileTDigest
    TDigest expectedTDigest = TDigest.createMergingDigest(PERCENTILE_TDIGEST_COMPRESSION);
    for (int value : _valuesArray[0]) {
      expectedTDigest.add(value);
    }
    for (int i = 1; i < NUM_ROWS; i++) {
      expectedTDigest.add(_tDigests[i]);
    }

    Iterator<GroupKeyGenerator.StringGroupKey> groupKeyIterator = groupByResult.getStringGroupKeyIterator();
    while (groupKeyIterator.hasNext()) {
      GroupKeyGenerator.StringGroupKey groupKey = groupKeyIterator.next();

      // Avg
      AvgPair avgPair = (AvgPair) groupByResult.getResultForKey(groupKey, 0);
      assertEquals(avgPair.getSum(), expectedAvgPair.getSum());
      assertEquals(avgPair.getCount(), expectedAvgPair.getCount());

      // DistinctCountHLL
      HyperLogLog hyperLogLog = (HyperLogLog) groupByResult.getResultForKey(groupKey, 1);
      assertEquals(hyperLogLog.cardinality(), expectedHyperLogLog.cardinality());

      // MinMaxRange
      MinMaxRangePair minMaxRangePair = (MinMaxRangePair) groupByResult.getResultForKey(groupKey, 2);
      assertEquals(minMaxRangePair.getMin(), expectedMinMaxRangePair.getMin());
      assertEquals(minMaxRangePair.getMax(), expectedMinMaxRangePair.getMax());

      // PercentileEst
      QuantileDigest quantileDigest = (QuantileDigest) groupByResult.getResultForKey(groupKey, 3);
      assertEquals(quantileDigest.getQuantile(0.5), expectedQuantileDigest.getQuantile(0.5));

      // PercentileTDigest
      TDigest tDigest = (TDigest) groupByResult.getResultForKey(groupKey, 4);
      assertEquals(tDigest.quantile(0.5), expectedTDigest.quantile(0.5), PERCENTILE_TDIGEST_DELTA);
    }
  }

  @Test
  public void testInterSegmentMVGroupBy() throws Exception {
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(getMVGroupByQuery());
    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    assertNotNull(aggregationResults);
    assertEquals(aggregationResults.size(), 5);

    // Simulate the process of server side merge and broker side merge

    // Avg
    AvgPair avgPair1 = new AvgPair(_avgPairs[0].getSum(), _avgPairs[0].getCount());
    AvgPair avgPair2 = new AvgPair(_avgPairs[0].getSum(), _avgPairs[0].getCount());
    for (int i = 1; i < NUM_ROWS; i++) {
      avgPair1.apply(_avgPairs[i]);
      avgPair2.apply(_avgPairs[i]);
    }
    avgPair1.apply(avgPair2);
    avgPair1 = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(ObjectSerDeUtils.AVG_PAIR_SER_DE.serialize(avgPair1));
    avgPair2 = ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(ObjectSerDeUtils.AVG_PAIR_SER_DE.serialize(avgPair1));
    avgPair1.apply(avgPair2);
    List<GroupByResult> groupByResults = aggregationResults.get(0).getGroupByResult();
    assertEquals(groupByResults.size(), 3);
    for (GroupByResult groupByResult : groupByResults) {
      assertEquals(Double.parseDouble((String) groupByResult.getValue()), avgPair1.getSum() / avgPair1.getCount(),
          1e-5);
    }

    // DistinctCountHLL
    HyperLogLog hyperLogLog1 = new HyperLogLog(DISTINCT_COUNT_HLL_LOG2M);
    HyperLogLog hyperLogLog2 = new HyperLogLog(DISTINCT_COUNT_HLL_LOG2M);
    for (int value : _valuesArray[0]) {
      hyperLogLog1.offer(value);
      hyperLogLog2.offer(value);
    }
    for (int i = 1; i < NUM_ROWS; i++) {
      hyperLogLog1.addAll(_hyperLogLogs[i]);
      hyperLogLog2.addAll(_hyperLogLogs[i]);
    }
    hyperLogLog1.addAll(hyperLogLog2);
    hyperLogLog1 = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE
        .deserialize(ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize(hyperLogLog1));
    hyperLogLog2 = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE
        .deserialize(ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize(hyperLogLog1));
    hyperLogLog1.addAll(hyperLogLog2);
    groupByResults = aggregationResults.get(1).getGroupByResult();
    assertEquals(groupByResults.size(), 3);
    for (GroupByResult groupByResult : groupByResults) {
      assertEquals(Long.parseLong((String) groupByResult.getValue()), hyperLogLog1.cardinality());
    }

    // MinMaxRange
    MinMaxRangePair minMaxRangePair1 =
        new MinMaxRangePair(_minMaxRangePairs[0].getMin(), _minMaxRangePairs[0].getMax());
    MinMaxRangePair minMaxRangePair2 =
        new MinMaxRangePair(_minMaxRangePairs[0].getMin(), _minMaxRangePairs[0].getMax());
    for (int i = 1; i < NUM_ROWS; i++) {
      minMaxRangePair1.apply(_minMaxRangePairs[i]);
      minMaxRangePair2.apply(_minMaxRangePairs[i]);
    }
    minMaxRangePair1.apply(minMaxRangePair2);
    minMaxRangePair1 = ObjectSerDeUtils.MIN_MAX_RANGE_PAIR_SER_DE
        .deserialize(ObjectSerDeUtils.MIN_MAX_RANGE_PAIR_SER_DE.serialize(minMaxRangePair1));
    minMaxRangePair2 = ObjectSerDeUtils.MIN_MAX_RANGE_PAIR_SER_DE
        .deserialize(ObjectSerDeUtils.MIN_MAX_RANGE_PAIR_SER_DE.serialize(minMaxRangePair1));
    minMaxRangePair1.apply(minMaxRangePair2);
    groupByResults = aggregationResults.get(2).getGroupByResult();
    assertEquals(groupByResults.size(), 3);
    for (GroupByResult groupByResult : groupByResults) {
      assertEquals(Double.parseDouble((String) groupByResult.getValue()),
          minMaxRangePair1.getMax() - minMaxRangePair1.getMin(), 1e-5);
    }

    // PercentileEst
    QuantileDigest quantileDigest1 = new QuantileDigest(PERCENTILE_EST_MAX_ERROR);
    QuantileDigest quantileDigest2 = new QuantileDigest(PERCENTILE_EST_MAX_ERROR);
    for (int value : _valuesArray[0]) {
      quantileDigest1.add(value);
      quantileDigest2.add(value);
    }
    for (int i = 1; i < NUM_ROWS; i++) {
      quantileDigest1.merge(_quantileDigests[i]);
      quantileDigest2.merge(_quantileDigests[i]);
    }
    quantileDigest1.merge(quantileDigest2);
    quantileDigest1 = ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE
        .deserialize(ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE.serialize(quantileDigest1));
    quantileDigest2 = ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE
        .deserialize(ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE.serialize(quantileDigest1));
    quantileDigest1.merge(quantileDigest2);
    groupByResults = aggregationResults.get(3).getGroupByResult();
    assertEquals(groupByResults.size(), 3);
    for (GroupByResult groupByResult : groupByResults) {
      assertEquals(Long.parseLong((String) groupByResult.getValue()), quantileDigest1.getQuantile(0.5));
    }

    // PercentileTDigest
    TDigest tDigest1 = TDigest.createMergingDigest(PERCENTILE_TDIGEST_COMPRESSION);
    TDigest tDigest2 = TDigest.createMergingDigest(PERCENTILE_TDIGEST_COMPRESSION);
    for (int value : _valuesArray[0]) {
      tDigest1.add(value);
      tDigest2.add(value);
    }
    for (int i = 1; i < NUM_ROWS; i++) {
      tDigest1.add(_tDigests[i]);
      tDigest2.add(_tDigests[i]);
    }
    tDigest1.add(tDigest2);
    tDigest1 = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(ObjectSerDeUtils.TDIGEST_SER_DE.serialize(tDigest1));
    tDigest2 = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(ObjectSerDeUtils.TDIGEST_SER_DE.serialize(tDigest1));
    tDigest1.add(tDigest2);
    groupByResults = aggregationResults.get(4).getGroupByResult();
    assertEquals(groupByResults.size(), 3);
    for (GroupByResult groupByResult : groupByResults) {
      assertEquals(Double.parseDouble((String) groupByResult.getValue()), tDigest1.quantile(0.5),
          PERCENTILE_TDIGEST_DELTA);
    }
  }

  private String getAggregationQuery() {
    return String.format(
        "SELECT AVG(%s), DISTINCTCOUNTHLL(%s), MINMAXRANGE(%s), PERCENTILEEST50(%s), PERCENTILETDIGEST50(%s) FROM %s",
        AVG_COLUMN, DISTINCT_COUNT_HLL_COLUMN, MIN_MAX_RANGE_COLUMN, PERCENTILE_EST_COLUMN, PERCENTILE_TDIGEST_COLUMN,
        RAW_TABLE_NAME);
  }

  private String getSVGroupByQuery() {
    return String.format("%s GROUP BY %s", getAggregationQuery(), GROUP_BY_SV_COLUMN);
  }

  private String getMVGroupByQuery() {
    return String.format("%s GROUP BY %s", getAggregationQuery(), GROUP_BY_MV_COLUMN);
  }

  @AfterClass
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }
}
