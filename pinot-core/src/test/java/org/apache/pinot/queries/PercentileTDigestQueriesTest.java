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

import com.tdunning.math.stats.TDigest;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.GroupByResult;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.AggregationGroupByOperator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.query.aggregation.function.PercentileTDigestAggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests for PERCENTILE_TDIGEST aggregation function.
 *
 * <ul>
 *   <li>Generates a segment with a double column, a TDigest column and a group-by column</li>
 *   <li>Runs aggregation and group-by queries on the generated segment</li>
 *   <li>
 *     Compares the results for PERCENTILE_TDIGEST on double column and TDigest column with results for PERCENTILE on
 *     double column
 *   </li>
 * </ul>
 */
public class PercentileTDigestQueriesTest extends BaseQueriesTest {
  protected static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "PercentileTDigestQueriesTest");
  protected static final String TABLE_NAME = "testTable";
  protected static final String SEGMENT_NAME = "testSegment";

  protected static final int NUM_ROWS = 1000;
  protected static final double VALUE_RANGE = Integer.MAX_VALUE;
  protected static final double DELTA = 0.05 * VALUE_RANGE; // Allow 5% quantile error
  protected static final String DOUBLE_COLUMN = "doubleColumn";
  protected static final String TDIGEST_COLUMN = "tDigestColumn";
  protected static final String GROUP_BY_COLUMN = "groupByColumn";
  protected static final String[] GROUPS = new String[]{"G1", "G2", "G3"};
  protected static final long RANDOM_SEED = System.nanoTime();
  protected static final Random RANDOM = new Random(RANDOM_SEED);
  protected static final String ERROR_MESSAGE = "Random seed: " + RANDOM_SEED;

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

  protected void buildSegment() throws Exception {
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      HashMap<String, Object> valueMap = new HashMap<>();

      double value = RANDOM.nextDouble() * VALUE_RANGE;
      valueMap.put(DOUBLE_COLUMN, value);

      TDigest tDigest = TDigest.createMergingDigest(PercentileTDigestAggregationFunction.DEFAULT_TDIGEST_COMPRESSION);
      tDigest.add(value);
      ByteBuffer byteBuffer = ByteBuffer.allocate(tDigest.byteSize());
      tDigest.asBytes(byteBuffer);
      valueMap.put(TDIGEST_COLUMN, byteBuffer.array());

      String group = GROUPS[RANDOM.nextInt(GROUPS.length)];
      valueMap.put(GROUP_BY_COLUMN, group);

      GenericRow genericRow = new GenericRow();
      genericRow.init(valueMap);
      rows.add(genericRow);
    }

    Schema schema = new Schema();
    schema.addField(new MetricFieldSpec(DOUBLE_COLUMN, FieldSpec.DataType.DOUBLE));
    schema.addField(new MetricFieldSpec(TDIGEST_COLUMN, FieldSpec.DataType.BYTES));
    schema.addField(new DimensionFieldSpec(GROUP_BY_COLUMN, FieldSpec.DataType.STRING, true));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);
    config.setRawIndexCreationColumns(Collections.singletonList(TDIGEST_COLUMN));

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  @Test
  public void testInnerSegmentAggregation() {
    // For inner segment case, percentile does not affect the intermediate result
    AggregationOperator aggregationOperator = getOperatorForPqlQuery(getAggregationQuery(0));
    IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
    List<Object> aggregationResult = resultsBlock.getAggregationResult();
    Assert.assertNotNull(aggregationResult);
    Assert.assertEquals(aggregationResult.size(), 6);
    DoubleList doubleList0 = (DoubleList) aggregationResult.get(0);
    Collections.sort(doubleList0);
    assertTDigest((TDigest) aggregationResult.get(1), doubleList0);
    assertTDigest((TDigest) aggregationResult.get(2), doubleList0);

    DoubleList doubleList3 = (DoubleList) aggregationResult.get(3);
    Collections.sort(doubleList3);
    Assert.assertEquals(doubleList3, doubleList0);
    assertTDigest((TDigest) aggregationResult.get(4), doubleList0);
    assertTDigest((TDigest) aggregationResult.get(5), doubleList0);
  }

  @Test
  public void testInterSegmentAggregation() {
    for (int percentile = 0; percentile <= 100; percentile++) {
      BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(getAggregationQuery(percentile));
      List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
      Assert.assertNotNull(aggregationResults);
      Assert.assertEquals(aggregationResults.size(), 6);
      double expected = Double.parseDouble((String) aggregationResults.get(0).getValue());
      double resultForDoubleColumn1 = Double.parseDouble((String) aggregationResults.get(1).getValue());
      Assert.assertEquals(resultForDoubleColumn1, expected, DELTA, ERROR_MESSAGE);
      double resultForTDigestColumn2 = Double.parseDouble((String) aggregationResults.get(2).getValue());
      Assert.assertEquals(resultForTDigestColumn2, expected, DELTA, ERROR_MESSAGE);
      double resultForDoubleColumn3 = Double.parseDouble((String) aggregationResults.get(3).getValue());
      Assert.assertEquals(resultForDoubleColumn3, expected, DELTA, ERROR_MESSAGE);
      double resultForDoubleColumn4 = Double.parseDouble((String) aggregationResults.get(4).getValue());
      Assert.assertEquals(resultForDoubleColumn4, expected, DELTA, ERROR_MESSAGE);
      double resultForTDigestColumn5 = Double.parseDouble((String) aggregationResults.get(5).getValue());
      Assert.assertEquals(resultForTDigestColumn5, expected, DELTA, ERROR_MESSAGE);
    }
  }

  @Test
  public void testInnerSegmentGroupBy() {
    // For inner segment case, percentile does not affect the intermediate result
    AggregationGroupByOperator groupByOperator = getOperatorForPqlQuery(getGroupByQuery(0));
    IntermediateResultsBlock resultsBlock = groupByOperator.nextBlock();
    AggregationGroupByResult groupByResult = resultsBlock.getAggregationGroupByResult();
    Assert.assertNotNull(groupByResult);
    Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = groupByResult.getGroupKeyIterator();
    while (groupKeyIterator.hasNext()) {
      int groupId = groupKeyIterator.next()._groupId;
      DoubleList doubleList0 = (DoubleList) groupByResult.getResultForGroupId(0, groupId);
      Collections.sort(doubleList0);
      assertTDigest((TDigest) groupByResult.getResultForGroupId(1, groupId), doubleList0);
      assertTDigest((TDigest) groupByResult.getResultForGroupId(2, groupId), doubleList0);

      DoubleList doubleList3 = (DoubleList) groupByResult.getResultForGroupId(3, groupId);
      Collections.sort(doubleList3);
      Assert.assertEquals(doubleList3, doubleList0);
      assertTDigest((TDigest) groupByResult.getResultForGroupId(4, groupId), doubleList0);
      assertTDigest((TDigest) groupByResult.getResultForGroupId(5, groupId), doubleList0);
    }
  }

  @Test
  public void testInterSegmentGroupBy() {
    for (int percentile = 0; percentile <= 100; percentile++) {
      BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(getGroupByQuery(percentile));
      List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
      Assert.assertNotNull(aggregationResults);
      Assert.assertEquals(aggregationResults.size(), 6);
      Map<String, Double> expectedValues = new HashMap<>();
      for (GroupByResult groupByResult : aggregationResults.get(0).getGroupByResult()) {
        expectedValues.put(groupByResult.getGroup().get(0), Double.parseDouble((String) groupByResult.getValue()));
      }
      for (GroupByResult groupByResult : aggregationResults.get(1).getGroupByResult()) {
        String group = groupByResult.getGroup().get(0);
        double expected = expectedValues.get(group);
        double resultForDoubleColumn = Double.parseDouble((String) groupByResult.getValue());
        Assert.assertEquals(resultForDoubleColumn, expected, DELTA, ERROR_MESSAGE);
      }
      for (GroupByResult groupByResult : aggregationResults.get(2).getGroupByResult()) {
        String group = groupByResult.getGroup().get(0);
        double expected = expectedValues.get(group);
        double resultForTDigestColumn = Double.parseDouble((String) groupByResult.getValue());
        Assert.assertEquals(resultForTDigestColumn, expected, DELTA, ERROR_MESSAGE);
      }
      for (GroupByResult groupByResult : aggregationResults.get(3).getGroupByResult()) {
        String group = groupByResult.getGroup().get(0);
        double expected = expectedValues.get(group);
        double resultForTDigestColumn = Double.parseDouble((String) groupByResult.getValue());
        Assert.assertEquals(resultForTDigestColumn, expected, DELTA, ERROR_MESSAGE);
      }
      for (GroupByResult groupByResult : aggregationResults.get(4).getGroupByResult()) {
        String group = groupByResult.getGroup().get(0);
        double expected = expectedValues.get(group);
        double resultForTDigestColumn = Double.parseDouble((String) groupByResult.getValue());
        Assert.assertEquals(resultForTDigestColumn, expected, DELTA, ERROR_MESSAGE);
      }
      for (GroupByResult groupByResult : aggregationResults.get(5).getGroupByResult()) {
        String group = groupByResult.getGroup().get(0);
        double expected = expectedValues.get(group);
        double resultForTDigestColumn = Double.parseDouble((String) groupByResult.getValue());
        Assert.assertEquals(resultForTDigestColumn, expected, DELTA, ERROR_MESSAGE);
      }
    }
  }

  protected String getAggregationQuery(int percentile) {
    return String.format(
        "SELECT PERCENTILE%1$d(%2$s), PERCENTILETDIGEST%1$d(%2$s), PERCENTILETDIGEST%1$d(%3$s), PERCENTILE(%2$s, %1$d), "
            + "PERCENTILETDIGEST(%2$s, %1$d), PERCENTILETDIGEST(%3$s, %1$d) FROM %4$s",
        percentile, DOUBLE_COLUMN, TDIGEST_COLUMN, TABLE_NAME);
  }

  private String getGroupByQuery(int percentile) {
    return String.format("%s GROUP BY %s", getAggregationQuery(percentile), GROUP_BY_COLUMN);
  }

  private void assertTDigest(TDigest tDigest, DoubleList doubleList) {
    for (int percentile = 0; percentile <= 100; percentile++) {
      double expected;
      if (percentile == 100) {
        expected = doubleList.getDouble(doubleList.size() - 1);
      } else {
        expected = doubleList.getDouble(doubleList.size() * percentile / 100);
      }
      Assert.assertEquals(tDigest.quantile(percentile / 100.0), expected, DELTA, ERROR_MESSAGE);
    }
  }

  @AfterClass
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }
}
