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
package org.apache.pinot.core.operator.timeseries;

import java.time.Duration;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.blocks.results.TimeSeriesResultsBlock;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.SimpleTimeSeriesBuilderFactory;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactory;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class TimeSeriesAggregationOperatorTest {
  private static final Random RANDOM = new Random();
  private static final String DUMMY_TIME_COLUMN = "someTimeColumn";
  private static final String GROUP_BY_COLUMN = "city";
  private static final AggInfo AGG_INFO = new AggInfo("SUM", Collections.emptyMap());
  private static final ExpressionContext VALUE_EXPRESSION = ExpressionContext.forIdentifier("someValueColumn");
  private static final TimeBuckets TIME_BUCKETS = TimeBuckets.ofSeconds(1000, Duration.ofSeconds(100), 10);
  private static final int NUM_DOCS_IN_DUMMY_DATA = 1000;

  @Test
  public void testTimeSeriesAggregationOperator() {
    TimeSeriesAggregationOperator timeSeriesAggregationOperator = buildOperatorWithSampleData(
        new SimpleTimeSeriesBuilderFactory());
    TimeSeriesResultsBlock resultsBlock = timeSeriesAggregationOperator.getNextBlock();
    // Expect 2 series: Chicago and San Francisco
    assertNotNull(resultsBlock);
    assertEquals(2, resultsBlock.getNumRows());
  }

  @Test
  public void testTimeSeriesAggregationOperatorWhenSeriesLimit() {
    // Since we test with 2 series, use 1 as the limit.
    TimeSeriesAggregationOperator timeSeriesAggregationOperator = buildOperatorWithSampleData(
        new SimpleTimeSeriesBuilderFactory(1, 100_000_000L));
    try {
      timeSeriesAggregationOperator.getNextBlock();
      fail();
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Limit: 1. Series in current"));
    }
  }

  @Test
  public void testTimeSeriesAggregationOperatorWhenDataPoints() {
    // Since we test with 2 series, use 1 as the limit.
    TimeSeriesAggregationOperator timeSeriesAggregationOperator = buildOperatorWithSampleData(
        new SimpleTimeSeriesBuilderFactory(1000, 11));
    try {
      timeSeriesAggregationOperator.getNextBlock();
      fail();
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Limit: 11. Data points in current"));
    }
  }

  @Test
  public void testGetTimeValueIndexForSeconds() {
    /*
     * TimeBuckets: [10_000, 10_100, 10_200, ..., 10_900]
     * storedTimeValues: [9_999, 10_000, 9_999, 9_901, 10_100, 10_899, 10_900]
     * expected indexes: [0, 0, 0, 0, 1, 9, 9]
     */
    final int[] expectedIndexes = new int[]{0, 0, 0, 0, 1, 9, 9};
    final TimeUnit storedTimeUnit = TimeUnit.SECONDS;
    TimeBuckets timeBuckets = TimeBuckets.ofSeconds(10_000, Duration.ofSeconds(100), 10);
    TimeSeriesAggregationOperator aggregationOperator = buildOperator(storedTimeUnit, timeBuckets);
    long[] storedTimeValues = new long[]{9_999L, 10_000L, 9_999L, 9_901L, 10_100L, 10_899L, 10_900L};
    int[] indexes = aggregationOperator.getTimeValueIndex(storedTimeValues, storedTimeValues.length);
    assertEquals(indexes, expectedIndexes);
  }

  @Test
  public void testGetTimeValueIndexForMillis() {
    /*
     * TimeBuckets: [10_000, 10_100, 10_200, ..., 10_900]
     * storedTimeValues: [9_999_000, 10_000_000, 10_500_000, 10_899_999, 10_800_001, 10_900_000]
     * expected indexes: [0, 0, 5, 9, 9, 9]
     */
    final int[] expectedIndexes = new int[]{0, 0, 5, 9, 9, 9};
    final TimeUnit storedTimeUnit = TimeUnit.MILLISECONDS;
    TimeBuckets timeBuckets = TimeBuckets.ofSeconds(10_000, Duration.ofSeconds(100), 10);
    TimeSeriesAggregationOperator aggregationOperator = buildOperator(storedTimeUnit, timeBuckets);
    long[] storedTimeValues = new long[]{9_999_000L, 10_000_000L, 10_500_000L, 10_899_999L, 10_800_001L, 10_900_000L};
    int[] indexes = aggregationOperator.getTimeValueIndex(storedTimeValues, storedTimeValues.length);
    assertEquals(indexes, expectedIndexes);
  }

  @Test
  public void testGetTimeValueIndexOutOfBounds() {
    final TimeUnit storedTimeUnit = TimeUnit.SECONDS;
    final int numTimeBuckets = 10;
    final int windowSeconds = 100;
    TimeBuckets timeBuckets = TimeBuckets.ofSeconds(10_000, Duration.ofSeconds(windowSeconds), numTimeBuckets);
    TimeSeriesAggregationOperator aggregationOperator = buildOperator(storedTimeUnit, timeBuckets);
    testOutOfBoundsTimeValueIndex(new long[]{8_000}, numTimeBuckets, aggregationOperator);
    testOutOfBoundsTimeValueIndex(new long[]{timeBuckets.getTimeRangeEndInclusive() + 1}, numTimeBuckets,
        aggregationOperator);
  }

  private void testOutOfBoundsTimeValueIndex(long[] storedTimeValues, int numTimeBuckets,
      TimeSeriesAggregationOperator aggOperator) {
    assertEquals(storedTimeValues.length, 1, "Misconfigured test: pass single stored time value");
    int[] indexes = aggOperator.getTimeValueIndex(storedTimeValues, storedTimeValues.length);
    assertTrue(indexes[0] < 0 || indexes[0] >= numTimeBuckets, "Expected time index to spill beyond valid range");
  }

  private TimeSeriesAggregationOperator buildOperatorWithSampleData(TimeSeriesBuilderFactory seriesBuilderFactory) {
    BaseProjectOperator<ValueBlock> mockProjectOperator = mock(BaseProjectOperator.class);
    ValueBlock valueBlock = buildValueBlockForProjectOperator();
    when(mockProjectOperator.nextBlock()).thenReturn(valueBlock, (ValueBlock) null);
    return new TimeSeriesAggregationOperator(DUMMY_TIME_COLUMN,
        TimeUnit.SECONDS, 0L, AGG_INFO, VALUE_EXPRESSION, Collections.singletonList(GROUP_BY_COLUMN),
        TIME_BUCKETS, mockProjectOperator, seriesBuilderFactory, mock(SegmentMetadata.class));
  }

  private static ValueBlock buildValueBlockForProjectOperator() {
    ValueBlock valueBlock = mock(ValueBlock.class);
    doReturn(NUM_DOCS_IN_DUMMY_DATA).when(valueBlock).getNumDocs();
    doReturn(buildBlockValSetForTime()).when(valueBlock).getBlockValueSet(DUMMY_TIME_COLUMN);
    doReturn(buildBlockValSetForValues()).when(valueBlock).getBlockValueSet(VALUE_EXPRESSION);
    doReturn(buildBlockValSetForGroupByColumns()).when(valueBlock).getBlockValueSet(GROUP_BY_COLUMN);
    return valueBlock;
  }

  private static BlockValSet buildBlockValSetForGroupByColumns() {
    BlockValSet blockValSet = mock(BlockValSet.class);
    String[] stringArray = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    for (int index = 0; index < NUM_DOCS_IN_DUMMY_DATA; index++) {
      stringArray[index] = RANDOM.nextBoolean() ? "Chicago" : "San Francisco";
    }
    doReturn(stringArray).when(blockValSet).getStringValuesSV();
    doReturn(FieldSpec.DataType.STRING).when(blockValSet).getValueType();
    return blockValSet;
  }

  private static BlockValSet buildBlockValSetForValues() {
    BlockValSet blockValSet = mock(BlockValSet.class);
    long[] valuesArray = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    for (int index = 0; index < NUM_DOCS_IN_DUMMY_DATA; index++) {
      valuesArray[index] = index;
    }
    doReturn(valuesArray).when(blockValSet).getLongValuesSV();
    doReturn(FieldSpec.DataType.LONG).when(blockValSet).getValueType();
    return blockValSet;
  }

  private static BlockValSet buildBlockValSetForTime() {
    BlockValSet blockValSet = mock(BlockValSet.class);
    long[] timeValueArray = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    for (int index = 0; index < NUM_DOCS_IN_DUMMY_DATA; index++) {
      timeValueArray[index] = 901 + RANDOM.nextInt(1000);
    }
    doReturn(timeValueArray).when(blockValSet).getLongValuesSV();
    return blockValSet;
  }

  private TimeSeriesAggregationOperator buildOperator(TimeUnit storedTimeUnit, TimeBuckets timeBuckets) {
    return new TimeSeriesAggregationOperator(
        DUMMY_TIME_COLUMN, storedTimeUnit, 0L, AGG_INFO, VALUE_EXPRESSION, Collections.emptyList(),
        timeBuckets, mock(BaseProjectOperator.class), mock(TimeSeriesBuilderFactory.class),
        mock(SegmentMetadata.class));
  }
}
