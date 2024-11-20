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
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactory;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class TimeSeriesAggregationOperatorTest {
  private static final String DUMMY_TIME_COLUMN = "someTimeColumn";
  private static final AggInfo AGG_INFO = new AggInfo("", Collections.emptyMap());
  private static final ExpressionContext VALUE_EXPRESSION = ExpressionContext.forIdentifier("someValueColumn");

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

  private TimeSeriesAggregationOperator buildOperator(TimeUnit storedTimeUnit, TimeBuckets timeBuckets) {
    return new TimeSeriesAggregationOperator(
        DUMMY_TIME_COLUMN, storedTimeUnit, 0L, AGG_INFO, VALUE_EXPRESSION, Collections.emptyList(),
        timeBuckets, mock(BaseProjectOperator.class), mock(TimeSeriesBuilderFactory.class),
        mock(SegmentMetadata.class));
  }
}
