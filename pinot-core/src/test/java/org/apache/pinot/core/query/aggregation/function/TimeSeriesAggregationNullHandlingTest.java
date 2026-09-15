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
package org.apache.pinot.core.query.aggregation.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.tsdb.spi.series.SimpleTimeSeriesBuilderFactory;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactoryProvider;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/// Null handling for `TIMESERIESAGGREGATE`.
///
/// This cannot be reached through [AggregationFunctionNullContractTest], which builds every function from a bare
/// [org.apache.pinot.common.request.context.FunctionContext]; this one needs time-series plan context. It is not
/// reachable from SQL either, but the time-series plan node passes the query options straight through, so a
/// time-series query that sets `enableNullHandling` arrives here with the option on.
///
/// The rows are laid out as two buckets of two rows: timestamps `100, 100, 110, 110` fall into bucket `0` and bucket
/// `1` respectively, and the aggregation is a `SUM`. The bucket values are read off the series builder rather than
/// through [TimeSeriesAggregationFunction#extractFinalResult], because that is the path the time-series engine takes
/// and because a bucket no row landed in stays `null`, which the final-result conversion cannot represent.
public class TimeSeriesAggregationNullHandlingTest {
  private static final String LANGUAGE = "TimeSeriesAggregationNullHandlingTest";
  private static final ExpressionContext TIME = ExpressionContext.forIdentifier("time");
  private static final ExpressionContext VALUE = ExpressionContext.forIdentifier("value");
  private static final long[] TIMESTAMPS = {100L, 100L, 110L, 110L};
  private static final double[] VALUES = {1.0, 2.0, 3.0, 4.0};

  @BeforeClass
  public void setUp() {
    TimeSeriesBuilderFactoryProvider.registerSeriesBuilderFactory(LANGUAGE, new SimpleTimeSeriesBuilderFactory());
  }

  @Test
  public void nullValueIsNotAggregatedWhenNullHandlingEnabled() {
    Double[] buckets = aggregate(true, RoaringBitmap.bitmapOf(0), null);

    assertEquals(buckets[0].doubleValue(), 2.0);
    assertEquals(buckets[1].doubleValue(), 7.0);
  }

  @Test
  public void nullValueIsAggregatedWhenNullHandlingDisabled() {
    Double[] buckets = aggregate(false, RoaringBitmap.bitmapOf(0), null);

    assertEquals(buckets[0].doubleValue(), 3.0);
    assertEquals(buckets[1].doubleValue(), 7.0);
  }

  /// A null timestamp leaves the point with no bucket to land in, so the row is skipped even though its value is
  /// perfectly good.
  @Test
  public void nullTimestampSkipsTheRow() {
    Double[] buckets = aggregate(true, null, RoaringBitmap.bitmapOf(2));

    assertEquals(buckets[0].doubleValue(), 3.0);
    assertEquals(buckets[1].doubleValue(), 4.0);
  }

  @Test
  public void aBucketWhoseRowsAreAllNullIsNeverWrittenTo() {
    Double[] buckets = aggregate(true, RoaringBitmap.bitmapOf(0, 1), null);

    assertNull(buckets[0]);
    assertEquals(buckets[1].doubleValue(), 7.0);
  }

  @Test
  public void everyRowNullLeavesNothingAggregated() {
    TimeSeriesAggregationFunction function = function(true);
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(VALUES.length, holder, block(RoaringBitmap.bitmapOf(0, 1, 2, 3), null));

    assertNull(function.extractAggregationResult(holder), "the holder should never have been touched");
  }

  @Test
  public void nullValueIsNotAggregatedIntoItsGroup() {
    TimeSeriesAggregationFunction function = function(true);
    GroupByResultHolder holder = function.createGroupByResultHolder(2, 2);
    function.aggregateGroupBySV(VALUES.length, new int[]{0, 0, 1, 1}, holder,
        block(RoaringBitmap.bitmapOf(0), null));

    assertEquals(function.extractGroupByResult(holder, 0).build().getDoubleValues()[0].doubleValue(), 2.0);
    assertEquals(function.extractGroupByResult(holder, 1).build().getDoubleValues()[1].doubleValue(), 7.0);
  }

  private static Double[] aggregate(boolean nullHandlingEnabled, RoaringBitmap valueNulls, RoaringBitmap timeNulls) {
    TimeSeriesAggregationFunction function = function(nullHandlingEnabled);
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(VALUES.length, holder, block(valueNulls, timeNulls));
    return function.extractAggregationResult(holder).build().getDoubleValues();
  }

  private static TimeSeriesAggregationFunction function(boolean nullHandlingEnabled) {
    return new TimeSeriesAggregationFunction(List.of(
        ExpressionContext.forLiteral(Literal.stringValue(LANGUAGE)),
        ExpressionContext.forLiteral(Literal.stringValue("SUM")),
        VALUE,
        TIME,
        ExpressionContext.forLiteral(Literal.stringValue("SECONDS")),
        ExpressionContext.forLiteral(Literal.longValue(0)),
        ExpressionContext.forLiteral(Literal.longValue(100)),
        ExpressionContext.forLiteral(Literal.longValue(10)),
        ExpressionContext.forLiteral(Literal.intValue(2)),
        ExpressionContext.forLiteral(Literal.stringValue(""))), nullHandlingEnabled);
  }

  private static Map<ExpressionContext, BlockValSet> block(RoaringBitmap valueNulls, RoaringBitmap timeNulls) {
    return Map.of(
        TIME, SyntheticBlockValSets.Long.create(timeNulls, TIMESTAMPS),
        VALUE, SyntheticBlockValSets.Double.create(valueNulls, VALUES)
    );
  }
}
