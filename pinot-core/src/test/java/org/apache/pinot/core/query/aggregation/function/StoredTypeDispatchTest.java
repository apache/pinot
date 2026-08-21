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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.funnel.window.FunnelEventsFunctionEvalAggregationFunction;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.tsdb.spi.series.SimpleTimeSeriesBuilderFactory;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactoryProvider;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;


/// Aggregation functions that read a column through a `switch` must dispatch on the **stored** type, not the
/// logical one.
///
/// `BOOLEAN` is stored as `INT`, `TIMESTAMP` as `LONG` and `JSON` as `STRING`, and the `BlockValSet` getters are
/// keyed to the stored representation. Switching on [BlockValSet#getValueType] instead rejects those three columns
/// as unsupported even though the getter that would read them is already in the switch.
///
/// The tell is a hand-written `case TIMESTAMP:` glued onto `case LONG:`, which is only needed because the dispatch
/// is on the wrong type; `BOOLEAN` and `JSON` never got the same manual patch and so still failed.
public class StoredTypeDispatchTest {
  private static final String LANGUAGE = "StoredTypeDispatchTest";
  private static final int NUM_DOCS = 4;

  @BeforeClass
  public void registerTimeSeriesLanguage() {
    TimeSeriesBuilderFactoryProvider.registerSeriesBuilderFactory(LANGUAGE, new SimpleTimeSeriesBuilderFactory());
  }

  /// Reports `logicalType` while serving the values of `delegate`, which is how a `BOOLEAN`, `TIMESTAMP` or `JSON`
  /// column arrives: stored as `INT`, `LONG` or `STRING` but typed as itself.
  private static BlockValSet asLogicalType(BlockValSet delegate, DataType logicalType) {
    return new SyntheticBlockValSets.Base() {
      @Override
      public RoaringBitmap getNullBitmap() {
        return delegate.getNullBitmap();
      }

      @Override
      public DataType getValueType() {
        return logicalType;
      }

      @Override
      public boolean isSingleValue() {
        return delegate.isSingleValue();
      }

      @Override
      public Dictionary getDictionary() {
        return delegate.getDictionary();
      }

      @Override
      public int[] getIntValuesSV() {
        return delegate.getIntValuesSV();
      }

      @Override
      public long[] getLongValuesSV() {
        return delegate.getLongValuesSV();
      }

      @Override
      public float[] getFloatValuesSV() {
        return delegate.getFloatValuesSV();
      }

      @Override
      public double[] getDoubleValuesSV() {
        return delegate.getDoubleValuesSV();
      }

      @Override
      public String[] getStringValuesSV() {
        return delegate.getStringValuesSV();
      }
    };
  }

  private static BlockValSet ints() {
    return SyntheticBlockValSets.Int.create(null, new int[]{1, 0, 1, 0});
  }

  private static BlockValSet longs() {
    return SyntheticBlockValSets.Long.create(null, new long[]{10L, 20L, 30L, 40L});
  }

  private static BlockValSet strings() {
    return SyntheticBlockValSets.Str.create(null, new String[]{"{}", "{}", "{}", "{}"});
  }

  /// The three logical types whose stored representation differs from themselves, paired with a block that serves
  /// that representation.
  @DataProvider(name = "storedTypeAliases")
  public static Object[][] storedTypeAliases() {
    return new Object[][]{
        {DataType.BOOLEAN, (java.util.function.Supplier<BlockValSet>) StoredTypeDispatchTest::ints},
        {DataType.TIMESTAMP, (java.util.function.Supplier<BlockValSet>) StoredTypeDispatchTest::longs},
        {DataType.JSON, (java.util.function.Supplier<BlockValSet>) StoredTypeDispatchTest::strings}
    };
  }

  /// An extra field of one of these types is payload the funnel carries alongside a matched event, and the getter
  /// for its stored representation is already in the switch, so it must not be rejected.
  @Test(dataProvider = "storedTypeAliases")
  public void testFunnelExtraFieldAcceptsStoredTypeAliases(DataType logicalType,
      java.util.function.Supplier<BlockValSet> block) {
    ExpressionContext timestamp = ExpressionContext.forIdentifier("ts");
    ExpressionContext step0 = ExpressionContext.forIdentifier("step0");
    ExpressionContext step1 = ExpressionContext.forIdentifier("step1");
    ExpressionContext extra = ExpressionContext.forIdentifier("extra");
    List<ExpressionContext> arguments = new ArrayList<>(List.of(timestamp,
        ExpressionContext.forLiteral(Literal.longValue(1000)),
        ExpressionContext.forLiteral(Literal.intValue(2)), step0, step1,
        ExpressionContext.forLiteral(Literal.intValue(1)), extra));

    FunnelEventsFunctionEvalAggregationFunction function =
        new FunnelEventsFunctionEvalAggregationFunction(arguments);
    AggregationResultHolder holder = function.createAggregationResultHolder();

    Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
    blockValSetMap.put(timestamp, longs());
    blockValSetMap.put(step0, ints());
    blockValSetMap.put(step1, SyntheticBlockValSets.Int.create(null, new int[]{0, 1, 0, 1}));
    blockValSetMap.put(extra, asLogicalType(block.get(), logicalType));

    function.aggregate(NUM_DOCS, holder, blockValSetMap);
    assertNotNull(function.extractAggregationResult(holder), logicalType + " extra field must be readable");
  }

  /// The numeric logical types whose stored representation differs from themselves, plus `FLOAT`, which was
  /// missing from the time-series allow-list altogether even though the numeric path reads `getDoubleValuesSV`.
  ///
  /// `JSON` is not here. Its stored type is `STRING`, so the fix does route it to `aggregateStringValues`, but
  /// [org.apache.pinot.tsdb.spi.series.BaseTimeSeriesBuilder] leaves string input unimplemented and the `SUM`
  /// builder throws on it. That is a builder limitation downstream of the dispatch, not something this change
  /// reaches, so asserting on it here would be testing the wrong component.
  @DataProvider(name = "numericStoredTypeAliases")
  public static Object[][] numericStoredTypeAliases() {
    return new Object[][]{{DataType.BOOLEAN}, {DataType.TIMESTAMP}, {DataType.FLOAT}};
  }

  /// The time-series value column is read through `getDoubleValuesSV`, which works for every numeric stored type.
  @Test(dataProvider = "numericStoredTypeAliases")
  public void testTimeSeriesValueAcceptsNumericStoredTypes(DataType logicalType) {
    BlockValSet doubles = SyntheticBlockValSets.Double.create(null, new double[]{1.0, 2.0, 3.0, 4.0});
    assertNotNull(aggregateTimeSeries(asLogicalType(doubles, logicalType)),
        logicalType + " value column must be readable");
  }

  private Object aggregateTimeSeries(BlockValSet valueBlock) {
    ExpressionContext value = ExpressionContext.forIdentifier("value");
    ExpressionContext time = ExpressionContext.forIdentifier("time");
    List<ExpressionContext> arguments = List.of(
        ExpressionContext.forLiteral(Literal.stringValue(LANGUAGE)),
        ExpressionContext.forLiteral(Literal.stringValue("SUM")),
        value, time,
        ExpressionContext.forLiteral(Literal.stringValue("SECONDS")),
        ExpressionContext.forLiteral(Literal.longValue(0)),
        ExpressionContext.forLiteral(Literal.longValue(100)),
        ExpressionContext.forLiteral(Literal.longValue(10)),
        ExpressionContext.forLiteral(Literal.intValue(2)),
        ExpressionContext.forLiteral(Literal.stringValue("")));

    TimeSeriesAggregationFunction function = new TimeSeriesAggregationFunction(arguments);
    AggregationResultHolder holder = function.createAggregationResultHolder();
    Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
    blockValSetMap.put(time, SyntheticBlockValSets.Long.create(null, new long[]{100L, 100L, 110L, 110L}));
    blockValSetMap.put(value, valueBlock);
    function.aggregate(NUM_DOCS, holder, blockValSetMap);
    return function.extractAggregationResult(holder);
  }
}
