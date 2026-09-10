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

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.BaseTimeSeriesBuilder;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactory;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactoryProvider;


/// Aggregation function used by the Time Series Engine. This can't be used with SQL because the Object Serde is not yet
/// implemented. Though we don't plan on exposing this function anytime soon.
///
/// # Converting Time Values to Bucket Indexes
///
///   This aggregation function will map each scanned data point to a time bucket index. This is done using the
///   formula: `((timeValue + timeOffset) - timeReferencePoint - 1) / bucketSize`. The entire calculation is done
///   in the Time Unit (seconds, ms, etc.) of the timeValue returned by the time expression chosen by the user.
///   The method used to add values to the series builders is:
///   [BaseTimeSeriesBuilder#addValueAtIndex(int, Double, long)].
///
///   The formula originates from the fact that we use half-open time intervals, which are open on the left.
///   The timeReferencePoint is usually the start of the time-range being scanned. Assuming everything is in seconds,
///   the time buckets can generally thought to look something like the following:
///
/// ```
/// (timeReferencePoint, timeReferencePoint + bucketSize]
/// (timeReferencePoint + bucketSize, timeReferencePoint + 2 * bucketSize]
/// ...
/// (timeReferencePoint + (numBuckets - 1) * bucketSize, timeReferencePoint + numBuckets * bucketSize]
/// ```
///
///   Also, note that the timeReferencePoint is simply calculated as follows:
///
/// ```
/// timeReferencePointInSeconds = firstBucketValue - bucketSizeInSeconds
/// ```
///
/// Both the value and the timestamp gate a row when null handling is on. A null value has nothing to add to its
/// bucket, and a null timestamp leaves the point with no bucket to land in, so either one makes the row
/// uncountable rather than merely incomplete.
public class TimeSeriesAggregationFunction extends BaseAggregationFunction<BaseTimeSeriesBuilder, DoubleArrayList> {
  private final TimeSeriesBuilderFactory _factory;
  private final AggInfo _aggInfo;
  private final ExpressionContext _valueExpression;
  private final ExpressionContext _timeExpression;
  private final TimeBuckets _timeBuckets;
  private final long _timeReferencePoint;
  private final long _timeOffset;
  private final long _timeBucketDivisor;

  /// Arguments are as shown below:
  ///
  /// ```
  /// timeSeriesAggregate("m3ql", "MIN", valueExpr, timeExpr, timeUnit, offsetSeconds, firstBucketValue,
  ///     bucketLenSeconds, numBuckets, "aggParam1=value1")
  /// ```
  public TimeSeriesAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(nullHandlingEnabled);
    // Initialize temporary variables.
    Preconditions.checkArgument(arguments.size() == 10, "Expected 10 arguments for time-series agg");
    String language = arguments.get(0).getLiteral().getStringValue();
    String aggFunctionName = arguments.get(1).getLiteral().getStringValue();
    ExpressionContext valueExpression = arguments.get(2);
    ExpressionContext timeExpression = arguments.get(3);
    TimeUnit timeUnit = TimeUnit.valueOf(arguments.get(4).getLiteral().getStringValue().toUpperCase(Locale.ENGLISH));
    long offsetSeconds = arguments.get(5).getLiteral().getLongValue();
    long firstBucketValue = arguments.get(6).getLiteral().getLongValue();
    long bucketWindowSeconds = arguments.get(7).getLiteral().getLongValue();
    int numBuckets = arguments.get(8).getLiteral().getIntValue();
    Map<String, String> aggParams = AggInfo.deserializeParams(arguments.get(9).getLiteral().getStringValue());
    AggInfo aggInfo = new AggInfo(aggFunctionName, true /* is partial agg */, aggParams);
    // Set all values
    _factory = TimeSeriesBuilderFactoryProvider.getSeriesBuilderFactory(language);
    _valueExpression = valueExpression;
    _timeExpression = timeExpression;
    _timeBuckets = TimeBuckets.ofSeconds(firstBucketValue, Duration.ofSeconds(bucketWindowSeconds), numBuckets);
    _aggInfo = aggInfo;
    _timeReferencePoint = timeUnit.convert(Duration.ofSeconds(firstBucketValue - bucketWindowSeconds));
    _timeOffset = timeUnit.convert(Duration.ofSeconds(offsetSeconds));
    _timeBucketDivisor = timeUnit.convert(_timeBuckets.getBucketSize());
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.TIMESERIESAGGREGATE;
  }

  @Override
  public String getResultColumnName() {
    return AggregationFunctionType.TIMESERIESAGGREGATE.getName();
  }

  @Override
  public List<ExpressionContext> getInputExpressions() {
    return List.of(_valueExpression, _timeExpression);
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet timeBlockValSet = blockValSetMap.get(_timeExpression);
    BlockValSet valueBlockValSet = blockValSetMap.get(_valueExpression);
    switch (valueBlockValSet.getValueType().getStoredType()) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BIG_DECIMAL:
        aggregateNumericValues(length, timeBlockValSet, aggregationResultHolder, valueBlockValSet);
        break;
      case STRING:
        aggregateStringValues(length, timeBlockValSet, aggregationResultHolder, valueBlockValSet);
        break;
      default:
        throw new UnsupportedOperationException(String.format("Unsupported type: %s in aggregate",
            valueBlockValSet.getValueType()));
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet timeBlockValSet = blockValSetMap.get(_timeExpression);
    BlockValSet valueBlockValSet = blockValSetMap.get(_valueExpression);
    switch (valueBlockValSet.getValueType().getStoredType()) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BIG_DECIMAL:
        aggregateGroupByNumericValues(length, groupKeyArray, timeBlockValSet, groupByResultHolder, valueBlockValSet);
        break;
      case STRING:
        aggregateGroupByStringValues(length, groupKeyArray, timeBlockValSet, groupByResultHolder, valueBlockValSet);
        break;
      default:
        throw new UnsupportedOperationException(String.format("Unsupported type: %s in aggregate",
            valueBlockValSet.getValueType()));
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    throw new UnsupportedOperationException("MV not supported yet");
  }

  @Nullable
  @Override
  public BaseTimeSeriesBuilder extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Nullable
  @Override
  public BaseTimeSeriesBuilder extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public BaseTimeSeriesBuilder merge(BaseTimeSeriesBuilder ir1, BaseTimeSeriesBuilder ir2) {
    ir1.mergeAlignedSeriesBuilder(ir2);
    return ir1;
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Nullable
  @Override
  public DoubleArrayList extractFinalResult(@Nullable BaseTimeSeriesBuilder seriesBuilder) {
    // A null intermediate result means nothing was aggregated, and there is no series to build
    if (seriesBuilder == null) {
      return null;
    }
    Double[] doubleValues = seriesBuilder.build().getDoubleValues();
    return new DoubleArrayList(Arrays.asList(doubleValues));
  }

  @Override
  public String toExplainString() {
    return "TIME_SERIES";
  }

  /// Returns the holder's series builder, creating and storing one on first use.
  ///
  /// Called from inside a non-null range rather than before the loop, so that a block whose rows are all null leaves
  /// the holder untouched and [#extractFinalResult] sees the `null` that means nothing was aggregated.
  private BaseTimeSeriesBuilder getOrCreateSeriesBuilder(AggregationResultHolder resultHolder) {
    BaseTimeSeriesBuilder seriesBuilder = resultHolder.getResult();
    if (seriesBuilder == null) {
      seriesBuilder = newSeriesBuilder();
      resultHolder.setValue(seriesBuilder);
    }
    return seriesBuilder;
  }

  private BaseTimeSeriesBuilder newSeriesBuilder() {
    return _factory.newTimeSeriesBuilder(_aggInfo, "TO_BE_REMOVED", _timeBuckets,
        BaseTimeSeriesBuilder.UNINITIALISED_TAG_NAMES, BaseTimeSeriesBuilder.UNINITIALISED_TAG_VALUES);
  }

  private int timeIndex(long timeValue) {
    return (int) (((timeValue + _timeOffset) - _timeReferencePoint - 1) / _timeBucketDivisor);
  }

  private void aggregateNumericValues(int length, BlockValSet timeBlockValSet, AggregationResultHolder resultHolder,
      BlockValSet blockValSet) {
    long[] timeValues = timeBlockValSet.getLongValuesSV();
    double[] values = blockValSet.getDoubleValuesSV();
    forEachNotNull(length, blockValSet, timeBlockValSet, (from, to) -> {
      BaseTimeSeriesBuilder currentSeriesBuilder = getOrCreateSeriesBuilder(resultHolder);
      for (int docIndex = from; docIndex < to; docIndex++) {
        currentSeriesBuilder.addValueAtIndex(timeIndex(timeValues[docIndex]), values[docIndex], timeValues[docIndex]);
      }
    });
  }

  private void aggregateStringValues(int length, BlockValSet timeBlockValSet, AggregationResultHolder resultHolder,
      BlockValSet blockValSet) {
    long[] timeValues = timeBlockValSet.getLongValuesSV();
    String[] values = blockValSet.getStringValuesSV();
    forEachNotNull(length, blockValSet, timeBlockValSet, (from, to) -> {
      BaseTimeSeriesBuilder currentSeriesBuilder = getOrCreateSeriesBuilder(resultHolder);
      for (int docIndex = from; docIndex < to; docIndex++) {
        currentSeriesBuilder.addValueAtIndex(timeIndex(timeValues[docIndex]), values[docIndex], timeValues[docIndex]);
      }
    });
  }

  private void aggregateGroupByNumericValues(int length, int[] groupKeyArray, BlockValSet timeBlockValSet,
      GroupByResultHolder resultHolder, BlockValSet blockValSet) {
    long[] timeValues = timeBlockValSet.getLongValuesSV();
    final double[] values = blockValSet.getDoubleValuesSV();
    forEachNotNull(length, blockValSet, timeBlockValSet, (from, to) -> {
      for (int docIndex = from; docIndex < to; docIndex++) {
        int groupId = groupKeyArray[docIndex];
        BaseTimeSeriesBuilder currentSeriesBuilder = resultHolder.getResult(groupId);
        if (currentSeriesBuilder == null) {
          currentSeriesBuilder = newSeriesBuilder();
          resultHolder.setValueForKey(groupId, currentSeriesBuilder);
        }
        currentSeriesBuilder.addValueAtIndex(timeIndex(timeValues[docIndex]), values[docIndex], timeValues[docIndex]);
      }
    });
  }

  private void aggregateGroupByStringValues(int length, int[] groupKeyArray, BlockValSet timeBlockValSet,
      GroupByResultHolder resultHolder, BlockValSet blockValSet) {
    long[] timeValues = timeBlockValSet.getLongValuesSV();
    final String[] values = blockValSet.getStringValuesSV();
    forEachNotNull(length, blockValSet, timeBlockValSet, (from, to) -> {
      for (int docIndex = from; docIndex < to; docIndex++) {
        int groupId = groupKeyArray[docIndex];
        BaseTimeSeriesBuilder currentSeriesBuilder = resultHolder.getResult(groupId);
        if (currentSeriesBuilder == null) {
          currentSeriesBuilder = newSeriesBuilder();
          resultHolder.setValueForKey(groupId, currentSeriesBuilder);
        }
        currentSeriesBuilder.addValueAtIndex(timeIndex(timeValues[docIndex]), values[docIndex], timeValues[docIndex]);
      }
    });
  }

  public static ExpressionContext create(String language, String valueExpressionStr, ExpressionContext timeExpression,
      TimeUnit timeUnit, long offsetSeconds, TimeBuckets timeBuckets, AggInfo aggInfo) {
    ExpressionContext valueExpression = RequestContextUtils.getExpression(valueExpressionStr);
    List<ExpressionContext> arguments = new ArrayList<>();
    arguments.add(ExpressionContext.forLiteral(Literal.stringValue(language)));
    arguments.add(ExpressionContext.forLiteral(Literal.stringValue(aggInfo.getAggFunction())));
    arguments.add(valueExpression);
    arguments.add(timeExpression);
    arguments.add(ExpressionContext.forLiteral(Literal.stringValue(timeUnit.toString())));
    arguments.add(ExpressionContext.forLiteral(Literal.longValue(offsetSeconds)));
    arguments.add(ExpressionContext.forLiteral(Literal.longValue(timeBuckets.getTimeBuckets()[0])));
    arguments.add(ExpressionContext.forLiteral(Literal.longValue(timeBuckets.getBucketSize().getSeconds())));
    arguments.add(ExpressionContext.forLiteral(Literal.intValue(timeBuckets.getNumBuckets())));
    arguments.add(ExpressionContext.forLiteral(Literal.stringValue(AggInfo.serializeParams(aggInfo.getParams()))));
    FunctionContext functionContext = new FunctionContext(FunctionContext.Type.AGGREGATION,
        AggregationFunctionType.TIMESERIESAGGREGATE.getName(), arguments);
    return ExpressionContext.forFunction(functionContext);
  }
}
