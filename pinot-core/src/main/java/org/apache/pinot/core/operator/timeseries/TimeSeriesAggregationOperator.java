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

import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.TimeSeriesBuilderBlock;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.blocks.results.TimeSeriesResultsBlock;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.BaseTimeSeriesBuilder;
import org.apache.pinot.tsdb.spi.series.TimeSeries;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactory;


/**
 * Segment level operator which converts data in relational model to a time-series model, by aggregating on a
 * configured aggregation expression.
 */
public class TimeSeriesAggregationOperator extends BaseOperator<TimeSeriesResultsBlock> {
  private static final String EXPLAIN_NAME = "TIME_SERIES_AGGREGATION";
  private final String _timeColumn;
  private final TimeUnit _storedTimeUnit;
  private final Long _timeOffset;
  private final AggInfo _aggInfo;
  private final ExpressionContext _valueExpression;
  private final List<String> _groupByExpressions;
  private final BaseProjectOperator<? extends ValueBlock> _projectOperator;
  private final TimeBuckets _timeBuckets;
  private final TimeSeriesBuilderFactory _seriesBuilderFactory;

  public TimeSeriesAggregationOperator(
      String timeColumn,
      TimeUnit timeUnit,
      Long timeOffsetSeconds,
      AggInfo aggInfo,
      ExpressionContext valueExpression,
      List<String> groupByExpressions,
      TimeBuckets timeBuckets,
      BaseProjectOperator<? extends ValueBlock> projectOperator,
      TimeSeriesBuilderFactory seriesBuilderFactory) {
    _timeColumn = timeColumn;
    _storedTimeUnit = timeUnit;
    _timeOffset = timeUnit.convert(Duration.ofSeconds(timeOffsetSeconds));
    _aggInfo = aggInfo;
    _valueExpression = valueExpression;
    _groupByExpressions = groupByExpressions;
    _projectOperator = projectOperator;
    _timeBuckets = timeBuckets;
    _seriesBuilderFactory = seriesBuilderFactory;
  }

  @Override
  protected TimeSeriesResultsBlock getNextBlock() {
    ValueBlock valueBlock;
    Map<Long, BaseTimeSeriesBuilder> seriesBuilderMap = new HashMap<>(1024);
    while ((valueBlock = _projectOperator.nextBlock()) != null) {
      // TODO: This is quite unoptimized and allocates liberally
      BlockValSet blockValSet = valueBlock.getBlockValueSet(_timeColumn);
      long[] timeValues = blockValSet.getLongValuesSV();
      if (_timeOffset != null && _timeOffset != 0L) {
        timeValues = applyTimeshift(_timeOffset, timeValues);
      }
      int[] timeValueIndexes = getTimeValueIndex(timeValues, _storedTimeUnit);
      Object[][] tagValues = new Object[_groupByExpressions.size()][];
      for (int i = 0; i < _groupByExpressions.size(); i++) {
        blockValSet = valueBlock.getBlockValueSet(_groupByExpressions.get(i));
        switch (blockValSet.getValueType()) {
          case JSON:
          case STRING:
            tagValues[i] = blockValSet.getStringValuesSV();
            break;
          case LONG:
            tagValues[i] = ArrayUtils.toObject(blockValSet.getLongValuesSV());
            break;
          case INT:
            tagValues[i] = ArrayUtils.toObject(blockValSet.getIntValuesSV());
            break;
          default:
            throw new NotImplementedException("Can't handle types other than string and long");
        }
      }
      BlockValSet valueExpressionBlockValSet = valueBlock.getBlockValueSet(_valueExpression);
      switch (valueExpressionBlockValSet.getValueType()) {
        case LONG:
          processLongExpression(valueExpressionBlockValSet, seriesBuilderMap, timeValueIndexes, tagValues);
          break;
        case INT:
          processIntExpression(valueExpressionBlockValSet, seriesBuilderMap, timeValueIndexes, tagValues);
          break;
        case DOUBLE:
          processDoubleExpression(valueExpressionBlockValSet, seriesBuilderMap, timeValueIndexes, tagValues);
          break;
        case STRING:
          processStringExpression(valueExpressionBlockValSet, seriesBuilderMap, timeValueIndexes, tagValues);
          break;
        default:
          // TODO: Support other types?
          throw new IllegalStateException(
              "Don't yet support value expression of type: " + valueExpressionBlockValSet.getValueType());
      }
    }
    return new TimeSeriesResultsBlock(new TimeSeriesBuilderBlock(_timeBuckets, seriesBuilderMap));
  }

  @Override
  @SuppressWarnings("rawtypes")
  public List<? extends Operator> getChildOperators() {
    return ImmutableList.of(_projectOperator);
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    // TODO: Implement this.
    return new ExecutionStatistics(0, 0, 0, 0);
  }

  private int[] getTimeValueIndex(long[] actualTimeValues, TimeUnit timeUnit) {
    if (timeUnit == TimeUnit.MILLISECONDS) {
      return getTimeValueIndexMillis(actualTimeValues);
    }
    int[] timeIndexes = new int[actualTimeValues.length];
    for (int index = 0; index < actualTimeValues.length; index++) {
      timeIndexes[index] = (int) ((actualTimeValues[index] - _timeBuckets.getStartTime())
          / _timeBuckets.getBucketSize().getSeconds());
    }
    return timeIndexes;
  }

  private int[] getTimeValueIndexMillis(long[] actualTimeValues) {
    int[] timeIndexes = new int[actualTimeValues.length];
    for (int index = 0; index < actualTimeValues.length; index++) {
      timeIndexes[index] = (int) ((actualTimeValues[index] - _timeBuckets.getStartTime() * 1000L)
          / _timeBuckets.getBucketSize().toMillis());
    }
    return timeIndexes;
  }

  public void processLongExpression(BlockValSet blockValSet, Map<Long, BaseTimeSeriesBuilder> seriesBuilderMap,
      int[] timeValueIndexes, Object[][] tagValues) {
    long[] valueColumnValues = blockValSet.getLongValuesSV();
    for (int docIdIndex = 0; docIdIndex < timeValueIndexes.length; docIdIndex++) {
      Object[] tagValuesForDoc = new Object[_groupByExpressions.size()];
      for (int tagIndex = 0; tagIndex < tagValues.length; tagIndex++) {
        tagValuesForDoc[tagIndex] = tagValues[tagIndex][docIdIndex];
      }
      long hash = TimeSeries.hash(tagValuesForDoc);
      seriesBuilderMap.computeIfAbsent(hash,
              k -> _seriesBuilderFactory.newTimeSeriesBuilder(_aggInfo, Long.toString(hash), _timeBuckets,
                  _groupByExpressions,
                  tagValuesForDoc))
          .addValueAtIndex(timeValueIndexes[docIdIndex], (double) valueColumnValues[docIdIndex]);
    }
  }

  public void processIntExpression(BlockValSet blockValSet, Map<Long, BaseTimeSeriesBuilder> seriesBuilderMap,
      int[] timeValueIndexes, Object[][] tagValues) {
    int[] valueColumnValues = blockValSet.getIntValuesSV();
    for (int docIdIndex = 0; docIdIndex < timeValueIndexes.length; docIdIndex++) {
      Object[] tagValuesForDoc = new Object[_groupByExpressions.size()];
      for (int tagIndex = 0; tagIndex < tagValues.length; tagIndex++) {
        tagValuesForDoc[tagIndex] = tagValues[tagIndex][docIdIndex];
      }
      long hash = TimeSeries.hash(tagValuesForDoc);
      seriesBuilderMap.computeIfAbsent(hash,
              k -> _seriesBuilderFactory.newTimeSeriesBuilder(_aggInfo, Long.toString(hash), _timeBuckets,
                  _groupByExpressions,
                  tagValuesForDoc))
          .addValueAtIndex(timeValueIndexes[docIdIndex], (double) valueColumnValues[docIdIndex]);
    }
  }

  public void processDoubleExpression(BlockValSet blockValSet, Map<Long, BaseTimeSeriesBuilder> seriesBuilderMap,
      int[] timeValueIndexes, Object[][] tagValues) {
    double[] valueColumnValues = blockValSet.getDoubleValuesSV();
    for (int docIdIndex = 0; docIdIndex < timeValueIndexes.length; docIdIndex++) {
      Object[] tagValuesForDoc = new Object[_groupByExpressions.size()];
      for (int tagIndex = 0; tagIndex < tagValues.length; tagIndex++) {
        tagValuesForDoc[tagIndex] = tagValues[tagIndex][docIdIndex];
      }
      long hash = TimeSeries.hash(tagValuesForDoc);
      seriesBuilderMap.computeIfAbsent(hash,
              k -> _seriesBuilderFactory.newTimeSeriesBuilder(_aggInfo, Long.toString(hash), _timeBuckets,
                  _groupByExpressions,
                  tagValuesForDoc))
          .addValueAtIndex(timeValueIndexes[docIdIndex], valueColumnValues[docIdIndex]);
    }
  }

  public void processStringExpression(BlockValSet blockValSet, Map<Long, BaseTimeSeriesBuilder> seriesBuilderMap,
      int[] timeValueIndexes, Object[][] tagValues) {
    String[] valueColumnValues = blockValSet.getStringValuesSV();
    for (int docIdIndex = 0; docIdIndex < timeValueIndexes.length; docIdIndex++) {
      Object[] tagValuesForDoc = new Object[_groupByExpressions.size()];
      for (int tagIndex = 0; tagIndex < tagValues.length; tagIndex++) {
        tagValuesForDoc[tagIndex] = tagValues[tagIndex][docIdIndex];
      }
      long hash = TimeSeries.hash(tagValuesForDoc);
      seriesBuilderMap.computeIfAbsent(hash,
              k -> _seriesBuilderFactory.newTimeSeriesBuilder(_aggInfo, Long.toString(hash), _timeBuckets,
                  _groupByExpressions, tagValuesForDoc))
          .addValueAtIndex(timeValueIndexes[docIdIndex], valueColumnValues[docIdIndex]);
    }
  }

  public static long[] applyTimeshift(long timeshift, long[] timeValues) {
    if (timeshift == 0) {
      return timeValues;
    }
    long[] shiftedTimeValues = new long[timeValues.length];
    for (int index = 0; index < timeValues.length; index++) {
      shiftedTimeValues[index] = timeValues[index] + timeshift;
    }
    return shiftedTimeValues;
  }
}
