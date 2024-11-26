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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
import org.apache.pinot.segment.spi.SegmentMetadata;
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
  private final long _timeOffset;
  private final AggInfo _aggInfo;
  private final ExpressionContext _valueExpression;
  private final List<String> _groupByExpressions;
  private final BaseProjectOperator<? extends ValueBlock> _projectOperator;
  private final TimeBuckets _timeBuckets;
  private final TimeSeriesBuilderFactory _seriesBuilderFactory;
  private final int _maxSeriesLimit;
  private final long _maxDataPointsLimit;
  private final long _numTotalDocs;
  private long _numDocsScanned = 0;

  public TimeSeriesAggregationOperator(
      String timeColumn,
      TimeUnit timeUnit,
      @Nullable Long timeOffsetSeconds,
      AggInfo aggInfo,
      ExpressionContext valueExpression,
      List<String> groupByExpressions,
      TimeBuckets timeBuckets,
      BaseProjectOperator<? extends ValueBlock> projectOperator,
      TimeSeriesBuilderFactory seriesBuilderFactory,
      SegmentMetadata segmentMetadata) {
    _timeColumn = timeColumn;
    _storedTimeUnit = timeUnit;
    _timeOffset = timeOffsetSeconds == null ? 0L : timeUnit.convert(Duration.ofSeconds(timeOffsetSeconds));
    _aggInfo = aggInfo;
    _valueExpression = valueExpression;
    _groupByExpressions = groupByExpressions;
    _projectOperator = projectOperator;
    _timeBuckets = timeBuckets;
    _seriesBuilderFactory = seriesBuilderFactory;
    _maxSeriesLimit = _seriesBuilderFactory.getMaxUniqueSeriesPerServerLimit();
    _maxDataPointsLimit = _seriesBuilderFactory.getMaxDataPointsPerServerLimit();
    _numTotalDocs = segmentMetadata.getTotalDocs();
  }

  @Override
  protected TimeSeriesResultsBlock getNextBlock() {
    ValueBlock valueBlock;
    Map<Long, BaseTimeSeriesBuilder> seriesBuilderMap = new HashMap<>(1024);
    while ((valueBlock = _projectOperator.nextBlock()) != null) {
      int numDocs = valueBlock.getNumDocs();
      _numDocsScanned += numDocs;
      // TODO: This is quite unoptimized and allocates liberally
      BlockValSet blockValSet = valueBlock.getBlockValueSet(_timeColumn);
      long[] timeValues = blockValSet.getLongValuesSV();
      applyTimeOffset(timeValues, numDocs);
      int[] timeValueIndexes = getTimeValueIndex(timeValues, numDocs);
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
          processLongExpression(valueExpressionBlockValSet, seriesBuilderMap, timeValueIndexes, tagValues, numDocs);
          break;
        case INT:
          processIntExpression(valueExpressionBlockValSet, seriesBuilderMap, timeValueIndexes, tagValues, numDocs);
          break;
        case DOUBLE:
          processDoubleExpression(valueExpressionBlockValSet, seriesBuilderMap, timeValueIndexes, tagValues, numDocs);
          break;
        case STRING:
          processStringExpression(valueExpressionBlockValSet, seriesBuilderMap, timeValueIndexes, tagValues, numDocs);
          break;
        default:
          // TODO: Support other types?
          throw new IllegalStateException(
              "Don't yet support value expression of type: " + valueExpressionBlockValSet.getValueType());
      }
      Preconditions.checkState(seriesBuilderMap.size() * (long) _timeBuckets.getNumBuckets() <= _maxDataPointsLimit,
          "Exceeded max data point limit per server. Limit: %s. Data points in current segment so far: %s",
          _maxDataPointsLimit, seriesBuilderMap.size() * _timeBuckets.getNumBuckets());
      Preconditions.checkState(seriesBuilderMap.size() <= _maxSeriesLimit,
          "Exceeded max unique series limit per server. Limit: %s. Series in current segment so far: %s",
          _maxSeriesLimit, seriesBuilderMap.size());
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
    long numEntriesScannedInFilter = _projectOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = _numDocsScanned * _projectOperator.getNumColumnsProjected();
    return new ExecutionStatistics(_numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
        _numTotalDocs);
  }

  @VisibleForTesting
  protected int[] getTimeValueIndex(long[] actualTimeValues, int numDocs) {
    if (_storedTimeUnit == TimeUnit.MILLISECONDS) {
      return getTimeValueIndexMillis(actualTimeValues, numDocs);
    }
    int[] timeIndexes = new int[numDocs];
    final long reference = _timeBuckets.getTimeRangeStartExclusive();
    final long divisor = _timeBuckets.getBucketSize().getSeconds();
    for (int index = 0; index < numDocs; index++) {
      timeIndexes[index] = (int) ((actualTimeValues[index] - reference - 1) / divisor);
    }
    return timeIndexes;
  }

  private int[] getTimeValueIndexMillis(long[] actualTimeValues, int numDocs) {
    int[] timeIndexes = new int[numDocs];
    final long reference = _timeBuckets.getTimeRangeStartExclusive() * 1000L;
    final long divisor = _timeBuckets.getBucketSize().toMillis();
    for (int index = 0; index < numDocs; index++) {
      timeIndexes[index] = (int) ((actualTimeValues[index] - reference - 1) / divisor);
    }
    return timeIndexes;
  }

  public void processLongExpression(BlockValSet blockValSet, Map<Long, BaseTimeSeriesBuilder> seriesBuilderMap,
      int[] timeValueIndexes, Object[][] tagValues, int numDocs) {
    long[] valueColumnValues = blockValSet.getLongValuesSV();
    for (int docIdIndex = 0; docIdIndex < numDocs; docIdIndex++) {
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
      int[] timeValueIndexes, Object[][] tagValues, int numDocs) {
    int[] valueColumnValues = blockValSet.getIntValuesSV();
    for (int docIdIndex = 0; docIdIndex < numDocs; docIdIndex++) {
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
      int[] timeValueIndexes, Object[][] tagValues, int numDocs) {
    double[] valueColumnValues = blockValSet.getDoubleValuesSV();
    for (int docIdIndex = 0; docIdIndex < numDocs; docIdIndex++) {
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
      int[] timeValueIndexes, Object[][] tagValues, int numDocs) {
    String[] valueColumnValues = blockValSet.getStringValuesSV();
    for (int docIdIndex = 0; docIdIndex < numDocs; docIdIndex++) {
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

  private void applyTimeOffset(long[] timeValues, int numDocs) {
    if (_timeOffset == 0L) {
      return;
    }
    for (int index = 0; index < numDocs; index++) {
      timeValues[index] = timeValues[index] + _timeOffset;
    }
  }
}
