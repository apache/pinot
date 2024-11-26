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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class TimeSeriesAggregationFunction implements AggregationFunction<Double[], DoubleArrayList> {
  private final AggregationFunctionType _aggregationFunctionType;
  private final ExpressionContext _valueExpression;
  private final ExpressionContext _timeExpression;
  private final int _numTimeBuckets;

  public TimeSeriesAggregationFunction(AggregationFunctionType aggregationFunctionType,
      ExpressionContext valueExpression, ExpressionContext timeExpression, ExpressionContext numTimeBuckets) {
    _aggregationFunctionType = aggregationFunctionType;
    _valueExpression = valueExpression;
    _timeExpression = timeExpression;
    _numTimeBuckets = numTimeBuckets.getLiteral().getIntValue();
  }

  @Override
  public AggregationFunctionType getType() {
    return _aggregationFunctionType;
  }

  @Override
  public String getResultColumnName() {
    return _aggregationFunctionType.getName();
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
    int[] timeIndexes = blockValSetMap.get(_timeExpression).getIntValuesSV();
    double[] values = blockValSetMap.get(_valueExpression).getDoubleValuesSV();
    Double[] currentValues = aggregationResultHolder.getResult();
    if (currentValues == null) {
      currentValues = new Double[_numTimeBuckets];
      aggregationResultHolder.setValue(currentValues);
    }
    switch (_aggregationFunctionType) {
      case TIMESERIESMAX:
        aggregateTimeSeriesMax(length, aggregationResultHolder, timeIndexes, values);
        break;
      case TIMESERIESMIN:
        aggregateTimeSeriesMin(length, aggregationResultHolder, timeIndexes, values);
        break;
      case TIMESERIESSUM:
        aggregateTimeSeriesSum(length, aggregationResultHolder, timeIndexes, values);
        break;
      default:
        throw new UnsupportedOperationException("Unknown aggregation function: " + _aggregationFunctionType);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    final int[] timeIndexes = blockValSetMap.get(_timeExpression).getIntValuesSV();
    final double[] values = blockValSetMap.get(_valueExpression).getDoubleValuesSV();
    switch (_aggregationFunctionType) {
      case TIMESERIESMAX:
        aggregateGroupBySVTimeSeriesMax(length, groupKeyArray, groupByResultHolder, timeIndexes, values);
        break;
      case TIMESERIESMIN:
        aggregateGroupBySVTimeSeriesMin(length, groupKeyArray, groupByResultHolder, timeIndexes, values);
        break;
      case TIMESERIESSUM:
        aggregateGroupBySVTimeSeriesSum(length, groupKeyArray, groupByResultHolder, timeIndexes, values);
        break;
      default:
        throw new UnsupportedOperationException("Unknown aggregation function: " + _aggregationFunctionType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    throw new UnsupportedOperationException("");
  }

  @Override
  public Double[] extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Override
  public Double[] extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public Double[] merge(Double[] intermediateResult1, Double[] intermediateResult2) {
    switch (_aggregationFunctionType) {
      case TIMESERIESMAX:
        mergeMax(intermediateResult1, intermediateResult2);
        break;
      case TIMESERIESMIN:
        mergeMin(intermediateResult1, intermediateResult2);
        break;
      case TIMESERIESSUM:
        mergeSum(intermediateResult1, intermediateResult2);
        break;
      default:
        throw new UnsupportedOperationException("Found unsupported time series aggregation: "
            + _aggregationFunctionType);
    }
    return intermediateResult1;
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.DOUBLE_ARRAY;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    throw new UnsupportedOperationException("getFinalResult not implemented yet");
  }

  @Override
  public DoubleArrayList extractFinalResult(Double[] seriesBuilder) {
    // TODO: Why does final result type required
    throw new UnsupportedOperationException("Extract final result not supported");
  }

  @Override
  public String toExplainString() {
    return "TIME_SERIES";
  }

  private void aggregateTimeSeriesMax(int length, AggregationResultHolder resultHolder, int[] timeIndexes,
      double[] values) {
    Double[] currentValues = resultHolder.getResult();
    for (int docIndex = 0; docIndex < length; docIndex++) {
      int timeIndex = timeIndexes[docIndex];
      currentValues[timeIndex] =
          currentValues[timeIndex] == null ? values[docIndex] : Math.max(currentValues[timeIndex], values[docIndex]);
    }
  }

  private void aggregateTimeSeriesMin(int length, AggregationResultHolder resultHolder, int[] timeIndexes,
      double[] values) {
    Double[] currentValues = resultHolder.getResult();
    for (int docIndex = 0; docIndex < length; docIndex++) {
      int timeIndex = timeIndexes[docIndex];
      currentValues[timeIndex] =
          currentValues[timeIndex] == null ? values[docIndex] : Math.min(currentValues[timeIndex], values[docIndex]);
    }
  }

  private void aggregateTimeSeriesSum(int length, AggregationResultHolder resultHolder, int[] timeIndexes,
      double[] values) {
    Double[] currentValues = resultHolder.getResult();
    for (int docIndex = 0; docIndex < length; docIndex++) {
      int timeIndex = timeIndexes[docIndex];
      currentValues[timeIndex] =
          currentValues[timeIndex] == null ? values[docIndex] : (currentValues[timeIndex] + values[docIndex]);
    }
  }

  private void aggregateGroupBySVTimeSeriesMax(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      int[] timeIndexes, double[] values) {
    for (int docIndex = 0; docIndex < length; docIndex++) {
      int groupId = groupKeyArray[docIndex];
      int timeIndex = timeIndexes[docIndex];
      double valueToAdd = values[docIndex];
      Double[] currentValues = groupByResultHolder.getResult(groupId);
      if (currentValues == null) {
        currentValues = new Double[_numTimeBuckets];
        groupByResultHolder.setValueForKey(groupId, currentValues);
      }
      currentValues[timeIndex] =
          currentValues[timeIndex] == null ? valueToAdd : Math.max(currentValues[timeIndex], valueToAdd);
    }
  }

  private void aggregateGroupBySVTimeSeriesMin(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      int[] timeIndexes, double[] values) {
    for (int docIndex = 0; docIndex < length; docIndex++) {
      int groupId = groupKeyArray[docIndex];
      int timeIndex = timeIndexes[docIndex];
      double valueToAdd = values[docIndex];
      Double[] currentValues = groupByResultHolder.getResult(groupId);
      if (currentValues == null) {
        currentValues = new Double[_numTimeBuckets];
        groupByResultHolder.setValueForKey(groupId, currentValues);
      }
      currentValues[timeIndex] =
          currentValues[timeIndex] == null ? valueToAdd : Math.min(currentValues[timeIndex], valueToAdd);
    }
  }

  private void aggregateGroupBySVTimeSeriesSum(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      int[] timeIndexes, double[] values) {
    for (int docIndex = 0; docIndex < length; docIndex++) {
      int groupId = groupKeyArray[docIndex];
      int timeIndex = timeIndexes[docIndex];
      double valueToAdd = values[docIndex];
      Double[] currentValues = groupByResultHolder.getResult(groupId);
      if (currentValues == null) {
        currentValues = new Double[_numTimeBuckets];
        groupByResultHolder.setValueForKey(groupId, currentValues);
      }
      currentValues[timeIndex] =
          currentValues[timeIndex] == null ? valueToAdd : (currentValues[timeIndex] + valueToAdd);
    }
  }

  private void mergeSum(Double[] ir1, Double[] ir2) {
    for (int index = 0; index < _numTimeBuckets; index++) {
      if (ir2[index] != null) {
        ir1[index] = ir1[index] == null ? ir2[index] : (ir1[index] + ir2[index]);
      }
    }
  }

  private void mergeMin(Double[] ir1, Double[] ir2) {
    for (int index = 0; index < _numTimeBuckets; index++) {
      if (ir2[index] != null) {
        ir1[index] = ir1[index] == null ? ir2[index] : Math.min(ir1[index], ir2[index]);
      }
    }
  }

  private void mergeMax(Double[] ir1, Double[] ir2) {
    for (int index = 0; index < _numTimeBuckets; index++) {
      if (ir2[index] != null) {
        ir1[index] = ir1[index] == null ? ir2[index] : Math.max(ir1[index], ir2[index]);
      }
    }
  }
}
