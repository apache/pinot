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

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.DoubleAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.DoubleGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.roaringbitmap.RoaringBitmap;


public class MinAggregationFunction extends BaseSingleInputAggregationFunction<Double, Double> {
  private static final double DEFAULT_VALUE = Double.POSITIVE_INFINITY;
  private final boolean _nullHandlingEnabled;

  public MinAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    this(verifySingleArgument(arguments, "MIN"), nullHandlingEnabled);
  }

  protected MinAggregationFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression);
    _nullHandlingEnabled = nullHandlingEnabled;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.MIN;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    if (_nullHandlingEnabled) {
      return new ObjectAggregationResultHolder();
    }
    return new DoubleAggregationResultHolder(DEFAULT_VALUE);
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    if (_nullHandlingEnabled) {
      return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
    }
    return new DoubleGroupByResultHolder(initialCapacity, maxCapacity, DEFAULT_VALUE);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (_nullHandlingEnabled) {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      if (nullBitmap == null) {
        nullBitmap = new RoaringBitmap();
      }
      aggregateNullHandlingEnabled(length, aggregationResultHolder, blockValSet, nullBitmap);
      return;
    }

    switch (blockValSet.getValueType().getStoredType()) {
      case INT: {
        int[] values = blockValSet.getIntValuesSV();
        int min = values[0];
        for (int i = 0; i < length & i < values.length; i++) {
          min = Math.min(values[i], min);
        }
        aggregationResultHolder.setValue(Math.min(min, aggregationResultHolder.getDoubleResult()));
        break;
      }
      case LONG: {
        long[] values = blockValSet.getLongValuesSV();
        long min = values[0];
        for (int i = 0; i < length & i < values.length; i++) {
          min = Math.min(values[i], min);
        }
        aggregationResultHolder.setValue(Math.min(min, aggregationResultHolder.getDoubleResult()));
        break;
      }
      case FLOAT: {
        float[] values = blockValSet.getFloatValuesSV();
        float min = values[0];
        for (int i = 0; i < length & i < values.length; i++) {
          min = Math.min(values[i], min);
        }
        aggregationResultHolder.setValue(Math.min(min, aggregationResultHolder.getDoubleResult()));
        break;
      }
      case DOUBLE: {
        double[] values = blockValSet.getDoubleValuesSV();
        double min = values[0];
        for (int i = 0; i < length & i < values.length; i++) {
          min = Math.min(values[i], min);
        }
        aggregationResultHolder.setValue(Math.min(min, aggregationResultHolder.getDoubleResult()));
        break;
      }
      case BIG_DECIMAL: {
        BigDecimal[] values = blockValSet.getBigDecimalValuesSV();
        BigDecimal min = values[0];
        for (int i = 0; i < length & i < values.length; i++) {
          min = values[i].min(min);
        }
        // TODO: even though the source data has BIG_DECIMAL type, we still only support double precision.
        aggregationResultHolder.setValue(Math.min(min.doubleValue(), aggregationResultHolder.getDoubleResult()));
        break;
      }
      default:
        throw new IllegalStateException("Cannot compute min for non-numeric type: " + blockValSet.getValueType());
    }
  }

  private void aggregateNullHandlingEnabled(int length, AggregationResultHolder aggregationResultHolder,
      BlockValSet blockValSet, RoaringBitmap nullBitmap) {
    switch (blockValSet.getValueType().getStoredType()) {
      case INT: {
        if (nullBitmap.getCardinality() < length) {
          int[] values = blockValSet.getIntValuesSV();
          int min = Integer.MAX_VALUE;
          for (int i = 0; i < length & i < values.length; i++) {
            if (!nullBitmap.contains(i)) {
              min = Math.min(values[i], min);
            }
          }
          updateAggregationResultHolder(aggregationResultHolder, min);
        }
        // Note: when all input values re null (nullBitmap.getCardinality() == values.length), min is null. As a result,
        // we don't update the value of aggregationResultHolder.
        break;
      }
      case LONG: {
        if (nullBitmap.getCardinality() < length) {
          long[] values = blockValSet.getLongValuesSV();
          long min = Long.MAX_VALUE;
          for (int i = 0; i < length & i < values.length; i++) {
            if (!nullBitmap.contains(i)) {
              min = Math.min(values[i], min);
            }
          }
          updateAggregationResultHolder(aggregationResultHolder, min);
        }
        break;
      }
      case FLOAT: {
        if (nullBitmap.getCardinality() < length) {
          float[] values = blockValSet.getFloatValuesSV();
          float min = Float.POSITIVE_INFINITY;
          for (int i = 0; i < length & i < values.length; i++) {
            if (!nullBitmap.contains(i)) {
              min = Math.min(values[i], min);
            }
          }
          updateAggregationResultHolder(aggregationResultHolder, min);
        }
        break;
      }
      case DOUBLE: {
        if (nullBitmap.getCardinality() < length) {
          double[] values = blockValSet.getDoubleValuesSV();
          double min = Double.POSITIVE_INFINITY;
          for (int i = 0; i < length & i < values.length; i++) {
            if (!nullBitmap.contains(i)) {
              min = Math.min(values[i], min);
            }
          }
          updateAggregationResultHolder(aggregationResultHolder, min);
        }
        break;
      }
      case BIG_DECIMAL: {
        if (nullBitmap.getCardinality() < length) {
          BigDecimal[] values = blockValSet.getBigDecimalValuesSV();
          BigDecimal min = null;
          for (int i = 0; i < length & i < values.length; i++) {
            if (!nullBitmap.contains(i)) {
              min = min == null ? values[i] : values[i].min(min);
            }
          }
          assert min != null;
          // TODO: even though the source data has BIG_DECIMAL type, we still only support double precision.
          updateAggregationResultHolder(aggregationResultHolder, min.doubleValue());
        }
        break;
      }
      default:
        throw new IllegalStateException("Cannot compute min for non-numeric type: " + blockValSet.getValueType());
    }
  }

  private void updateAggregationResultHolder(AggregationResultHolder aggregationResultHolder, double min) {
    Double otherMin = aggregationResultHolder.getResult();
    aggregationResultHolder.setValue(otherMin == null ? min : Math.min(min, otherMin));
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (_nullHandlingEnabled) {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      if (nullBitmap == null) {
        nullBitmap = new RoaringBitmap();
      }
      if (nullBitmap.getCardinality() < length) {
        double[] valueArray = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          double value = valueArray[i];
          int groupKey = groupKeyArray[i];
          Double result = groupByResultHolder.getResult(groupKey);
          if (!nullBitmap.contains(i) && (result == null || value < result)) {
            groupByResultHolder.setValueForKey(groupKey, value);
          }
        }
      }
      return;
    }

    double[] valueArray = blockValSet.getDoubleValuesSV();
    for (int i = 0; i < length; i++) {
      double value = valueArray[i];
      int groupKey = groupKeyArray[i];
      if (value < groupByResultHolder.getDoubleResult(groupKey)) {
        groupByResultHolder.setValueForKey(groupKey, value);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    double[] valueArray = blockValSetMap.get(_expression).getDoubleValuesSV();
    for (int i = 0; i < length; i++) {
      double value = valueArray[i];
      for (int groupKey : groupKeysArray[i]) {
        if (value < groupByResultHolder.getDoubleResult(groupKey)) {
          groupByResultHolder.setValueForKey(groupKey, value);
        }
      }
    }
  }

  @Override
  public Double extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    if (_nullHandlingEnabled) {
      return aggregationResultHolder.getResult();
    }
    return aggregationResultHolder.getDoubleResult();
  }

  @Override
  public Double extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    if (_nullHandlingEnabled) {
      return groupByResultHolder.getResult(groupKey);
    }
    return groupByResultHolder.getDoubleResult(groupKey);
  }

  @Override
  public Double merge(Double intermediateMinResult1, Double intermediateMinResult2) {
    if (_nullHandlingEnabled) {
      if (intermediateMinResult1 == null) {
        return intermediateMinResult2;
      }
      if (intermediateMinResult2 == null) {
        return intermediateMinResult1;
      }
    }

    if (intermediateMinResult1 < intermediateMinResult2) {
      return intermediateMinResult1;
    }
    return intermediateMinResult2;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.DOUBLE;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.DOUBLE;
  }

  @Override
  public Double extractFinalResult(Double intermediateResult) {
    return intermediateResult;
  }
}
