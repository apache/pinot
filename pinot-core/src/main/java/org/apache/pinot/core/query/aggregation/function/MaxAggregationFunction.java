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


public class MaxAggregationFunction extends BaseSingleInputAggregationFunction<Double, Double> {
  private static final double DEFAULT_INITIAL_VALUE = Double.NEGATIVE_INFINITY;
  private final boolean _nullHandlingEnabled;

  public MaxAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    this(verifyArguments(arguments), nullHandlingEnabled);
  }

  private static ExpressionContext verifyArguments(List<ExpressionContext> arguments) {
    Preconditions.checkArgument(arguments.size() == 1, "MAX expects 1 argument, got: %s", arguments.size());
    return arguments.get(0);
  }

  protected MaxAggregationFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression);
    _nullHandlingEnabled = nullHandlingEnabled;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.MAX;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    if (_nullHandlingEnabled) {
      return new ObjectAggregationResultHolder();
    }
    return new DoubleAggregationResultHolder(DEFAULT_INITIAL_VALUE);
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    if (_nullHandlingEnabled) {
      return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
    }
    return new DoubleGroupByResultHolder(initialCapacity, maxCapacity, DEFAULT_INITIAL_VALUE);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (_nullHandlingEnabled) {
      // TODO: avoid the null bitmap check when it is null or empty for better performance.
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
        int max = values[0];
        for (int i = 0; i < length & i < values.length; i++) {
          max = Math.max(values[i], max);
        }
        aggregationResultHolder.setValue(Math.max(max, aggregationResultHolder.getDoubleResult()));
        break;
      }
      case LONG: {
        long[] values = blockValSet.getLongValuesSV();
        long max = values[0];
        for (int i = 0; i < length & i < values.length; i++) {
          max = Math.max(values[i], max);
        }
        aggregationResultHolder.setValue(Math.max(max, aggregationResultHolder.getDoubleResult()));
        break;
      }
      case FLOAT: {
        float[] values = blockValSet.getFloatValuesSV();
        float max = values[0];
        for (int i = 0; i < length & i < values.length; i++) {
          max = Math.max(values[i], max);
        }
        aggregationResultHolder.setValue(Math.max(max, aggregationResultHolder.getDoubleResult()));
        break;
      }
      case DOUBLE: {
        double[] values = blockValSet.getDoubleValuesSV();
        double max = values[0];
        for (int i = 0; i < length & i < values.length; i++) {
          max = Math.max(values[i], max);
        }
        aggregationResultHolder.setValue(Math.max(max, aggregationResultHolder.getDoubleResult()));
        break;
      }
      case BIG_DECIMAL: {
        BigDecimal[] values = blockValSet.getBigDecimalValuesSV();
        BigDecimal max = values[0];
        for (int i = 0; i < length & i < values.length; i++) {
          max = values[i].max(max);
        }
        // TODO: even though the source data has BIG_DECIMAL type, we still only support double precision.
        aggregationResultHolder.setValue(Math.max(max.doubleValue(), aggregationResultHolder.getDoubleResult()));
        break;
      }
      default:
        throw new IllegalStateException("Cannot compute max for non-numeric type: " + blockValSet.getValueType());
    }
  }

  private void aggregateNullHandlingEnabled(int length, AggregationResultHolder aggregationResultHolder,
      BlockValSet blockValSet, RoaringBitmap nullBitmap) {
    switch (blockValSet.getValueType().getStoredType()) {
      case INT: {
        if (nullBitmap.getCardinality() < length) {
          int[] values = blockValSet.getIntValuesSV();
          int max = Integer.MIN_VALUE;
          for (int i = 0; i < length & i < values.length; i++) {
            if (!nullBitmap.contains(i)) {
              max = Math.max(values[i], max);
            }
          }
          updateAggregationResultHolder(aggregationResultHolder, max);
        }
        // Note: when all input values re null (nullBitmap.getCardinality() == values.length), max is null. As a result,
        // we don't update the value of aggregationResultHolder.
        break;
      }
      case LONG: {
        if (nullBitmap.getCardinality() < length) {
          long[] values = blockValSet.getLongValuesSV();
          long max = Long.MIN_VALUE;
          for (int i = 0; i < length & i < values.length; i++) {
            if (!nullBitmap.contains(i)) {
              max = Math.max(values[i], max);
            }
          }
          updateAggregationResultHolder(aggregationResultHolder, max);
        }
        break;
      }
      case FLOAT: {
        if (nullBitmap.getCardinality() < length) {
          float[] values = blockValSet.getFloatValuesSV();
          float max = Float.NEGATIVE_INFINITY;
          for (int i = 0; i < length & i < values.length; i++) {
            if (!nullBitmap.contains(i)) {
              max = Math.max(values[i], max);
            }
          }
          updateAggregationResultHolder(aggregationResultHolder, max);
        }
        break;
      }
      case DOUBLE: {
        if (nullBitmap.getCardinality() < length) {
          double[] values = blockValSet.getDoubleValuesSV();
          double max = Double.NEGATIVE_INFINITY;
          for (int i = 0; i < length & i < values.length; i++) {
            if (!nullBitmap.contains(i)) {
              max = Math.max(values[i], max);
            }
          }
          updateAggregationResultHolder(aggregationResultHolder, max);
        }
        break;
      }
      case BIG_DECIMAL: {
        if (nullBitmap.getCardinality() < length) {
          BigDecimal[] values = blockValSet.getBigDecimalValuesSV();
          BigDecimal max = null;
          for (int i = 0; i < length & i < values.length; i++) {
            if (!nullBitmap.contains(i)) {
              max = max == null ? values[i] : values[i].max(max);
            }
          }
          // TODO: even though the source data has BIG_DECIMAL type, we still only support double precision.
          assert max != null;
          updateAggregationResultHolder(aggregationResultHolder, max.doubleValue());
        }
        break;
      }
      default:
        throw new IllegalStateException("Cannot compute max for non-numeric type: " + blockValSet.getValueType());
    }
  }

  private void updateAggregationResultHolder(AggregationResultHolder aggregationResultHolder, double max) {
    Double otherMax = aggregationResultHolder.getResult();
    aggregationResultHolder.setValue(otherMax == null ? max : Math.max(max, otherMax));
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
          if (!nullBitmap.contains(i) && (result == null || value > result)) {
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
      if (value > groupByResultHolder.getDoubleResult(groupKey)) {
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
        if (value > groupByResultHolder.getDoubleResult(groupKey)) {
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
  public Double merge(Double intermediateMaxResult1, Double intermediateMaxResult2) {
    if (_nullHandlingEnabled) {
      if (intermediateMaxResult1 == null) {
        return intermediateMaxResult2;
      }
      if (intermediateMaxResult2 == null) {
        return intermediateMaxResult1;
      }
    }

    if (intermediateMaxResult1 > intermediateMaxResult2) {
      return intermediateMaxResult1;
    }
    return intermediateMaxResult2;
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
