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
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.DoubleAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.DoubleGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.roaringbitmap.RoaringBitmap;


public class SumAggregationFunction extends BaseSingleInputAggregationFunction<Double, Double> {
  private static final double DEFAULT_VALUE = 0.0;
  private final boolean _nullHandlingEnabled;

  private Integer _groupKeyForNullValue = null;

  public SumAggregationFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression);
    _nullHandlingEnabled = nullHandlingEnabled;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.SUM;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new DoubleAggregationResultHolder(DEFAULT_VALUE);
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new DoubleGroupByResultHolder(initialCapacity, maxCapacity, DEFAULT_VALUE);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    double sum = aggregationResultHolder.getDoubleResult();
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (_nullHandlingEnabled) {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        aggregateNullHandlingEnabled(length, aggregationResultHolder, blockValSet, nullBitmap);
        return;
      }
    }

    switch (blockValSet.getValueType().getStoredType()) {
      case INT: {
        int[] values = blockValSet.getIntValuesSV();
        for (int i = 0; i < length & i < values.length; i++) {
          sum += values[i];
        }
        break;
      }
      case LONG: {
        long[] values = blockValSet.getLongValuesSV();
        for (int i = 0; i < length & i < values.length; i++) {
          sum += values[i];
        }
        break;
      }
      case FLOAT: {
        float[] values = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length & i < values.length; i++) {
          sum += values[i];
        }
        break;
      }
      case DOUBLE: {
        double[] values = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length & i < values.length; i++) {
          sum += values[i];
        }
        break;
      }
      case BIG_DECIMAL: {
        BigDecimal decimalSum = BigDecimal.valueOf(sum);
        BigDecimal[] values = blockValSet.getBigDecimalValuesSV();
        for (int i = 0; i < length & i < values.length; i++) {
          decimalSum = decimalSum.add(values[i]);
        }
        // TODO: even though the source data has BIG_DECIMAL type, we still only support double precision.
        sum = decimalSum.doubleValue();
        break;
      }
      default:
        throw new IllegalStateException("Cannot compute sum for non-numeric type: " + blockValSet.getValueType());
    }
    aggregationResultHolder.setValue(sum);
  }

  private void aggregateNullHandlingEnabled(int length, AggregationResultHolder aggregationResultHolder,
      BlockValSet blockValSet, RoaringBitmap nullBitmap) {
    double sum = aggregationResultHolder.getDoubleResult();

    switch (blockValSet.getValueType().getStoredType()) {
      case INT: {
        int[] values = blockValSet.getIntValuesSV();
        if (nullBitmap.getCardinality() < values.length) {
          for (int i = 0; i < length & i < values.length; i++) {
            if (!nullBitmap.contains(i)) {
              sum += values[i];
            }
          }
        }
        break;
      }
      case LONG: {
        long[] values = blockValSet.getLongValuesSV();
        if (nullBitmap.getCardinality() < values.length) {
          for (int i = 0; i < length & i < values.length; i++) {
            if (!nullBitmap.contains(i)) {
              sum += values[i];
            }
          }
        }
        break;
      }
      case FLOAT: {
        float[] values = blockValSet.getFloatValuesSV();
        if (nullBitmap.getCardinality() < values.length) {
          for (int i = 0; i < length & i < values.length; i++) {
            if (!nullBitmap.contains(i)) {
              sum += values[i];
            }
          }
        }
        break;
      }
      case DOUBLE: {
        double[] values = blockValSet.getDoubleValuesSV();
        if (nullBitmap.getCardinality() < values.length) {
          for (int i = 0; i < length & i < values.length; i++) {
            if (!nullBitmap.contains(i)) {
              sum += values[i];
            }
          }
        }
        break;
      }
      case BIG_DECIMAL: {
        BigDecimal decimalSum = BigDecimal.valueOf(sum);
        BigDecimal[] values = blockValSet.getBigDecimalValuesSV();
        if (nullBitmap.getCardinality() < values.length) {
          for (int i = 0; i < length & i < values.length; i++) {
            if (!nullBitmap.contains(i)) {
              decimalSum = decimalSum.add(values[i]);
            }
          }
        }
        // TODO: even though the source data has BIG_DECIMAL type, we still only support double precision.
        sum = decimalSum.doubleValue();
        break;
      }
      default:
        throw new IllegalStateException("Cannot compute sum for non-numeric type: " + blockValSet.getValueType());
    }
    aggregationResultHolder.setValue(sum);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    double[] valueArray = blockValSet.getDoubleValuesSV();
    if (_nullHandlingEnabled) {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        aggregateGroupBySVNullHandlingEnabled(length, groupKeyArray, groupByResultHolder, valueArray, nullBitmap);
        return;
      }
    }

    for (int i = 0; i < length; i++) {
      int groupKey = groupKeyArray[i];
      groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + valueArray[i]);
    }
  }

  private void aggregateGroupBySVNullHandlingEnabled(int length, int[] groupKeyArray,
      GroupByResultHolder groupByResultHolder, double[] valueArray, RoaringBitmap nullBitmap) {
    if (nullBitmap.getCardinality() < length) {
      for (int i = 0; i < length; i++) {
        int groupKey = groupKeyArray[i];
        // Preserve null group key.
        if (nullBitmap.contains(i)) {
          // There should be only one groupKey for the null value.
          assert _groupKeyForNullValue == null || _groupKeyForNullValue == groupKey;
          _groupKeyForNullValue = groupKey;
        } else {
          groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + valueArray[i]);
        }
      }
    } else {
      _groupKeyForNullValue = groupKeyArray[0];
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    double[] valueArray = blockValSetMap.get(_expression).getDoubleValuesSV();
    for (int i = 0; i < length; i++) {
      double value = valueArray[i];
      for (int groupKey : groupKeysArray[i]) {
        groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + value);
      }
    }
  }

  @Override
  public Double extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getDoubleResult();
  }

  @Override
  public Double extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    if (_groupKeyForNullValue != null && _groupKeyForNullValue == groupKey) {
      return null;
    }
    return groupByResultHolder.getDoubleResult(groupKey);
  }

  @Override
  public Double merge(Double intermediateResult1, Double intermediateResult2) {
    if (_nullHandlingEnabled) {
      if (intermediateResult1 == null) {
        return intermediateResult2;
      }
      if (intermediateResult2 == null) {
        return intermediateResult1;
      }
    }
    return intermediateResult1 + intermediateResult2;
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
