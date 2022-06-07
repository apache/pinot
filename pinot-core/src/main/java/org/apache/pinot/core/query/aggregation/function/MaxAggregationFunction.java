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
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public class MaxAggregationFunction extends BaseSingleInputAggregationFunction<Double, Double> {
  private static final double DEFAULT_INITIAL_VALUE = Double.NEGATIVE_INFINITY;
  // stores id of the groupKey where the corresponding value is null.
  private Integer _groupKeyForNullValue = null;

  public MaxAggregationFunction(ExpressionContext expression) {
    super(expression);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.MAX;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new DoubleAggregationResultHolder(DEFAULT_INITIAL_VALUE);
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new DoubleGroupByResultHolder(initialCapacity, maxCapacity, DEFAULT_INITIAL_VALUE);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    // Nulls should be ignored in aggregation functions (similar to Presto).
    ImmutableRoaringBitmap nullBitmap = blockValSet.getNullBitmap();
    switch (blockValSet.getValueType().getStoredType()) {
      case INT: {
        int[] values = blockValSet.getIntValuesSV();
        // First value can be null.
        int max = Integer.MIN_VALUE;
        if (nullBitmap == null || nullBitmap.getCardinality() < values.length) {
          for (int i = 0; i < length & i < values.length; i++) {
            if (nullBitmap == null || !nullBitmap.contains(i)) {
              max = Math.max(values[i], max);
            }
          }
        }
        // TODO: When all input values are null (nullBitmap.getCardinality() == values.length), Pinot returns
        //  Integer.MIN_VALUE while Presto returns null. Same for other stored types.
        aggregationResultHolder.setValue(Math.max(max, aggregationResultHolder.getDoubleResult()));
        break;
      }
      case LONG: {
        long[] values = blockValSet.getLongValuesSV();
        long max = Long.MIN_VALUE;
        if (nullBitmap == null || nullBitmap.getCardinality() < values.length) {
          for (int i = 0; i < length & i < values.length; i++) {
            if (nullBitmap == null || !nullBitmap.contains(i)) {
              max = Math.max(values[i], max);
            }
          }
        }
        aggregationResultHolder.setValue(Math.max(max, aggregationResultHolder.getDoubleResult()));
        break;
      }
      case FLOAT: {
        float[] values = blockValSet.getFloatValuesSV();
        float max = Float.NEGATIVE_INFINITY;
        if (nullBitmap == null || nullBitmap.getCardinality() < values.length) {
          for (int i = 0; i < length & i < values.length; i++) {
            if (nullBitmap == null || !nullBitmap.contains(i)) {
              max = Math.max(values[i], max);
            }
          }
        }
        aggregationResultHolder.setValue(Math.max(max, aggregationResultHolder.getDoubleResult()));
        break;
      }
      case DOUBLE: {
        double[] values = blockValSet.getDoubleValuesSV();
        double max = Double.NEGATIVE_INFINITY;
        if (nullBitmap == null || nullBitmap.getCardinality() < values.length) {
          for (int i = 0; i < length & i < values.length; i++) {
            if (nullBitmap == null || !nullBitmap.contains(i)) {
              max = Math.max(values[i], max);
            }
          }
        }
        aggregationResultHolder.setValue(Math.max(max, aggregationResultHolder.getDoubleResult()));
        break;
      }
      case BIG_DECIMAL: {
        BigDecimal[] values = blockValSet.getBigDecimalValuesSV();
        BigDecimal max = null;
        if (nullBitmap == null || nullBitmap.getCardinality() < values.length) {
          for (int i = 0; i < length & i < values.length; i++) {
            if (nullBitmap == null || !nullBitmap.contains(i)) {
              max = max == null ? values[i] : values[i].max(max);
            }
          }
        }
        // TODO: even though the source data has BIG_DECIMAL type, we still only support double precision.
        aggregationResultHolder.setValue(Math.max(max == null ? Double.NEGATIVE_INFINITY : max.doubleValue(),
            aggregationResultHolder.getDoubleResult()));
        break;
      }
      default:
        throw new IllegalStateException("Cannot compute max for non-numeric type: " + blockValSet.getValueType());
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    double[] valueArray = blockValSet.getDoubleValuesSV();
    ImmutableRoaringBitmap nullBitmap = blockValSet.getNullBitmap();
    if (nullBitmap == null || nullBitmap.getCardinality() < length) {
      for (int i = 0; i < length; i++) {
        double value = valueArray[i];
        int groupKey = groupKeyArray[i];
        double result = groupByResultHolder.getDoubleResult(groupKey);
        // Preserve null group key.
        if (nullBitmap != null && nullBitmap.contains(i)) {
          // The default value of un-initialized result could be -Infinity (dim column), so don't compare value w/ result.
          // There should be only one groupKey for the null value.
          assert _groupKeyForNullValue == null || _groupKeyForNullValue == groupKey;
          _groupKeyForNullValue = groupKey;
        } else if (value > result) {
          groupByResultHolder.setValueForKey(groupKey, value);
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
        if (value > groupByResultHolder.getDoubleResult(groupKey)) {
          groupByResultHolder.setValueForKey(groupKey, value);
        }
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
  public Double merge(Double intermediateMaxResult1, Double intermediateMaxResult2) {
    if (intermediateMaxResult1 == null) {
      return intermediateMaxResult2;
    }
    if (intermediateMaxResult2 == null) {
      return intermediateMaxResult1;
    }

    if (intermediateMaxResult1 > intermediateMaxResult2) {
      return intermediateMaxResult1;
    } else {
      return intermediateMaxResult2;
    }
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
