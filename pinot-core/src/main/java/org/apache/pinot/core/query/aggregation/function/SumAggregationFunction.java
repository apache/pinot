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
import org.apache.pinot.spi.exception.BadQueryRequestException;


public class SumAggregationFunction extends NullableSingleInputAggregationFunction<Double, Double> {
  protected static final double DEFAULT_VALUE = 0.0;

  public SumAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    this(verifySingleArgument(arguments, "SUM"), nullHandlingEnabled);
  }

  protected SumAggregationFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression, nullHandlingEnabled);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.SUM;
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

    Double sum;
    switch (blockValSet.getValueType().getStoredType()) {
      case INT: {
        int[] values = blockValSet.getIntValuesSV();

        sum = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          double innerSum = 0;
          for (int i = from; i < to; i++) {
            innerSum += values[i];
          }
          return acum == null ? innerSum : acum + innerSum;
        });

        break;
      }
      case LONG: {
        long[] values = blockValSet.getLongValuesSV();

        sum = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          double innerSum = 0;
          for (int i = from; i < to; i++) {
            innerSum += values[i];
          }
          return acum == null ? innerSum : acum + innerSum;
        });

        break;
      }
      case FLOAT: {
        float[] values = blockValSet.getFloatValuesSV();

        sum = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          double innerSum = 0;
          for (int i = from; i < to; i++) {
            innerSum += values[i];
          }
          return acum == null ? innerSum : acum + innerSum;
        });

        break;
      }
      case DOUBLE: {
        double[] values = blockValSet.getDoubleValuesSV();

        sum = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          double innerSum = 0;
          for (int i = from; i < to; i++) {
            innerSum += values[i];
          }
          return acum == null ? innerSum : acum + innerSum;
        });

        break;
      }
      case BIG_DECIMAL: {
        BigDecimal[] values = blockValSet.getBigDecimalValuesSV();

        BigDecimal decimalSum = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          BigDecimal innerSum = BigDecimal.ZERO;
          for (int i = from; i < to; i++) {
            innerSum = innerSum.add(values[i]);
          }
          return acum == null ? innerSum : acum.add(innerSum);
        });
        // TODO: even though the source data has BIG_DECIMAL type, we still only support double precision.
        sum = decimalSum == null ? null : decimalSum.doubleValue();
        break;
      }
      default:
        throw new BadQueryRequestException("Cannot compute sum for non-numeric type: " + blockValSet.getValueType());
    }
    updateAggregationResultHolder(aggregationResultHolder, sum);
  }

  protected void updateAggregationResultHolder(AggregationResultHolder aggregationResultHolder, Double sum) {
    if (sum != null) {
      if (_nullHandlingEnabled) {
        Double otherSum = aggregationResultHolder.getResult();
        aggregationResultHolder.setValue(otherSum == null ? sum : sum + otherSum);
      } else {
        double otherSum = aggregationResultHolder.getDoubleResult();
        aggregationResultHolder.setValue(sum + otherSum);
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    double[] valueArray = blockValSet.getDoubleValuesSV();

    if (_nullHandlingEnabled) {
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          int groupKey = groupKeyArray[i];
          Double result = groupByResultHolder.getResult(groupKey);
          groupByResultHolder.setValueForKey(groupKey, result == null ? valueArray[i] : result + valueArray[i]);
        }
      });
    } else {
      for (int i = 0; i < length; i++) {
        int groupKey = groupKeyArray[i];
        groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + valueArray[i]);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    double[] valueArray = blockValSet.getDoubleValuesSV();

    if (_nullHandlingEnabled) {
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          for (int groupKey : groupKeysArray[i]) {
            Double result = groupByResultHolder.getResult(groupKey);
            groupByResultHolder.setValueForKey(groupKey, result == null ? valueArray[i] : result + valueArray[i]);
          }
        }
      });
    } else {
      for (int i = 0; i < length; i++) {
        double value = valueArray[i];
        for (int groupKey : groupKeysArray[i]) {
          groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + value);
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

  @Override
  public Double mergeFinalResult(Double finalResult1, Double finalResult2) {
    return merge(finalResult1, finalResult2);
  }
}
