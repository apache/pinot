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


public class MinAggregationFunction extends NullableSingleInputAggregationFunction<Double, Double> {
  private static final double DEFAULT_VALUE = Double.POSITIVE_INFINITY;

  public MinAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    this(verifySingleArgument(arguments, "MIN"), nullHandlingEnabled);
  }

  protected MinAggregationFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression, nullHandlingEnabled);
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

    switch (blockValSet.getValueType().getStoredType()) {
      case INT: {
        int[] values = blockValSet.getIntValuesSV();

        Integer min = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          int innerMin = values[from];
          for (int i = from; i < to; i++) {
            innerMin = Math.min(innerMin, values[i]);
          }
          return acum == null ? innerMin : Math.min(acum, innerMin);
        });

        updateAggregationResultHolder(aggregationResultHolder, min);
        break;
      }
      case LONG: {
        long[] values = blockValSet.getLongValuesSV();

        Long min = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          long innerMin = values[from];
          for (int i = from; i < to; i++) {
            innerMin = Math.min(innerMin, values[i]);
          }
          return acum == null ? innerMin : Math.min(acum, innerMin);
        });

        updateAggregationResultHolder(aggregationResultHolder, min);
        break;
      }
      case FLOAT: {
        float[] values = blockValSet.getFloatValuesSV();

        Float min = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          float innerMin = values[from];
          for (int i = from; i < to; i++) {
            innerMin = Math.min(innerMin, values[i]);
          }
          return acum == null ? innerMin : Math.min(acum, innerMin);
        });

        updateAggregationResultHolder(aggregationResultHolder, min);
        break;
      }
      case DOUBLE: {
        double[] values = blockValSet.getDoubleValuesSV();

        Double min = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          double innerMin = values[from];
          for (int i = from; i < to; i++) {
            innerMin = Math.min(innerMin, values[i]);
          }
          return acum == null ? innerMin : Math.min(acum, innerMin);
        });

        updateAggregationResultHolder(aggregationResultHolder, min);
        break;
      }
      case BIG_DECIMAL: {
        BigDecimal[] values = blockValSet.getBigDecimalValuesSV();

        BigDecimal min = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          BigDecimal innerMin = values[from];
          for (int i = from; i < to; i++) {
            innerMin = innerMin.min(values[i]);
          }
          return acum == null ? innerMin : acum.min(innerMin);
        });

        // TODO: even though the source data has BIG_DECIMAL type, we still only support double precision.
        updateAggregationResultHolder(aggregationResultHolder, min);
        break;
      }
      default:
        throw new BadQueryRequestException("Cannot compute min for non-numeric type: " + blockValSet.getValueType());
    }
  }

  private void updateAggregationResultHolder(AggregationResultHolder aggregationResultHolder, Number min) {
    if (min != null) {
      if (_nullHandlingEnabled) {
        Double otherMin = aggregationResultHolder.getResult();
        aggregationResultHolder.setValue(otherMin == null ? min.doubleValue() : Math.min(min.doubleValue(), otherMin));
      } else {
        double otherMin = aggregationResultHolder.getDoubleResult();
        aggregationResultHolder.setValue(Math.min(min.doubleValue(), otherMin));
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
          double value = valueArray[i];
          int groupKey = groupKeyArray[i];
          Double result = groupByResultHolder.getResult(groupKey);
          if (result == null || value < result) {
            groupByResultHolder.setValueForKey(groupKey, value);
          }
        }
      });
    } else {
      for (int i = 0; i < length; i++) {
        double value = valueArray[i];
        int groupKey = groupKeyArray[i];
        if (value < groupByResultHolder.getDoubleResult(groupKey)) {
          groupByResultHolder.setValueForKey(groupKey, value);
        }
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
          double value = valueArray[i];
          for (int groupKey : groupKeysArray[i]) {
            Double result = groupByResultHolder.getResult(groupKey);
            if (result == null || value < result) {
              groupByResultHolder.setValueForKey(groupKey, value);
            }
          }
        }
      });
    } else {
      for (int i = 0; i < length; i++) {
        double value = valueArray[i];
        for (int groupKey : groupKeysArray[i]) {
          if (value < groupByResultHolder.getDoubleResult(groupKey)) {
            groupByResultHolder.setValueForKey(groupKey, value);
          }
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

  @Override
  public Double mergeFinalResult(Double finalResult1, Double finalResult2) {
    return merge(finalResult1, finalResult2);
  }
}
