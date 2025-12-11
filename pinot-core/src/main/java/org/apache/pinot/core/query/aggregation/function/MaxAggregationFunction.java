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


public class MaxAggregationFunction extends NullableSingleInputAggregationFunction<Double, Double> {
  protected static final double DEFAULT_INITIAL_VALUE = Double.NEGATIVE_INFINITY;

  public MaxAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    this(verifySingleArgument(arguments, "MAX"), nullHandlingEnabled);
  }

  protected MaxAggregationFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression, nullHandlingEnabled);
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

    if (blockValSet.isSingleValue()) {
      aggregateSV(blockValSet, length, aggregationResultHolder);
    } else {
      aggregateMV(blockValSet, length, aggregationResultHolder);
    }
  }

  protected void aggregateSV(BlockValSet blockValSet, int length, AggregationResultHolder aggregationResultHolder) {
    switch (blockValSet.getValueType().getStoredType()) {
      case INT: {
        int[] values = blockValSet.getIntValuesSV();

        Integer max = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          int innerMax = values[from];
          for (int i = from; i < to; i++) {
            innerMax = Math.max(innerMax, values[i]);
          }
          return acum == null ? innerMax : Math.max(acum, innerMax);
        });

        updateAggregationResultHolder(aggregationResultHolder, max);
        break;
      }
      case LONG: {
        long[] values = blockValSet.getLongValuesSV();

        Long max = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          long innerMax = values[from];
          for (int i = from; i < to; i++) {
            innerMax = Math.max(innerMax, values[i]);
          }
          return acum == null ? innerMax : Math.max(acum, innerMax);
        });

        updateAggregationResultHolder(aggregationResultHolder, max);
        break;
      }
      case FLOAT: {
        float[] values = blockValSet.getFloatValuesSV();

        Float max = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          float innerMax = values[from];
          for (int i = from; i < to; i++) {
            innerMax = Math.max(innerMax, values[i]);
          }
          return acum == null ? innerMax : Math.max(acum, innerMax);
        });

        updateAggregationResultHolder(aggregationResultHolder, max);
        break;
      }
      case DOUBLE: {
        double[] values = blockValSet.getDoubleValuesSV();

        Double max = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          double innerMax = values[from];
          for (int i = from; i < to; i++) {
            innerMax = Math.max(innerMax, values[i]);
          }
          return acum == null ? innerMax : Math.max(acum, innerMax);
        });

        updateAggregationResultHolder(aggregationResultHolder, max);
        break;
      }
      case BIG_DECIMAL: {
        BigDecimal[] values = blockValSet.getBigDecimalValuesSV();

        BigDecimal max = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          BigDecimal innerMax = values[from];
          for (int i = from; i < to; i++) {
            innerMax = innerMax.max(values[i]);
          }
          return acum == null ? innerMax : acum.max(innerMax);
        });

        // TODO: even though the source data has BIG_DECIMAL type, we still only support double precision.
        updateAggregationResultHolder(aggregationResultHolder, max);
        break;
      }
      default:
        throw new BadQueryRequestException("Cannot compute max for non-numeric type: " + blockValSet.getValueType());
    }
  }

  protected void aggregateMV(BlockValSet blockValSet, int length, AggregationResultHolder aggregationResultHolder) {
    double[][] valuesArray = blockValSet.getDoubleValuesMV();
    Double max = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
      double innerMax = DEFAULT_INITIAL_VALUE;
      for (int i = from; i < to; i++) {
        double[] values = valuesArray[i];
        for (double value : values) {
          if (value > innerMax) {
            innerMax = value;
          }
        }
      }
      return acum == null ? innerMax : Math.max(acum, innerMax);
    });

    updateAggregationResultHolder(aggregationResultHolder, max);
  }

  protected void updateAggregationResultHolder(AggregationResultHolder aggregationResultHolder, Number max) {
    if (max != null) {
      if (_nullHandlingEnabled) {
        Double otherMax = aggregationResultHolder.getResult();
        aggregationResultHolder.setValue(otherMax == null ? max.doubleValue() : Math.max(max.doubleValue(), otherMax));
      } else {
        double otherMax = aggregationResultHolder.getDoubleResult();
        aggregationResultHolder.setValue(Math.max(max.doubleValue(), otherMax));
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    if (blockValSet.isSingleValue()) {
      aggregateSVGroupBySV(blockValSet, length, groupKeyArray, groupByResultHolder);
    } else {
      aggregateMVGroupBySV(blockValSet, length, groupKeyArray, groupByResultHolder);
    }
  }

  protected void aggregateSVGroupBySV(BlockValSet blockValSet, int length, int[] groupKeyArray,
      GroupByResultHolder groupByResultHolder) {
    double[] valueArray = blockValSet.getDoubleValuesSV();

    if (_nullHandlingEnabled) {
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          double value = valueArray[i];
          int groupKey = groupKeyArray[i];
          Double result = groupByResultHolder.getResult(groupKey);
          if (result == null || value > result) {
            groupByResultHolder.setValueForKey(groupKey, value);
          }
        }
      });
    } else {
      for (int i = 0; i < length; i++) {
        double value = valueArray[i];
        int groupKey = groupKeyArray[i];
        if (value > groupByResultHolder.getDoubleResult(groupKey)) {
          groupByResultHolder.setValueForKey(groupKey, value);
        }
      }
    }
  }

  protected void aggregateMVGroupBySV(BlockValSet blockValSet, int length, int[] groupKeyArray,
      GroupByResultHolder groupByResultHolder) {
    double[][] valuesArray = blockValSet.getDoubleValuesMV();

    if (_nullHandlingEnabled) {
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          int groupKey = groupKeyArray[i];
          Double max = groupByResultHolder.getResult(groupKey);
          for (double value : valuesArray[i]) {
            if (max == null || value > max) {
              max = value;
            }
          }
          groupByResultHolder.setValueForKey(groupKey, max);
        }
      });
    } else {
      for (int i = 0; i < length; i++) {
        int groupKey = groupKeyArray[i];
        double max = groupByResultHolder.getDoubleResult(groupKey);
        for (double value : valuesArray[i]) {
          if (value > max) {
            max = value;
          }
        }
        groupByResultHolder.setValueForKey(groupKey, max);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    if (blockValSet.isSingleValue()) {
      aggregateSVGroupByMV(blockValSet, length, groupKeysArray, groupByResultHolder);
    } else {
      aggregateMVGroupByMV(blockValSet, length, groupKeysArray, groupByResultHolder);
    }
  }

  protected void aggregateSVGroupByMV(BlockValSet blockValSet, int length, int[][] groupKeysArray,
      GroupByResultHolder groupByResultHolder) {
    double[] valueArray = blockValSet.getDoubleValuesSV();

    if (_nullHandlingEnabled) {
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          double value = valueArray[i];
          for (int groupKey : groupKeysArray[i]) {
            Double result = groupByResultHolder.getResult(groupKey);
            if (result == null || value > result) {
              groupByResultHolder.setValueForKey(groupKey, value);
            }
          }
        }
      });
    } else {
      for (int i = 0; i < length; i++) {
        double value = valueArray[i];
        for (int groupKey : groupKeysArray[i]) {
          if (value > groupByResultHolder.getDoubleResult(groupKey)) {
            groupByResultHolder.setValueForKey(groupKey, value);
          }
        }
      }
    }
  }

  protected void aggregateMVGroupByMV(BlockValSet blockValSet, int length, int[][] groupKeysArray,
      GroupByResultHolder groupByResultHolder) {
    double[][] valuesArray = blockValSet.getDoubleValuesMV();

    if (_nullHandlingEnabled) {
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          Double max = null;
          for (double value : valuesArray[i]) {
            if (max == null || value > max) {
              max = value;
            }
          }

          for (int groupKey : groupKeysArray[i]) {
            Double currentMax = groupByResultHolder.getResult(groupKey);
            if (currentMax == null || (max != null && max > currentMax)) {
              groupByResultHolder.setValueForKey(groupKey, max);
            }
          }
        }
      });
    } else {
      for (int i = 0; i < length; i++) {
        double[] values = valuesArray[i];
        for (int groupKey : groupKeysArray[i]) {
          double max = groupByResultHolder.getDoubleResult(groupKey);
          for (double value : values) {
            if (value > max) {
              max = value;
            }
          }
          groupByResultHolder.setValueForKey(groupKey, max);
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

  @Override
  public Double mergeFinalResult(Double finalResult1, Double finalResult2) {
    return merge(finalResult1, finalResult2);
  }
}
