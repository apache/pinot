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
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.utils.BigDecimalUtils;


/**
 * This function is used for BigDecimal calculations. It supports the sum aggregation using both precision and scale.
 * <p>The function can be used as SUMPRECISION(expression, precision, scale)
 * <p>Following arguments are supported:
 * <ul>
 *   <li>Expression: expression that contains the values to be summed up, can be serialized BigDecimal objects</li>
 *   <li>Precision (optional): precision to be set to the final result</li>
 *   <li>Scale (optional): scale to be set to the final result</li>
 * </ul>
 */
public class SumPrecisionAggregationFunction extends NullableSingleInputAggregationFunction<BigDecimal, BigDecimal> {
  private final Integer _precision;
  private final Integer _scale;

  public SumPrecisionAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(arguments.get(0), nullHandlingEnabled);

    int numArguments = arguments.size();
    Preconditions.checkArgument(numArguments <= 3, "SumPrecision expects at most 3 arguments, got: %s", numArguments);
    if (numArguments > 1) {
      _precision = arguments.get(1).getLiteral().getIntValue();
      if (numArguments > 2) {
        _scale = arguments.get(2).getLiteral().getIntValue();
      } else {
        _scale = null;
      }
    } else {
      _precision = null;
      _scale = null;
    }
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.SUMPRECISION;
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
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    BigDecimal sum;
    switch (blockValSet.getValueType().getStoredType()) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();

        sum = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          BigDecimal innerSum = BigDecimal.ZERO;
          for (int i = from; i < to; i++) {
            innerSum = innerSum.add(BigDecimal.valueOf(intValues[i]));
          }
          return acum == null ? innerSum : acum.add(innerSum);
        });

        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();

        sum = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          BigDecimal innerSum = BigDecimal.ZERO;
          for (int i = from; i < to; i++) {
            innerSum = innerSum.add(BigDecimal.valueOf(longValues[i]));
          }
          return acum == null ? innerSum : acum.add(innerSum);
        });

        break;
      case FLOAT:
      case DOUBLE:
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();

        sum = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          BigDecimal innerSum = BigDecimal.ZERO;
          for (int i = from; i < to; i++) {
            innerSum = innerSum.add(new BigDecimal(stringValues[i]));
          }
          return acum == null ? innerSum : acum.add(innerSum);
        });

        break;
      case BIG_DECIMAL:
        BigDecimal[] bigDecimalValues = blockValSet.getBigDecimalValuesSV();

        sum = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          BigDecimal innerSum = BigDecimal.ZERO;
          for (int i = from; i < to; i++) {
            innerSum = innerSum.add(bigDecimalValues[i]);
          }
          return acum == null ? innerSum : acum.add(innerSum);
        });

        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();

        sum = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          BigDecimal innerSum = BigDecimal.ZERO;
          for (int i = from; i < to; i++) {
            innerSum = innerSum.add(BigDecimalUtils.deserialize(bytesValues[i]));
          }
          return acum == null ? innerSum : acum.add(innerSum);
        });

        break;
      default:
        throw new IllegalStateException();
    }
    updateAggregationResult(aggregationResultHolder, sum);
  }

  protected void updateAggregationResult(AggregationResultHolder aggregationResultHolder, BigDecimal sum) {
    if (_nullHandlingEnabled) {
      if (sum != null) {
        BigDecimal otherSum = aggregationResultHolder.getResult();
        aggregationResultHolder.setValue(otherSum == null ? sum : sum.add(otherSum));
      }
    } else {
      if (sum == null) {
        sum = BigDecimal.ZERO;
      }
      BigDecimal otherSum = aggregationResultHolder.getResult();
      aggregationResultHolder.setValue(otherSum == null ? sum : sum.add(otherSum));
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    switch (blockValSet.getValueType().getStoredType()) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();

        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            updateGroupByResult(groupKeyArray[i], groupByResultHolder, BigDecimal.valueOf(intValues[i]));
          }
        });

        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();

        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            updateGroupByResult(groupKeyArray[i], groupByResultHolder, BigDecimal.valueOf(longValues[i]));
          }
        });

        break;
      case FLOAT:
      case DOUBLE:
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();

        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            updateGroupByResult(groupKeyArray[i], groupByResultHolder, new BigDecimal(stringValues[i]));
          }
        });

        break;
      case BIG_DECIMAL:
        BigDecimal[] bigDecimalValues = blockValSet.getBigDecimalValuesSV();

        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            updateGroupByResult(groupKeyArray[i], groupByResultHolder, bigDecimalValues[i]);
          }
        });

        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();

        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            updateGroupByResult(groupKeyArray[i], groupByResultHolder, BigDecimalUtils.deserialize(bytesValues[i]));
          }
        });

        break;
      default:
        throw new IllegalStateException();
    }
  }

  private void updateGroupByResult(int groupKey, GroupByResultHolder groupByResultHolder, BigDecimal value) {
    BigDecimal sum = groupByResultHolder.getResult(groupKey);
    sum = sum == null ? value : sum.add(value);
    groupByResultHolder.setValueForKey(groupKey, sum);
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    switch (blockValSet.getValueType().getStoredType()) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();

        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int groupKey : groupKeysArray[i]) {
              updateGroupByResult(groupKey, groupByResultHolder, BigDecimal.valueOf(intValues[i]));
            }
          }
        });

        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();

        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int groupKey : groupKeysArray[i]) {
              updateGroupByResult(groupKey, groupByResultHolder, BigDecimal.valueOf(longValues[i]));
            }
          }
        });

        break;
      case FLOAT:
      case DOUBLE:
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();

        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int groupKey : groupKeysArray[i]) {
              updateGroupByResult(groupKey, groupByResultHolder, new BigDecimal(stringValues[i]));
            }
          }
        });

        break;
      case BIG_DECIMAL:
        BigDecimal[] bigDecimalValues = blockValSet.getBigDecimalValuesSV();

        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int groupKey : groupKeysArray[i]) {
              updateGroupByResult(groupKey, groupByResultHolder, bigDecimalValues[i]);
            }
          }
        });

        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();

        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int groupKey : groupKeysArray[i]) {
              updateGroupByResult(groupKey, groupByResultHolder, BigDecimalUtils.deserialize(bytesValues[i]));
            }
          }
        });

        break;
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  public BigDecimal extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    BigDecimal result = aggregationResultHolder.getResult();
    if (result == null) {
      return _nullHandlingEnabled ? null : BigDecimal.ZERO;
    }
    return result;
  }

  @Override
  public BigDecimal extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    BigDecimal result = groupByResultHolder.getResult(groupKey);
    if (result == null) {
      return _nullHandlingEnabled ? null : BigDecimal.ZERO;
    }
    return result;
  }

  @Override
  public BigDecimal merge(BigDecimal intermediateResult1, BigDecimal intermediateResult2) {
    if (_nullHandlingEnabled) {
      if (intermediateResult1 == null) {
        return intermediateResult2;
      }
      if (intermediateResult2 == null) {
        return intermediateResult1;
      }
    }
    return intermediateResult1.add(intermediateResult2);
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    // TODO: Revisit if we should change this to BIG_DECIMAL
    return ColumnDataType.STRING;
  }

  @Override
  public BigDecimal extractFinalResult(BigDecimal intermediateResult) {
    if (intermediateResult == null) {
      return null;
    }
    if (_precision == null) {
      return intermediateResult;
    }
    BigDecimal result = intermediateResult.round(new MathContext(_precision, RoundingMode.HALF_EVEN));
    return _scale == null ? result : result.setScale(_scale, RoundingMode.HALF_EVEN);
  }

  @Override
  public BigDecimal mergeFinalResult(BigDecimal finalResult1, BigDecimal finalResult2) {
    return merge(finalResult1, finalResult2);
  }

  public BigDecimal getDefaultResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    BigDecimal result = groupByResultHolder.getResult(groupKey);
    return result != null ? result : BigDecimal.ZERO;
  }
}
