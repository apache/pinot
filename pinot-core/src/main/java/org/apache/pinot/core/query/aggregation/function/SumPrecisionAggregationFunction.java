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
import org.apache.pinot.segment.spi.datasource.NullMode;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.roaringbitmap.RoaringBitmap;


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
public class SumPrecisionAggregationFunction extends BaseSingleInputAggregationFunction<BigDecimal, BigDecimal> {
  private final Integer _precision;
  private final Integer _scale;
  private final NullMode _nullMode;

  public SumPrecisionAggregationFunction(List<ExpressionContext> arguments, NullMode nullMode) {
    super(arguments.get(0));

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
    _nullMode = nullMode;
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
    if (_nullMode.nullAtQueryTime()) {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap(_nullMode);
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        aggregateNullHandlingEnabled(length, aggregationResultHolder, blockValSet, nullBitmap);
        return;
      }
    }

    BigDecimal sum = getDefaultResult(aggregationResultHolder);
    switch (blockValSet.getValueType().getStoredType()) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          sum = sum.add(BigDecimal.valueOf(intValues[i]));
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          sum = sum.add(BigDecimal.valueOf(longValues[i]));
        }
        break;
      case FLOAT:
      case DOUBLE:
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          sum = sum.add(new BigDecimal(stringValues[i]));
        }
        break;
      case BIG_DECIMAL:
        BigDecimal[] bigDecimalValues = blockValSet.getBigDecimalValuesSV();
        for (int i = 0; i < length; i++) {
          sum = sum.add(bigDecimalValues[i]);
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          sum = sum.add(BigDecimalUtils.deserialize(bytesValues[i]));
        }
        break;
      default:
        throw new IllegalStateException();
    }
    aggregationResultHolder.setValue(sum);
  }

  private void aggregateNullHandlingEnabled(int length, AggregationResultHolder aggregationResultHolder,
      BlockValSet blockValSet, RoaringBitmap nullBitmap) {
    BigDecimal sum = BigDecimal.ZERO;
    switch (blockValSet.getValueType().getStoredType()) {
      case INT: {
        if (nullBitmap.getCardinality() < length) {
          int[] intValues = blockValSet.getIntValuesSV();
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              sum = sum.add(BigDecimal.valueOf(intValues[i]));
            }
          }
          setAggregationResult(aggregationResultHolder, sum);
        }
        break;
      }
      case LONG: {
        if (nullBitmap.getCardinality() < length) {
          long[] longValues = blockValSet.getLongValuesSV();
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              sum = sum.add(BigDecimal.valueOf(longValues[i]));
            }
          }
          setAggregationResult(aggregationResultHolder, sum);
        }
        break;
      }
      case FLOAT: {
        if (nullBitmap.getCardinality() < length) {
          float[] floatValues = blockValSet.getFloatValuesSV();
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              if (Float.isFinite(floatValues[i])) {
                sum = sum.add(BigDecimal.valueOf(floatValues[i]));
              }
            }
          }
          setAggregationResult(aggregationResultHolder, sum);
        }
        break;
      }
      case DOUBLE: {
        if (nullBitmap.getCardinality() < length) {
          double[] doubleValues = blockValSet.getDoubleValuesSV();
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              // TODO(nhejazi): throw an exception here instead of ignoring infinite values?
              if (Double.isFinite(doubleValues[i])) {
                sum = sum.add(BigDecimal.valueOf(doubleValues[i]));
              }
            }
          }
          setAggregationResult(aggregationResultHolder, sum);
        }
        break;
      }
      case STRING:
        if (nullBitmap.getCardinality() < length) {
          String[] stringValues = blockValSet.getStringValuesSV();
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              sum = sum.add(new BigDecimal(stringValues[i]));
            }
          }
          setAggregationResult(aggregationResultHolder, sum);
        }
        break;
      case BIG_DECIMAL: {
        if (nullBitmap.getCardinality() < length) {
          BigDecimal[] bigDecimalValues = blockValSet.getBigDecimalValuesSV();
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              sum = sum.add(bigDecimalValues[i]);
            }
          }
          setAggregationResult(aggregationResultHolder, sum);
        }
        break;
      }
      case BYTES:
        if (nullBitmap.getCardinality() < length) {
          byte[][] bytesValues = blockValSet.getBytesValuesSV();
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              sum = sum.add(BigDecimalUtils.deserialize(bytesValues[i]));
            }
          }
          setAggregationResult(aggregationResultHolder, sum);
        }
        break;
      default:
        throw new IllegalStateException();
    }
  }

  protected void setAggregationResult(AggregationResultHolder aggregationResultHolder, BigDecimal sum) {
    BigDecimal otherSum = aggregationResultHolder.getResult();
    aggregationResultHolder.setValue(otherSum == null ? sum : sum.add(otherSum));
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (_nullMode.nullAtQueryTime()) {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap(_nullMode);
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        aggregateGroupBySVNullHandlingEnabled(length, groupKeyArray, groupByResultHolder, blockValSet, nullBitmap);
        return;
      }
    }

    switch (blockValSet.getValueType().getStoredType()) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          int groupKey = groupKeyArray[i];
          BigDecimal sum = getDefaultResult(groupByResultHolder, groupKey);
          sum = sum.add(BigDecimal.valueOf(intValues[i]));
          groupByResultHolder.setValueForKey(groupKey, sum);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          int groupKey = groupKeyArray[i];
          BigDecimal sum = getDefaultResult(groupByResultHolder, groupKey);
          sum = sum.add(BigDecimal.valueOf(longValues[i]));
          groupByResultHolder.setValueForKey(groupKey, sum);
        }
        break;
      case FLOAT:
      case DOUBLE:
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          int groupKey = groupKeyArray[i];
          BigDecimal sum = getDefaultResult(groupByResultHolder, groupKey);
          sum = sum.add(new BigDecimal(stringValues[i]));
          groupByResultHolder.setValueForKey(groupKey, sum);
        }
        break;
      case BIG_DECIMAL:
        BigDecimal[] bigDecimalValues = blockValSet.getBigDecimalValuesSV();
        for (int i = 0; i < length; i++) {
          int groupKey = groupKeyArray[i];
          BigDecimal sum = getDefaultResult(groupByResultHolder, groupKey);
          sum = sum.add(bigDecimalValues[i]);
          groupByResultHolder.setValueForKey(groupKey, sum);
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          int groupKey = groupKeyArray[i];
          BigDecimal sum = getDefaultResult(groupByResultHolder, groupKey);
          sum = sum.add(BigDecimalUtils.deserialize(bytesValues[i]));
          groupByResultHolder.setValueForKey(groupKey, sum);
        }
        break;
      default:
        throw new IllegalStateException();
    }
  }

  private void aggregateGroupBySVNullHandlingEnabled(int length, int[] groupKeyArray,
      GroupByResultHolder groupByResultHolder, BlockValSet blockValSet, RoaringBitmap nullBitmap) {
    switch (blockValSet.getValueType().getStoredType()) {
      case INT:
        if (nullBitmap.getCardinality() < length) {
          int[] intValues = blockValSet.getIntValuesSV();
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              setGroupByResult(groupKeyArray[i], groupByResultHolder, BigDecimal.valueOf(intValues[i]));
            }
          }
        }
        break;
      case LONG:
        if (nullBitmap.getCardinality() < length) {
          long[] longValues = blockValSet.getLongValuesSV();
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              setGroupByResult(groupKeyArray[i], groupByResultHolder, BigDecimal.valueOf(longValues[i]));
            }
          }
        }
        break;
      case FLOAT:
      case DOUBLE:
      case STRING:
        if (nullBitmap.getCardinality() < length) {
          String[] stringValues = blockValSet.getStringValuesSV();
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              setGroupByResult(groupKeyArray[i], groupByResultHolder, new BigDecimal(stringValues[i]));
            }
          }
        }
        break;
      case BIG_DECIMAL:
        if (nullBitmap.getCardinality() < length) {
          BigDecimal[] bigDecimalValues = blockValSet.getBigDecimalValuesSV();
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              setGroupByResult(groupKeyArray[i], groupByResultHolder, bigDecimalValues[i]);
            }
          }
        }
        break;
      case BYTES:
        if (nullBitmap.getCardinality() < length) {
          byte[][] bytesValues = blockValSet.getBytesValuesSV();
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              setGroupByResult(groupKeyArray[i], groupByResultHolder, BigDecimalUtils.deserialize(bytesValues[i]));
            }
          }
        }
        break;
      default:
        throw new IllegalStateException();
    }
  }

  private void setGroupByResult(int groupKey, GroupByResultHolder groupByResultHolder, BigDecimal value) {
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
        for (int i = 0; i < length; i++) {
          int value = intValues[i];
          for (int groupKey : groupKeysArray[i]) {
            BigDecimal sum = getDefaultResult(groupByResultHolder, groupKey);
            sum = sum.add(BigDecimal.valueOf(value));
            groupByResultHolder.setValueForKey(groupKey, sum);
          }
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          long value = longValues[i];
          for (int groupKey : groupKeysArray[i]) {
            BigDecimal sum = getDefaultResult(groupByResultHolder, groupKey);
            sum = sum.add(BigDecimal.valueOf(value));
            groupByResultHolder.setValueForKey(groupKey, sum);
          }
        }
        break;
      case FLOAT:
      case DOUBLE:
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          String value = stringValues[i];
          for (int groupKey : groupKeysArray[i]) {
            BigDecimal sum = getDefaultResult(groupByResultHolder, groupKey);
            sum = sum.add(new BigDecimal(value));
            groupByResultHolder.setValueForKey(groupKey, sum);
          }
        }
        break;
      case BIG_DECIMAL:
        BigDecimal[] bigDecimalValues = blockValSet.getBigDecimalValuesSV();
        for (int i = 0; i < length; i++) {
          BigDecimal value = bigDecimalValues[i];
          for (int groupKey : groupKeysArray[i]) {
            BigDecimal sum = getDefaultResult(groupByResultHolder, groupKey);
            sum = sum.add(value);
            groupByResultHolder.setValueForKey(groupKey, sum);
          }
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          byte[] value = bytesValues[i];
          for (int groupKey : groupKeysArray[i]) {
            BigDecimal sum = getDefaultResult(groupByResultHolder, groupKey);
            sum = sum.add(BigDecimalUtils.deserialize(value));
            groupByResultHolder.setValueForKey(groupKey, sum);
          }
        }
        break;
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  public BigDecimal extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    BigDecimal result = aggregationResultHolder.getResult();
    if (result == null) {
      return _nullMode.nullAtQueryTime() ? null : BigDecimal.ZERO;
    }
    return result;
  }

  @Override
  public BigDecimal extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    BigDecimal result = groupByResultHolder.getResult(groupKey);
    if (result == null) {
      return _nullMode.nullAtQueryTime() ? null : BigDecimal.ZERO;
    }
    return result;
  }

  @Override
  public BigDecimal merge(BigDecimal intermediateResult1, BigDecimal intermediateResult2) {
    if (_nullMode.nullAtQueryTime()) {
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

  public BigDecimal getDefaultResult(AggregationResultHolder aggregationResultHolder) {
    BigDecimal result = aggregationResultHolder.getResult();
    return result != null ? result : BigDecimal.ZERO;
  }

  public BigDecimal getDefaultResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    BigDecimal result = groupByResultHolder.getResult(groupKey);
    return result != null ? result : BigDecimal.ZERO;
  }
}
