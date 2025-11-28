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
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.local.customobject.AvgPrecisionPair;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * This function is used for BigDecimal average calculations.
 * It supports the average aggregation using precision and scale.
 * <p>The function can be used as AVGPRECISION(expression, precision, scale, roundingMode)
 * <p>Following arguments are supported:
 * <ul>
 *   <li>Expression: expression that contains the values to be averaged, can be serialized BigDecimal objects</li>
 *   <li>Precision (optional): precision to be set to the final result</li>
 *   <li>Scale (optional): scale to be set to the final result</li>
 *   <li>RoundingMode (optional): rounding mode to be used (default: HALF_EVEN)</li>
 * </ul>
 */
public class AvgPrecisionAggregationFunction
    extends NullableSingleInputAggregationFunction<AvgPrecisionPair, BigDecimal> {
  private final Integer _precision;
  private final Integer _scale;
  private final RoundingMode _roundingMode;

  public AvgPrecisionAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(arguments.get(0), nullHandlingEnabled);

    int numArguments = arguments.size();
    Preconditions.checkArgument(numArguments <= 4, "AvgPrecision expects at most 4 arguments, got: %s", numArguments);

    if (numArguments > 1) {
      _precision = arguments.get(1).getLiteral().getIntValue();
      if (numArguments > 2) {
        _scale = arguments.get(2).getLiteral().getIntValue();
        if (numArguments > 3) {
          String roundingModeStr = arguments.get(3).getLiteral().getStringValue();
          _roundingMode = RoundingMode.valueOf(roundingModeStr.toUpperCase());
        } else {
          _roundingMode = RoundingMode.HALF_EVEN;
        }
      } else {
        _scale = null;
        _roundingMode = RoundingMode.HALF_EVEN;
      }
    } else {
      _precision = null;
      _scale = null;
      _roundingMode = RoundingMode.HALF_EVEN;
    }
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.AVGPRECISION;
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

    AvgPrecisionPair avgPair;
    switch (blockValSet.getValueType().getStoredType()) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();

        avgPair = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          AvgPrecisionPair innerPair = acum == null ? new AvgPrecisionPair() : acum;
          for (int i = from; i < to; i++) {
            innerPair.apply(BigDecimal.valueOf(intValues[i]));
          }
          return innerPair;
        });

        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();

        avgPair = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          AvgPrecisionPair innerPair = acum == null ? new AvgPrecisionPair() : acum;
          for (int i = from; i < to; i++) {
            innerPair.apply(BigDecimal.valueOf(longValues[i]));
          }
          return innerPair;
        });

        break;
      case FLOAT:
      case DOUBLE:
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();

        avgPair = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          AvgPrecisionPair innerPair = acum == null ? new AvgPrecisionPair() : acum;
          for (int i = from; i < to; i++) {
            innerPair.apply(new BigDecimal(stringValues[i]));
          }
          return innerPair;
        });

        break;
      case BIG_DECIMAL:
        BigDecimal[] bigDecimalValues = blockValSet.getBigDecimalValuesSV();

        avgPair = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
          AvgPrecisionPair innerPair = acum == null ? new AvgPrecisionPair() : acum;
          for (int i = from; i < to; i++) {
            innerPair.apply(bigDecimalValues[i]);
          }
          return innerPair;
        });

        break;
      case BYTES:
        // Serialized AvgPrecisionPair
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        avgPair = new AvgPrecisionPair();
        for (int i = 0; i < length; i++) {
          AvgPrecisionPair value = ObjectSerDeUtils.AVG_PRECISION_PAIR_SER_DE.deserialize(bytesValues[i]);
          avgPair.apply(value);
        }
        break;
      default:
        throw new IllegalStateException("Unsupported value type: " + blockValSet.getValueType());
    }
    updateAggregationResult(aggregationResultHolder, avgPair);
  }

  protected void updateAggregationResult(AggregationResultHolder aggregationResultHolder, AvgPrecisionPair avgPair) {
    if (_nullHandlingEnabled) {
      if (avgPair != null && avgPair.getCount() > 0) {
        AvgPrecisionPair otherPair = aggregationResultHolder.getResult();
        if (otherPair == null) {
          aggregationResultHolder.setValue(avgPair);
        } else {
          otherPair.apply(avgPair);
        }
      }
    } else {
      if (avgPair == null) {
        avgPair = new AvgPrecisionPair();
      }
      AvgPrecisionPair otherPair = aggregationResultHolder.getResult();
      if (otherPair == null) {
        aggregationResultHolder.setValue(avgPair);
      } else {
        otherPair.apply(avgPair);
      }
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
        // Serialized AvgPrecisionPair
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          AvgPrecisionPair avgPair = ObjectSerDeUtils.AVG_PRECISION_PAIR_SER_DE.deserialize(bytesValues[i]);
          updateGroupByResult(groupKeyArray[i], groupByResultHolder, avgPair);
        }
        break;
      default:
        throw new IllegalStateException("Unsupported value type: " + blockValSet.getValueType());
    }
  }

  private void updateGroupByResult(int groupKey, GroupByResultHolder groupByResultHolder, BigDecimal value) {
    AvgPrecisionPair avgPair = groupByResultHolder.getResult(groupKey);
    if (avgPair == null) {
      avgPair = new AvgPrecisionPair();
      groupByResultHolder.setValueForKey(groupKey, avgPair);
    }
    avgPair.apply(value);
  }

  private void updateGroupByResult(int groupKey, GroupByResultHolder groupByResultHolder, AvgPrecisionPair value) {
    AvgPrecisionPair avgPair = groupByResultHolder.getResult(groupKey);
    if (avgPair == null) {
      groupByResultHolder.setValueForKey(groupKey, value);
    } else {
      avgPair.apply(value);
    }
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
        // Serialized AvgPrecisionPair
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          AvgPrecisionPair avgPair = ObjectSerDeUtils.AVG_PRECISION_PAIR_SER_DE.deserialize(bytesValues[i]);
          for (int groupKey : groupKeysArray[i]) {
            updateGroupByResult(groupKey, groupByResultHolder, avgPair);
          }
        }
        break;
      default:
        throw new IllegalStateException("Unsupported value type: " + blockValSet.getValueType());
    }
  }

  @Override
  public AvgPrecisionPair extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    AvgPrecisionPair result = aggregationResultHolder.getResult();
    if (result == null) {
      return _nullHandlingEnabled ? null : new AvgPrecisionPair();
    }
    return result;
  }

  @Override
  public AvgPrecisionPair extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    AvgPrecisionPair result = groupByResultHolder.getResult(groupKey);
    if (result == null) {
      return _nullHandlingEnabled ? null : new AvgPrecisionPair();
    }
    return result;
  }

  @Override
  public AvgPrecisionPair merge(AvgPrecisionPair intermediateResult1, AvgPrecisionPair intermediateResult2) {
    if (_nullHandlingEnabled) {
      if (intermediateResult1 == null) {
        return intermediateResult2;
      }
      if (intermediateResult2 == null) {
        return intermediateResult1;
      }
    }
    intermediateResult1.apply(intermediateResult2);
    return intermediateResult1;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(AvgPrecisionPair avgPrecisionPair) {
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.AvgPrecisionPair.getValue(),
        ObjectSerDeUtils.AVG_PRECISION_PAIR_SER_DE.serialize(avgPrecisionPair));
  }

  @Override
  public AvgPrecisionPair deserializeIntermediateResult(CustomObject customObject) {
    return ObjectSerDeUtils.AVG_PRECISION_PAIR_SER_DE.deserialize(customObject.getBuffer());
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.STRING;
  }

  @Override
  public BigDecimal extractFinalResult(AvgPrecisionPair intermediateResult) {
    if (intermediateResult == null || intermediateResult.getCount() == 0) {
      return null;
    }

    BigDecimal sum = intermediateResult.getSum();
    long count = intermediateResult.getCount();

    MathContext mathContext = _precision != null
        ? new MathContext(_precision, _roundingMode)
        : MathContext.DECIMAL128;

    BigDecimal average = sum.divide(BigDecimal.valueOf(count), mathContext);
    return _scale != null
        ? average.setScale(_scale, _roundingMode)
        : average;
  }
}
