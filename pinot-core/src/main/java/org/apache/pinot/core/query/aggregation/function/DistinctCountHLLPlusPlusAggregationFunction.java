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

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants;


public class DistinctCountHLLPlusPlusAggregationFunction
    extends BaseSingleInputAggregationFunction<HyperLogLogPlus, Long> {

  protected final int _normalPrecision;
  protected final int _sparsePrecision;

  public DistinctCountHLLPlusPlusAggregationFunction(List<ExpressionContext> arguments) {
    super(arguments.get(0));
    int numExpressions = arguments.size();
    // This function expects 1 or 3 arguments.
    Preconditions
        .checkArgument(numExpressions == 1 || numExpressions == 3,
            "DistinctCountHLLPlusPlus expects 1 or 3 arguments, got: %s", numExpressions);
    if (arguments.size() == 3) {
      _normalPrecision = Integer.parseInt(arguments.get(1).getLiteral());
      _sparsePrecision = Integer.parseInt(arguments.get(2).getLiteral());
    } else {
      _normalPrecision = CommonConstants.Helix.DEFAULT_HYPERLOGLOGPLUSPLUS_NORMAL_PRECISION;
      _sparsePrecision = CommonConstants.Helix.DEFAULT_HYPERLOGLOGPLUSPLUS_SPARSE_PRECISION;
    }
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTHLLPLUSPLUS;
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
    DataType storedType = blockValSet.getValueType().getStoredType();
    if (storedType != DataType.BYTES) {
      HyperLogLogPlus hyperLogLogPlus = getDefaultHyperLogLogPlus(aggregationResultHolder);
      switch (storedType) {
        case INT:
          int[] intValues = blockValSet.getIntValuesSV();
          for (int i = 0; i < length; i++) {
            hyperLogLogPlus.offer(intValues[i]);
          }
          break;
        case LONG:
          long[] longValues = blockValSet.getLongValuesSV();
          for (int i = 0; i < length; i++) {
            hyperLogLogPlus.offer(longValues[i]);
          }
          break;
        case FLOAT:
          float[] floatValues = blockValSet.getFloatValuesSV();
          for (int i = 0; i < length; i++) {
            hyperLogLogPlus.offer(floatValues[i]);
          }
          break;
        case DOUBLE:
          double[] doubleValues = blockValSet.getDoubleValuesSV();
          for (int i = 0; i < length; i++) {
            hyperLogLogPlus.offer(doubleValues[i]);
          }
          break;
        case STRING:
          String[] stringValues = blockValSet.getStringValuesSV();
          for (int i = 0; i < length; i++) {
            hyperLogLogPlus.offer(stringValues[i]);
          }
          break;
        default:
          throw new IllegalStateException(
              "Illegal data type for DISTINCT_COUNT_HLL_PLUS_PLUS aggregation function: " + storedType);
      }
    } else {
      // Serialized HyperLogLogPlus
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      try {
        HyperLogLogPlus hyperLogLogPlus = aggregationResultHolder.getResult();
        if (hyperLogLogPlus != null) {
          for (int i = 0; i < length; i++) {
            hyperLogLogPlus.merge(ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_PLUS_SER_DE.deserialize(bytesValues[i]));
          }
        } else {
          hyperLogLogPlus = ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_PLUS_SER_DE.deserialize(bytesValues[0]);
          aggregationResultHolder.setValue(hyperLogLogPlus);
          for (int i = 1; i < length; i++) {
            hyperLogLogPlus.merge(ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_PLUS_SER_DE.deserialize(bytesValues[i]));
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while merging HyperLogLogPluss", e);
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          getDefaultHyperLogLogPlus(groupByResultHolder, groupKeyArray[i]).offer(intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          getDefaultHyperLogLogPlus(groupByResultHolder, groupKeyArray[i]).offer(longValues[i]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          getDefaultHyperLogLogPlus(groupByResultHolder, groupKeyArray[i]).offer(floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          getDefaultHyperLogLogPlus(groupByResultHolder, groupKeyArray[i]).offer(doubleValues[i]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          getDefaultHyperLogLogPlus(groupByResultHolder, groupKeyArray[i]).offer(stringValues[i]);
        }
        break;
      case BYTES:
        // Serialized HyperLogLogPlus
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        try {
          for (int i = 0; i < length; i++) {
            HyperLogLogPlus value = ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_PLUS_SER_DE.deserialize(bytesValues[i]);
            int groupKey = groupKeyArray[i];
            HyperLogLogPlus hyperLogLogPlus = groupByResultHolder.getResult(groupKey);
            if (hyperLogLogPlus != null) {
              hyperLogLogPlus.merge(value);
            } else {
              groupByResultHolder.setValueForKey(groupKey, value);
            }
          }
        } catch (Exception e) {
          throw new RuntimeException("Caught exception while merging HyperLogLogPluss", e);
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_HLL_PLUS_PLUS aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          int value = intValues[i];
          for (int groupKey : groupKeysArray[i]) {
            getDefaultHyperLogLogPlus(groupByResultHolder, groupKey).offer(value);
          }
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          long value = longValues[i];
          for (int groupKey : groupKeysArray[i]) {
            getDefaultHyperLogLogPlus(groupByResultHolder, groupKey).offer(value);
          }
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          float value = floatValues[i];
          for (int groupKey : groupKeysArray[i]) {
            getDefaultHyperLogLogPlus(groupByResultHolder, groupKey).offer(value);
          }
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          double value = doubleValues[i];
          for (int groupKey : groupKeysArray[i]) {
            getDefaultHyperLogLogPlus(groupByResultHolder, groupKey).offer(value);
          }
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          String value = stringValues[i];
          for (int groupKey : groupKeysArray[i]) {
            getDefaultHyperLogLogPlus(groupByResultHolder, groupKey).offer(value);
          }
        }
        break;
      case BYTES:
        // Serialized HyperLogLogPlus
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        try {
          for (int i = 0; i < length; i++) {
            HyperLogLogPlus value = ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_PLUS_SER_DE.deserialize(bytesValues[i]);
            for (int groupKey : groupKeysArray[i]) {
              HyperLogLogPlus hyperLogLogPlus = groupByResultHolder.getResult(groupKey);
              if (hyperLogLogPlus != null) {
                hyperLogLogPlus.merge(value);
              } else {
                // Create a new HyperLogLogPlus for the group
                groupByResultHolder
                    .setValueForKey(groupKey,
                        ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_PLUS_SER_DE.deserialize(bytesValues[i]));
              }
            }
          }
        } catch (Exception e) {
          throw new RuntimeException("Caught exception while merging HyperLogLogPluss", e);
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_HLL_PLUS_PLUS aggregation function: " + storedType);
    }
  }

  @Override
  public HyperLogLogPlus extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    HyperLogLogPlus hyperLogLogPlus = aggregationResultHolder.getResult();
    if (hyperLogLogPlus == null) {
      return new HyperLogLogPlus(_normalPrecision, _sparsePrecision);
    } else {
      return hyperLogLogPlus;
    }
  }

  @Override
  public HyperLogLogPlus extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    HyperLogLogPlus hyperLogLogPlus = groupByResultHolder.getResult(groupKey);
    if (hyperLogLogPlus == null) {
      return new HyperLogLogPlus(_normalPrecision, _sparsePrecision);
    } else {
      return hyperLogLogPlus;
    }
  }

  @Override
  public HyperLogLogPlus merge(HyperLogLogPlus intermediateResult1, HyperLogLogPlus intermediateResult2) {
    if (intermediateResult1.sizeof() != intermediateResult2.sizeof()) {
      if (intermediateResult1.cardinality() == 0) {
        return intermediateResult2;
      } else {
        Preconditions
            .checkState(intermediateResult2.cardinality() == 0, "Cannot merge HyperLogLogPlus of different sizes");
        return intermediateResult1;
      }
    }
    try {
      intermediateResult1.merge(intermediateResult2);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while merging HyperLogLogPluss", e);
    }
    return intermediateResult1;
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return false;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.LONG;
  }

  @Override
  public Long extractFinalResult(HyperLogLogPlus intermediateResult) {
    return intermediateResult.cardinality();
  }

  /**
   * Returns the HyperLogLogPlus from the result holder or creates a new one with default precision values.
   *
   * @param aggregationResultHolder Result holder
   * @return HyperLogLogPlus from the result holder
   */
  protected HyperLogLogPlus getDefaultHyperLogLogPlus(AggregationResultHolder aggregationResultHolder) {
    HyperLogLogPlus hyperLogLogPlus = aggregationResultHolder.getResult();
    if (hyperLogLogPlus == null) {
      hyperLogLogPlus = new HyperLogLogPlus(_normalPrecision, _sparsePrecision);
      aggregationResultHolder.setValue(hyperLogLogPlus);
    }
    return hyperLogLogPlus;
  }

  /**
   * Returns the HyperLogLogPlus for the given group key if exists, or creates a new one with default precision.
   *
   * @param groupByResultHolder Result holder
   * @param groupKey Group key for which to return the HyperLogLogPlus
   * @return HyperLogLogPlus for the group key
   */
  protected HyperLogLogPlus getDefaultHyperLogLogPlus(GroupByResultHolder groupByResultHolder, int groupKey) {
    HyperLogLogPlus hyperLogLogPlus = groupByResultHolder.getResult(groupKey);
    if (hyperLogLogPlus == null) {
      hyperLogLogPlus = new HyperLogLogPlus(_normalPrecision, _sparsePrecision);
      groupByResultHolder.setValueForKey(groupKey, hyperLogLogPlus);
    }
    return hyperLogLogPlus;
  }
}
