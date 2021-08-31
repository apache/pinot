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
import com.google.zetasketch.HyperLogLogPlusPlus;
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


public class DistinctCountHLLPlusPlusAggregationFunction extends BaseSingleInputAggregationFunction<HyperLogLogPlusPlus, Long> {

  public DistinctCountHLLPlusPlusAggregationFunction(List<ExpressionContext> arguments) {
    super(arguments.get(0));
    int numExpressions = arguments.size();
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
      HyperLogLogPlusPlus hyperLogLog = getDefaultHyperLogLogPlusPlus(aggregationResultHolder, storedType);
      switch (storedType) {
        case INT:
          int[] intValues = blockValSet.getIntValuesSV();
          for (int i = 0; i < length; i++) {
            hyperLogLog.add(intValues[i]);
          }
          break;
        case LONG:
          long[] longValues = blockValSet.getLongValuesSV();
          for (int i = 0; i < length; i++) {
            hyperLogLog.add(longValues[i]);
          }
          break;
        case FLOAT:
          float[] floatValues = blockValSet.getFloatValuesSV();
          for (int i = 0; i < length; i++) {
            hyperLogLog.add(floatValues[i]);
          }
          break;
        case DOUBLE:
          double[] doubleValues = blockValSet.getDoubleValuesSV();
          for (int i = 0; i < length; i++) {
            hyperLogLog.add(doubleValues[i]);
          }
          break;
        case STRING:
          String[] stringValues = blockValSet.getStringValuesSV();
          for (int i = 0; i < length; i++) {
            hyperLogLog.add(stringValues[i]);
          }
          break;
        default:
          throw new IllegalStateException(
              "Illegal data type for DISTINCT_COUNT_HLL_PLUS_PLUS aggregation function: " + storedType);
      }
    } else {
      // Serialized HyperLogLogPlusPlus
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      try {
        HyperLogLogPlusPlus hyperLogLog = aggregationResultHolder.getResult();
        if (hyperLogLog != null) {
          for (int i = 0; i < length; i++) {
            hyperLogLog.merge(ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_PLUS_SER_DE.deserialize(bytesValues[i]));
          }
        } else {
          hyperLogLog = ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_PLUS_SER_DE.deserialize(bytesValues[0]);
          aggregationResultHolder.setValue(hyperLogLog);
          for (int i = 1; i < length; i++) {
            hyperLogLog.merge(ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_PLUS_SER_DE.deserialize(bytesValues[i]));
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while merging HyperLogLogPlusPluss", e);
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
          getDefaultHyperLogLogPlusPlus(groupByResultHolder, groupKeyArray[i], storedType).add(intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          getDefaultHyperLogLogPlusPlus(groupByResultHolder, groupKeyArray[i], storedType).add(longValues[i]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          getDefaultHyperLogLogPlusPlus(groupByResultHolder, groupKeyArray[i], storedType).add(floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          getDefaultHyperLogLogPlusPlus(groupByResultHolder, groupKeyArray[i], storedType).add(doubleValues[i]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          getDefaultHyperLogLogPlusPlus(groupByResultHolder, groupKeyArray[i], storedType).add(stringValues[i]);
        }
        break;
      case BYTES:
        // Serialized HyperLogLogPlusPlus
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        try {
          for (int i = 0; i < length; i++) {
            HyperLogLogPlusPlus value = ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_PLUS_SER_DE.deserialize(bytesValues[i]);
            int groupKey = groupKeyArray[i];
            HyperLogLogPlusPlus hyperLogLog = groupByResultHolder.getResult(groupKey);
            if (hyperLogLog != null) {
              hyperLogLog.merge(value);
            } else {
              groupByResultHolder.setValueForKey(groupKey, value);
            }
          }
        } catch (Exception e) {
          throw new RuntimeException("Caught exception while merging HyperLogLogPlusPluss", e);
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT_HLL_PLUS_PLUS aggregation function: " + storedType);
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
            getDefaultHyperLogLogPlusPlus(groupByResultHolder, groupKey, storedType).add(value);
          }
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          long value = longValues[i];
          for (int groupKey : groupKeysArray[i]) {
            getDefaultHyperLogLogPlusPlus(groupByResultHolder, groupKey, storedType).add(value);
          }
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          float value = floatValues[i];
          for (int groupKey : groupKeysArray[i]) {
            getDefaultHyperLogLogPlusPlus(groupByResultHolder, groupKey, storedType).add(value);
          }
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          double value = doubleValues[i];
          for (int groupKey : groupKeysArray[i]) {
            getDefaultHyperLogLogPlusPlus(groupByResultHolder, groupKey, storedType).add(value);
          }
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          String value = stringValues[i];
          for (int groupKey : groupKeysArray[i]) {
            getDefaultHyperLogLogPlusPlus(groupByResultHolder, groupKey, storedType).add(value);
          }
        }
        break;
      case BYTES:
        // Serialized HyperLogLogPlusPlus
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        try {
          for (int i = 0; i < length; i++) {
            HyperLogLogPlusPlus value = ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_PLUS_SER_DE.deserialize(bytesValues[i]);
            for (int groupKey : groupKeysArray[i]) {
              HyperLogLogPlusPlus hyperLogLog = groupByResultHolder.getResult(groupKey);
              if (hyperLogLog != null) {
                hyperLogLog.merge(value);
              } else {
                // Create a new HyperLogLogPlusPlus for the group
                groupByResultHolder
                    .setValueForKey(groupKey, ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_PLUS_SER_DE.deserialize(bytesValues[i]));
              }
            }
          }
        } catch (Exception e) {
          throw new RuntimeException("Caught exception while merging HyperLogLogPlusPluss", e);
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT_HLL_PLUS_PLUS aggregation function: " + storedType);
    }
  }

  @Override
  public HyperLogLogPlusPlus extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    HyperLogLogPlusPlus hyperLogLog = aggregationResultHolder.getResult();
    if (hyperLogLog == null) {
      return new HyperLogLogPlusPlus.Builder().buildForStrings();
    } else {
      return hyperLogLog;
    }
  }

  @Override
  public HyperLogLogPlusPlus extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    HyperLogLogPlusPlus hyperLogLog = groupByResultHolder.getResult(groupKey);
    if (hyperLogLog == null) {
      return new HyperLogLogPlusPlus.Builder().buildForStrings();
    } else {
      return hyperLogLog;
    }
  }

  @Override
  public HyperLogLogPlusPlus merge(HyperLogLogPlusPlus intermediateResult1, HyperLogLogPlusPlus intermediateResult2) {
    // Can happen when aggregating serialized HyperLogLogPlusPlus with non-default log2m
    if (intermediateResult1.numValues() != intermediateResult2.numValues()) {
      if (intermediateResult1.result() == 0) {
        return intermediateResult2;
      } else {
        Preconditions
            .checkState(intermediateResult2.result() == 0, "Cannot merge HyperLogLogPlusPlus of different sizes");
        return intermediateResult1;
      }
    }
    try {
      intermediateResult1.merge(intermediateResult2);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while merging HyperLogLogPlusPluss", e);
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
  public Long extractFinalResult(HyperLogLogPlusPlus intermediateResult) {
    return intermediateResult.result();
  }

  /**
   * Returns the HyperLogLogPlusPlus from the result holder or creates a new one with default log2m if it does not exist.
   *
   * @param aggregationResultHolder Result holder
   * @param storedType
   * @return HyperLogLogPlusPlus from the result holder
   */
  protected HyperLogLogPlusPlus getDefaultHyperLogLogPlusPlus(AggregationResultHolder aggregationResultHolder,
      DataType storedType) {
    HyperLogLogPlusPlus hyperLogLog = aggregationResultHolder.getResult();
    if (hyperLogLog == null) {
      switch (storedType) {
        case INT:
          hyperLogLog = new HyperLogLogPlusPlus.Builder().buildForIntegers();
          break;
        case LONG:
          hyperLogLog = new HyperLogLogPlusPlus.Builder().buildForLongs();
          break;
        case FLOAT:
        case DOUBLE:
        case STRING:
          hyperLogLog = new HyperLogLogPlusPlus.Builder().buildForStrings();
          break;
        default:
          throw new IllegalStateException(
              "Illegal data type for DISTINCT_COUNT_HLL_PLUS_PLUS aggregation function: " + storedType);
      }
      aggregationResultHolder.setValue(hyperLogLog);
    }
    return hyperLogLog;
  }

  /**
   * Returns the HyperLogLogPlusPlus for the given group key if exists, or creates a new one with default log2m.
   *
   * @param groupByResultHolder Result holder
   * @param groupKey Group key for which to return the HyperLogLogPlusPlus
   * @return HyperLogLogPlusPlus for the group key
   */
  protected HyperLogLogPlusPlus getDefaultHyperLogLogPlusPlus(GroupByResultHolder groupByResultHolder, int groupKey, DataType storedType) {
    HyperLogLogPlusPlus hyperLogLog = groupByResultHolder.getResult(groupKey);
    if (hyperLogLog == null) {
      switch (storedType) {
        case INT:
          hyperLogLog = new HyperLogLogPlusPlus.Builder().buildForIntegers();
          break;
        case LONG:
          hyperLogLog = new HyperLogLogPlusPlus.Builder().buildForLongs();
          break;
        case FLOAT:
        case DOUBLE:
        case STRING:
          hyperLogLog = new HyperLogLogPlusPlus.Builder().buildForStrings();
          break;
        default:
          throw new IllegalStateException(
              "Illegal data type for DISTINCT_COUNT_HLL_PLUS_PLUS aggregation function: " + storedType);
      }
      groupByResultHolder.setValueForKey(groupKey, hyperLogLog);
    }
    return hyperLogLog;
  }
}
