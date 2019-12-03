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

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.google.common.base.Preconditions;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;


public class DistinctCountHLLAggregationFunction implements AggregationFunction<HyperLogLog, Long> {
  public static final int DEFAULT_LOG2M = 8;

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTHLL;
  }

  @Override
  public void accept(AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
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
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder, BlockValSet... blockValSets) {
    DataType valueType = blockValSets[0].getValueType();
    if (valueType != DataType.BYTES) {
      HyperLogLog hyperLogLog = getDefaultHyperLogLog(aggregationResultHolder);
      switch (valueType) {
        case INT:
          int[] intValues = blockValSets[0].getIntValuesSV();
          for (int i = 0; i < length; i++) {
            hyperLogLog.offer(intValues[i]);
          }
          break;
        case LONG:
          long[] longValues = blockValSets[0].getLongValuesSV();
          for (int i = 0; i < length; i++) {
            hyperLogLog.offer(longValues[i]);
          }
          break;
        case FLOAT:
          float[] floatValues = blockValSets[0].getFloatValuesSV();
          for (int i = 0; i < length; i++) {
            hyperLogLog.offer(floatValues[i]);
          }
          break;
        case DOUBLE:
          double[] doubleValues = blockValSets[0].getDoubleValuesSV();
          for (int i = 0; i < length; i++) {
            hyperLogLog.offer(doubleValues[i]);
          }
          break;
        case STRING:
          String[] stringValues = blockValSets[0].getStringValuesSV();
          for (int i = 0; i < length; i++) {
            hyperLogLog.offer(stringValues[i]);
          }
          break;
        default:
          throw new IllegalStateException(
              "Illegal data type for DISTINCT_COUNT_HLL aggregation function: " + valueType);
      }
    } else {
      // Serialized HyperLogLog
      byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
      try {
        HyperLogLog hyperLogLog = aggregationResultHolder.getResult();
        if (hyperLogLog != null) {
          for (int i = 0; i < length; i++) {
            hyperLogLog.addAll(ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(bytesValues[i]));
          }
        } else {
          hyperLogLog = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(bytesValues[0]);
          aggregationResultHolder.setValue(hyperLogLog);
          for (int i = 1; i < length; i++) {
            hyperLogLog.addAll(ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(bytesValues[i]));
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while merging HyperLogLogs", e);
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      BlockValSet... blockValSets) {
    DataType valueType = blockValSets[0].getValueType();
    switch (valueType) {
      case INT:
        int[] intValues = blockValSets[0].getIntValuesSV();
        for (int i = 0; i < length; i++) {
          getDefaultHyperLogLog(groupByResultHolder, groupKeyArray[i]).offer(intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSets[0].getLongValuesSV();
        for (int i = 0; i < length; i++) {
          getDefaultHyperLogLog(groupByResultHolder, groupKeyArray[i]).offer(longValues[i]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSets[0].getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          getDefaultHyperLogLog(groupByResultHolder, groupKeyArray[i]).offer(floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSets[0].getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          getDefaultHyperLogLog(groupByResultHolder, groupKeyArray[i]).offer(doubleValues[i]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSets[0].getStringValuesSV();
        for (int i = 0; i < length; i++) {
          getDefaultHyperLogLog(groupByResultHolder, groupKeyArray[i]).offer(stringValues[i]);
        }
        break;
      case BYTES:
        // Serialized HyperLogLog
        byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
        try {
          for (int i = 0; i < length; i++) {
            HyperLogLog value = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(bytesValues[i]);
            int groupKey = groupKeyArray[i];
            HyperLogLog hyperLogLog = groupByResultHolder.getResult(groupKey);
            if (hyperLogLog != null) {
              hyperLogLog.addAll(value);
            } else {
              groupByResultHolder.setValueForKey(groupKey, value);
            }
          }
        } catch (Exception e) {
          throw new RuntimeException("Caught exception while merging HyperLogLogs", e);
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT_HLL aggregation function: " + valueType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      BlockValSet... blockValSets) {
    DataType valueType = blockValSets[0].getValueType();
    switch (valueType) {
      case INT:
        int[] intValues = blockValSets[0].getIntValuesSV();
        for (int i = 0; i < length; i++) {
          int value = intValues[i];
          for (int groupKey : groupKeysArray[i]) {
            getDefaultHyperLogLog(groupByResultHolder, groupKey).offer(value);
          }
        }
        break;
      case LONG:
        long[] longValues = blockValSets[0].getLongValuesSV();
        for (int i = 0; i < length; i++) {
          long value = longValues[i];
          for (int groupKey : groupKeysArray[i]) {
            getDefaultHyperLogLog(groupByResultHolder, groupKey).offer(value);
          }
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSets[0].getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          float value = floatValues[i];
          for (int groupKey : groupKeysArray[i]) {
            getDefaultHyperLogLog(groupByResultHolder, groupKey).offer(value);
          }
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSets[0].getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          double value = doubleValues[i];
          for (int groupKey : groupKeysArray[i]) {
            getDefaultHyperLogLog(groupByResultHolder, groupKey).offer(value);
          }
        }
        break;
      case STRING:
        String[] stringValues = blockValSets[0].getStringValuesSV();
        for (int i = 0; i < length; i++) {
          String value = stringValues[i];
          for (int groupKey : groupKeysArray[i]) {
            getDefaultHyperLogLog(groupByResultHolder, groupKey).offer(value);
          }
        }
        break;
      case BYTES:
        // Serialized HyperLogLog
        byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
        try {
          for (int i = 0; i < length; i++) {
            HyperLogLog value = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(bytesValues[i]);
            for (int groupKey : groupKeysArray[i]) {
              HyperLogLog hyperLogLog = groupByResultHolder.getResult(groupKey);
              if (hyperLogLog != null) {
                hyperLogLog.addAll(value);
              } else {
                // Create a new HyperLogLog for the group
                groupByResultHolder
                    .setValueForKey(groupKey, ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(bytesValues[i]));
              }
            }
          }
        } catch (Exception e) {
          throw new RuntimeException("Caught exception while merging HyperLogLogs", e);
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT_HLL aggregation function: " + valueType);
    }
  }

  @Override
  public HyperLogLog extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    HyperLogLog hyperLogLog = aggregationResultHolder.getResult();
    if (hyperLogLog == null) {
      return new HyperLogLog(DEFAULT_LOG2M);
    } else {
      return hyperLogLog;
    }
  }

  @Override
  public HyperLogLog extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    HyperLogLog hyperLogLog = groupByResultHolder.getResult(groupKey);
    if (hyperLogLog == null) {
      return new HyperLogLog(DEFAULT_LOG2M);
    } else {
      return hyperLogLog;
    }
  }

  @Override
  public HyperLogLog merge(HyperLogLog intermediateResult1, HyperLogLog intermediateResult2) {
    // Can happen when aggregating serialized HyperLogLog with non-default log2m
    if (intermediateResult1.sizeof() != intermediateResult2.sizeof()) {
      if (intermediateResult1.cardinality() == 0) {
        return intermediateResult2;
      } else {
        Preconditions
            .checkState(intermediateResult2.cardinality() == 0, "Cannot merge HyperLogLogs of different sizes");
        return intermediateResult1;
      }
    }
    try {
      intermediateResult1.addAll(intermediateResult2);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while merging HyperLogLogs", e);
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
  public Long extractFinalResult(HyperLogLog intermediateResult) {
    return intermediateResult.cardinality();
  }

  /**
   * Returns the HyperLogLog from the result holder or creates a new one with default log2m if it does not exist.
   *
   * @param aggregationResultHolder Result holder
   * @return HyperLogLog from the result holder
   */
  protected static HyperLogLog getDefaultHyperLogLog(AggregationResultHolder aggregationResultHolder) {
    HyperLogLog hyperLogLog = aggregationResultHolder.getResult();
    if (hyperLogLog == null) {
      hyperLogLog = new HyperLogLog(DEFAULT_LOG2M);
      aggregationResultHolder.setValue(hyperLogLog);
    }
    return hyperLogLog;
  }

  /**
   * Returns the HyperLogLog for the given group key if exists, or creates a new one with default log2m.
   *
   * @param groupByResultHolder Result holder
   * @param groupKey Group key for which to return the HyperLogLog
   * @return HyperLogLog for the group key
   */
  protected static HyperLogLog getDefaultHyperLogLog(GroupByResultHolder groupByResultHolder, int groupKey) {
    HyperLogLog hyperLogLog = groupByResultHolder.getResult(groupKey);
    if (hyperLogLog == null) {
      hyperLogLog = new HyperLogLog(DEFAULT_LOG2M);
      groupByResultHolder.setValueForKey(groupKey, hyperLogLog);
    }
    return hyperLogLog;
  }
}
