/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.aggregation.function;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.utils.DataSchema;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.ObjectSerDeUtils;
import com.linkedin.pinot.core.query.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import javax.annotation.Nonnull;


public class DistinctCountHLLAggregationFunction implements AggregationFunction<HyperLogLog, Long> {
  public static final int DEFAULT_LOG2M = 8;

  @Nonnull
  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTHLL;
  }

  @Nonnull
  @Override
  public String getColumnName(@Nonnull String column) {
    return AggregationFunctionType.DISTINCTCOUNTHLL.getName() + "_" + column;
  }

  @Override
  public void accept(@Nonnull AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  @Nonnull
  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Nonnull
  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, @Nonnull AggregationResultHolder aggregationResultHolder,
      @Nonnull BlockValSet... blockValSets) {
    HyperLogLog hyperLogLog = getHyperLogLog(aggregationResultHolder);

    FieldSpec.DataType valueType = blockValSets[0].getValueType();
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
      case BYTES:
        // Serialized HyperLogLog
        byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
        try {
          for (int i = 0; i < length; i++) {
            hyperLogLog.addAll(ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(bytesValues[i]));
          }
        } catch (Exception e) {
          throw new RuntimeException("Caught exception while aggregating HyperLogLog", e);
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT_HLL aggregation function: " + valueType);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    FieldSpec.DataType valueType = blockValSets[0].getValueType();
    switch (valueType) {
      case INT:
        int[] intValues = blockValSets[0].getIntValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKey(groupByResultHolder, groupKeyArray[i], intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSets[0].getLongValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKey(groupByResultHolder, groupKeyArray[i], longValues[i]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSets[0].getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKey(groupByResultHolder, groupKeyArray[i], floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSets[0].getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKey(groupByResultHolder, groupKeyArray[i], doubleValues[i]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSets[0].getStringValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKey(groupByResultHolder, groupKeyArray[i], stringValues[i]);
        }
        break;
      case BYTES:
        // Serialized HyperLogLog
        byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
        try {
          for (int i = 0; i < length; i++) {
            setValueForGroupKey(groupByResultHolder, groupKeyArray[i],
                ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(bytesValues[i]));
          }
        } catch (Exception e) {
          throw new RuntimeException("Caught exception while aggregating HyperLogLog", e);
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT_HLL aggregation function: " + valueType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    FieldSpec.DataType valueType = blockValSets[0].getValueType();
    switch (valueType) {
      case INT:
        int[] intValues = blockValSets[0].getIntValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSets[0].getLongValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], longValues[i]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSets[0].getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSets[0].getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], doubleValues[i]);
        }
        break;
      case STRING:
        String[] singleValues = blockValSets[0].getStringValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], singleValues[i]);
        }
        break;
      case BYTES:
        // Serialized HyperLogLog
        byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
        try {
          for (int i = 0; i < length; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i],
                ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(bytesValues[i]));
          }
        } catch (Exception e) {
          throw new RuntimeException("Caught exception while aggregating HyperLogLog", e);
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT_HLL aggregation function: " + valueType);
    }
  }

  @Nonnull
  @Override
  public HyperLogLog extractAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder) {
    HyperLogLog hyperLogLog = aggregationResultHolder.getResult();
    if (hyperLogLog == null) {
      return new HyperLogLog(DEFAULT_LOG2M);
    } else {
      return hyperLogLog;
    }
  }

  @Nonnull
  @Override
  public HyperLogLog extractGroupByResult(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    HyperLogLog hyperLogLog = groupByResultHolder.getResult(groupKey);
    if (hyperLogLog == null) {
      return new HyperLogLog(DEFAULT_LOG2M);
    } else {
      return hyperLogLog;
    }
  }

  @Nonnull
  @Override
  public HyperLogLog merge(@Nonnull HyperLogLog intermediateResult1, @Nonnull HyperLogLog intermediateResult2) {
    try {
      intermediateResult1.addAll(intermediateResult2);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while merging HyperLogLog", e);
    }
    return intermediateResult1;
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return false;
  }

  @Nonnull
  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Nonnull
  @Override
  public Long extractFinalResult(@Nonnull HyperLogLog intermediateResult) {
    return intermediateResult.cardinality();
  }

  /**
   * Helper method to set value for a groupKey into the result holder.
   *
   * @param groupByResultHolder Result holder
   * @param groupKey Group-key for which to set the value
   * @param value Value for the group key
   */
  private static void setValueForGroupKey(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey,
      Object value) {
    HyperLogLog hyperLogLog = getHyperLogLog(groupByResultHolder, groupKey);
    hyperLogLog.offer(value);
  }

  /**
   * Helper method to set HyperLogLog value for a groupKey into the result holder.
   *
   * @param groupByResultHolder Result holder
   * @param groupKey Group-key for which to set the value
   * @param value HyperLogLog value for the group key
   */
  private static void setValueForGroupKey(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey,
      HyperLogLog value) throws CardinalityMergeException {
    HyperLogLog hyperLogLog = getHyperLogLog(groupByResultHolder, groupKey);
    hyperLogLog.addAll(value);
  }

  /**
   * Helper method to set value for a given array of group keys.
   *
   * @param groupByResultHolder Result Holder
   * @param groupKeys Group keys for which to set the value
   * @param value Value to set
   */
  private static void setValueForGroupKeys(@Nonnull GroupByResultHolder groupByResultHolder, int[] groupKeys,
      Object value) {
    for (int groupKey : groupKeys) {
      setValueForGroupKey(groupByResultHolder, groupKey, value);
    }
  }

  /**
   * Helper method to set HyperLogLog value for a given array of group keys.
   *
   * @param groupByResultHolder Result Holder
   * @param groupKeys Group keys for which to set the value
   * @param value HyperLogLog value to set
   */
  private static void setValueForGroupKeys(@Nonnull GroupByResultHolder groupByResultHolder, int[] groupKeys,
      HyperLogLog value) throws CardinalityMergeException {
    for (int groupKey : groupKeys) {
      setValueForGroupKey(groupByResultHolder, groupKey, value);
    }
  }

  /**
   * Returns the HyperLogLog from the result holder or creates a new one if it does not exist.
   *
   * @param aggregationResultHolder Result holder
   * @return HyperLogLog from the result holder
   */
  protected static HyperLogLog getHyperLogLog(@Nonnull AggregationResultHolder aggregationResultHolder) {
    HyperLogLog hyperLogLog = aggregationResultHolder.getResult();
    if (hyperLogLog == null) {
      hyperLogLog = new HyperLogLog(DEFAULT_LOG2M);
      aggregationResultHolder.setValue(hyperLogLog);
    }
    return hyperLogLog;
  }

  /**
   * Returns the HyperLogLog for the given group key. If one does not exist, creates a new one and returns that.
   *
   * @param groupByResultHolder Result holder
   * @param groupKey Group key for which to return the HyperLogLog
   * @return HyperLogLog for the group key
   */
  protected static HyperLogLog getHyperLogLog(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    HyperLogLog hyperLogLog = groupByResultHolder.getResult(groupKey);
    if (hyperLogLog == null) {
      hyperLogLog = new HyperLogLog(DEFAULT_LOG2M);
      groupByResultHolder.setValueForKey(groupKey, hyperLogLog);
    }
    return hyperLogLog;
  }
}
