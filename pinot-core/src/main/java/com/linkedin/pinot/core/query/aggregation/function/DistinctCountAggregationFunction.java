/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.query.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import javax.annotation.Nonnull;


public class DistinctCountAggregationFunction implements AggregationFunction<IntOpenHashSet, Integer> {
  private static final String NAME = AggregationFunctionFactory.AggregationFunctionType.DISTINCTCOUNT.getName();

  @Nonnull
  @Override
  public String getName() {
    return NAME;
  }

  @Nonnull
  @Override
  public String getColumnName(@Nonnull String[] columns) {
    return NAME + "_" + columns[0];
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
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity, int trimSize) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity, trimSize);
  }

  @Override
  public void aggregate(int length, @Nonnull AggregationResultHolder aggregationResultHolder,
      @Nonnull BlockValSet... blockValSets) {
    IntOpenHashSet valueSet = aggregationResultHolder.getResult();
    if (valueSet == null) {
      valueSet = new IntOpenHashSet();
      aggregationResultHolder.setValue(valueSet);
    }

    FieldSpec.DataType valueType = blockValSets[0].getValueType();
    switch (valueType) {
      case INT:
        int[] intValues = blockValSets[0].getIntValuesSV();
        for (int i = 0; i < length; i++) {
          valueSet.add(intValues[i]);
        }
        break;

      case LONG:
        long[] longValues = blockValSets[0].getLongValuesSV();
        for (int i = 0; i < length; i++) {
          valueSet.add(Long.valueOf(longValues[i]).hashCode());
        }
        break;

      case FLOAT:
        float[] floatValues = blockValSets[0].getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          valueSet.add(Float.valueOf(floatValues[i]).hashCode());
        }
        break;

      case DOUBLE:
        double[] doubleValues = blockValSets[0].getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          valueSet.add(Double.valueOf(doubleValues[i]).hashCode());
        }
        break;

      case STRING:
        String[] stringValues = blockValSets[0].getStringValuesSV();
        for (int i = 0; i < length; i++) {
          valueSet.add(stringValues[i].hashCode());
        }
        break;

      default:
        throw new IllegalArgumentException("Illegal data type for distinct count aggregation function: " + valueType);
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
          // Hashcode for int is the same as its value.
          setValueForGroupKey(groupByResultHolder, groupKeyArray[i], intValues[i]);
        }
        break;

      case LONG:
        long[] longValues = blockValSets[0].getLongValuesSV();
        for (int i = 0; i < length; i++) {
          int hashCode = Long.valueOf(longValues[i]).hashCode();
          setValueForGroupKey(groupByResultHolder, groupKeyArray[i], hashCode);
        }
        break;

      case FLOAT:
        float[] floatValues = blockValSets[0].getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          int hashCode = Float.valueOf(floatValues[i]).hashCode();
          setValueForGroupKey(groupByResultHolder, groupKeyArray[i], hashCode);
        }
        break;

      case DOUBLE:
        double[] doubleValues = blockValSets[0].getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          int hashCode = Double.valueOf(doubleValues[i]).hashCode();
          setValueForGroupKey(groupByResultHolder, groupKeyArray[i], hashCode);
        }
        break;

      case STRING:
        String[] stringValues = blockValSets[0].getStringValuesSV();
        for (int i = 0; i < length; i++) {
          int hashCode = stringValues[i].hashCode();
          setValueForGroupKey(groupByResultHolder, groupKeyArray[i], hashCode);
        }
        break;

      default:
        throw new IllegalArgumentException("Illegal data type for distinct count aggregation function: " + valueType);
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
          int hashCode = Long.valueOf(longValues[i]).hashCode();
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], hashCode);
        }
        break;

      case FLOAT:
        float[] floatValues = blockValSets[0].getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          int hashCode = Float.valueOf(floatValues[i]).hashCode();
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], hashCode);
        }
        break;

      case DOUBLE:
        double[] doubleValues = blockValSets[0].getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          int hashCode = Double.valueOf(doubleValues[i]).hashCode();
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], hashCode);
        }
        break;

      case STRING:
        String[] stringValues = blockValSets[0].getStringValuesSV();
        for (int i = 0; i < length; i++) {
          int hashCode = stringValues[i].hashCode();
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], hashCode);
        }
        break;

      default:
        throw new IllegalArgumentException("Illegal data type for distinct count aggregation function: " + valueType);
    }
  }

  @Nonnull
  @Override
  public IntOpenHashSet extractAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder) {
    IntOpenHashSet valueSet = aggregationResultHolder.getResult();
    if (valueSet == null) {
      return new IntOpenHashSet();
    } else {
      return valueSet;
    }
  }

  @Nonnull
  @Override
  public IntOpenHashSet extractGroupByResult(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    IntOpenHashSet valueSet = groupByResultHolder.getResult(groupKey);
    if (valueSet == null) {
      return new IntOpenHashSet();
    } else {
      return valueSet;
    }
  }

  @Nonnull
  @Override
  public IntOpenHashSet merge(@Nonnull IntOpenHashSet intermediateResult1,
      @Nonnull IntOpenHashSet intermediateResult2) {
    intermediateResult1.addAll(intermediateResult2);
    return intermediateResult1;
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return false;
  }

  @Nonnull
  @Override
  public FieldSpec.DataType getIntermediateResultDataType() {
    return FieldSpec.DataType.OBJECT;
  }

  @Nonnull
  @Override
  public Integer extractFinalResult(@Nonnull IntOpenHashSet intermediateResult) {
    return intermediateResult.size();
  }

  /**
   * Helper method to set value for a groupKey into the result holder.
   *
   * @param groupByResultHolder Result holder
   * @param groupKey Group-key for which to set the value
   * @param value Value for the group key
   */
  private void setValueForGroupKey(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey, int value) {
    IntOpenHashSet valueSet = groupByResultHolder.getResult(groupKey);
    if (valueSet == null) {
      valueSet = new IntOpenHashSet();
      groupByResultHolder.setValueForKey(groupKey, valueSet);
    }
    valueSet.add(value);
  }

  /**
   * Helper method to set values for a given array of groupKeys, into the result holder.
   *
   * @param groupByResultHolder Result holder
   * @param groupKeys Array of group keys for which to set the value
   * @param value Value to be set
   */
  private void setValueForGroupKeys(@Nonnull GroupByResultHolder groupByResultHolder, int[] groupKeys, int value) {
    for (int groupKey : groupKeys) {
      setValueForGroupKey(groupByResultHolder, groupKey, value);
    }
  }
}
