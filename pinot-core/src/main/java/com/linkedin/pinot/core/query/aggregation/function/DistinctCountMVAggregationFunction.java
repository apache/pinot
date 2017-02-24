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
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import javax.annotation.Nonnull;


public class DistinctCountMVAggregationFunction extends DistinctCountAggregationFunction {
  private static final String NAME = AggregationFunctionFactory.AggregationFunctionType.DISTINCTCOUNTMV.getName();

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
        int[][] intValues = blockValSets[0].getIntValuesMV();
        for (int i = 0; i < length; i++) {
          for (int value : intValues[i]) {
            valueSet.add(value);
          }
        }
        break;

      case LONG:
        long[][] longValues = blockValSets[0].getLongValuesMV();
        for (int i = 0; i < length; i++) {
          for (long value : longValues[i]) {
            valueSet.add(Long.valueOf(value).hashCode());
          }
        }
        break;

      case FLOAT:
        float[][] floatValues = blockValSets[0].getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          for (float value : floatValues[i]) {
            valueSet.add(Float.valueOf(value).hashCode());
          }
        }

      case DOUBLE:
        double[][] doubleValues = blockValSets[0].getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          for (double value : doubleValues[i]) {
            valueSet.add(Double.valueOf(value).hashCode());
          }
        }
        break;

      case STRING:
        String[][] stringValues = blockValSets[0].getStringValuesMV();
        for (int i = 0; i < length; i++) {
          for (String value : stringValues[i]) {
            valueSet.add(value.hashCode());
          }
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
        int[][] intValues = blockValSets[0].getIntValuesMV();
        for (int i = 0; i < length; i++) {
          IntOpenHashSet valueSet = getOrCreateHashSetForGroupKey(groupByResultHolder, groupKeyArray[i]);
          for (int value : intValues[i]) {
            valueSet.add(value);
          }
        }
        break;

      case LONG:
        long[][] longValues = blockValSets[0].getLongValuesMV();
        for (int i = 0; i < length; i++) {
          IntOpenHashSet valueSet = getOrCreateHashSetForGroupKey(groupByResultHolder, groupKeyArray[i]);
          for (long value : longValues[i]) {
            valueSet.add(Long.valueOf(value).hashCode());
          }
        }
        break;

      case FLOAT:
        float[][] floatValues = blockValSets[0].getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          IntOpenHashSet valueSet = getOrCreateHashSetForGroupKey(groupByResultHolder, groupKeyArray[i]);
          for (float value : floatValues[i]) {
            valueSet.add(Float.valueOf(value).hashCode());
          }
        }
        break;

      case DOUBLE:
        double[][] doubleValues = blockValSets[0].getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          IntOpenHashSet valueSet = getOrCreateHashSetForGroupKey(groupByResultHolder, groupKeyArray[i]);
          for (double value : doubleValues[i]) {
            valueSet.add(Double.valueOf(value).hashCode());
          }
        }
        break;

      case STRING:
        String[][] stringValues = blockValSets[0].getStringValuesMV();
        for (int i = 0; i < length; i++) {
          IntOpenHashSet valueSet = getOrCreateHashSetForGroupKey(groupByResultHolder, groupKeyArray[i]);
          for (String value : stringValues[i]) {
            valueSet.add(value.hashCode());
          }
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
        int[][] intValues = blockValSets[0].getIntValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            IntOpenHashSet valueSet = getOrCreateHashSetForGroupKey(groupByResultHolder, groupKey);
            for (int value : intValues[i]) {
              valueSet.add(value);
            }
          }
        }
        break;

      case LONG:
        long[][] longValues = blockValSets[0].getLongValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            IntOpenHashSet valueSet = getOrCreateHashSetForGroupKey(groupByResultHolder, groupKey);
            for (long value : longValues[i]) {
              valueSet.add(Long.valueOf(value).hashCode());
            }
          }
        }
        break;

      case FLOAT:
        float[][] floatValues = blockValSets[0].getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            IntOpenHashSet valueSet = getOrCreateHashSetForGroupKey(groupByResultHolder, groupKey);
            for (float value : floatValues[i]) {
              valueSet.add(Float.valueOf(value).hashCode());
            }
          }
        }
        break;

      case DOUBLE:
        double[][] doubleValues = blockValSets[0].getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            IntOpenHashSet valueSet = getOrCreateHashSetForGroupKey(groupByResultHolder, groupKey);
            for (double value : doubleValues[i]) {
              valueSet.add(Double.valueOf(value).hashCode());
            }
          }
        }
        break;

      case STRING:
        String[][] stringValues = blockValSets[0].getStringValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            IntOpenHashSet valueSet = getOrCreateHashSetForGroupKey(groupByResultHolder, groupKey);
            for (String value : stringValues[i]) {
              valueSet.add(value.hashCode());
            }
          }
        }
        break;

      default:
        throw new IllegalArgumentException("Illegal data type for distinct count aggregation function: " + valueType);
    }
  }

  /**
   * Returns the hash-set for the given group-key. Creates and returns one, if it does not exist.
   *
   * @param groupByResultHolder Result Holder
   * @param groupKey Group key for which to get the hash set.
   * @return Hash-set for the group key
   */
  private IntOpenHashSet getOrCreateHashSetForGroupKey(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    IntOpenHashSet valueSet = groupByResultHolder.getResult(groupKey);
    if (valueSet == null) {
      valueSet = new IntOpenHashSet();
      groupByResultHolder.setValueForKey(groupKey, valueSet);
    }
    return valueSet;
  }
}
