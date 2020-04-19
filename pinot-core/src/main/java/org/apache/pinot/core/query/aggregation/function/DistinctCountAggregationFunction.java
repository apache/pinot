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

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.Map;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.spi.data.FieldSpec;


public class DistinctCountAggregationFunction implements AggregationFunction<IntOpenHashSet, Integer> {

  protected final String _column;

  /**
   * Constructor for the class.
   * @param column Column name to aggregate on.
   */
  public DistinctCountAggregationFunction(String column) {
    _column = column;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNT;
  }

  @Override
  public String getColumnName() {
    return getType().getName() + "_" + _column;
  }

  @Override
  public String getResultColumnName() {
    return getType().getName().toLowerCase() + "(" + _column + ")";
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
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder, Map<String, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_column);
    IntOpenHashSet valueSet = getValueSet(aggregationResultHolder);

    FieldSpec.DataType valueType = blockValSet.getValueType();
    switch (valueType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          valueSet.add(intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          valueSet.add(Long.hashCode(longValues[i]));
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          valueSet.add(Float.hashCode(floatValues[i]));
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          valueSet.add(Double.hashCode(doubleValues[i]));
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          valueSet.add(stringValues[i].hashCode());
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT aggregation function: " + valueType);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<String, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_column);
    FieldSpec.DataType valueType = blockValSet.getValueType();

    switch (valueType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKey(groupByResultHolder, groupKeyArray[i], intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKey(groupByResultHolder, groupKeyArray[i], Long.hashCode(longValues[i]));
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKey(groupByResultHolder, groupKeyArray[i], Float.hashCode(floatValues[i]));
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKey(groupByResultHolder, groupKeyArray[i], Double.hashCode(doubleValues[i]));
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKey(groupByResultHolder, groupKeyArray[i], stringValues[i].hashCode());
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT aggregation function: " + valueType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<String, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_column);

    FieldSpec.DataType valueType = blockValSet.getValueType();
    switch (valueType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], Long.hashCode(longValues[i]));
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], Float.hashCode(floatValues[i]));
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], Double.hashCode(doubleValues[i]));
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], stringValues[i].hashCode());
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT aggregation function: " + valueType);
    }
  }

  @Override
  public IntOpenHashSet extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    IntOpenHashSet valueSet = aggregationResultHolder.getResult();
    if (valueSet == null) {
      return new IntOpenHashSet();
    } else {
      return valueSet;
    }
  }

  @Override
  public IntOpenHashSet extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    IntOpenHashSet valueSet = groupByResultHolder.getResult(groupKey);
    if (valueSet == null) {
      return new IntOpenHashSet();
    } else {
      return valueSet;
    }
  }

  @Override
  public IntOpenHashSet merge(IntOpenHashSet intermediateResult1, IntOpenHashSet intermediateResult2) {
    intermediateResult1.addAll(intermediateResult2);
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
    return ColumnDataType.INT;
  }

  @Override
  public Integer extractFinalResult(IntOpenHashSet intermediateResult) {
    return intermediateResult.size();
  }

  /**
   * Helper method to set value for a groupKey into the result holder.
   *
   * @param groupByResultHolder Result holder
   * @param groupKey Group-key for which to set the value
   * @param value Value for the group key
   */
  private void setValueForGroupKey(GroupByResultHolder groupByResultHolder, int groupKey, int value) {
    IntOpenHashSet valueSet = getValueSet(groupByResultHolder, groupKey);
    valueSet.add(value);
  }

  /**
   * Helper method to set values for a given array of groupKeys, into the result holder.
   *
   * @param groupByResultHolder Result holder
   * @param groupKeys Array of group keys for which to set the value
   * @param value Value to be set
   */
  private void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys, int value) {
    for (int groupKey : groupKeys) {
      setValueForGroupKey(groupByResultHolder, groupKey, value);
    }
  }

  /**
   * Returns the value set from the result holder or creates a new one if it does not exist.
   *
   * @param aggregationResultHolder Result holder
   * @return Value set from the result holder
   */
  protected static IntOpenHashSet getValueSet(AggregationResultHolder aggregationResultHolder) {
    IntOpenHashSet valueSet = aggregationResultHolder.getResult();
    if (valueSet == null) {
      valueSet = new IntOpenHashSet();
      aggregationResultHolder.setValue(valueSet);
    }
    return valueSet;
  }

  /**
   * Returns the value set for the given group key. If one does not exist, creates a new one and returns that.
   *
   * @param groupByResultHolder Result holder
   * @param groupKey Group key for which to return the value set
   * @return Value set for the group key
   */
  protected static IntOpenHashSet getValueSet(GroupByResultHolder groupByResultHolder, int groupKey) {
    IntOpenHashSet valueSet = groupByResultHolder.getResult(groupKey);
    if (valueSet == null) {
      valueSet = new IntOpenHashSet();
      groupByResultHolder.setValueForKey(groupKey, valueSet);
    }
    return valueSet;
  }
}
