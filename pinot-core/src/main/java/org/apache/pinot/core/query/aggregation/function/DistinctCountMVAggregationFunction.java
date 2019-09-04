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
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;


public class DistinctCountMVAggregationFunction extends DistinctCountAggregationFunction {

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTMV;
  }

  @Override
  public String getColumnName(String column) {
    return AggregationFunctionType.DISTINCTCOUNTMV.getName() + "_" + column;
  }

  @Override
  public void accept(AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder, BlockValSet... blockValSets) {
    IntOpenHashSet valueSet = getValueSet(aggregationResultHolder);

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
            valueSet.add(Long.hashCode(value));
          }
        }
        break;
      case FLOAT:
        float[][] floatValues = blockValSets[0].getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          for (float value : floatValues[i]) {
            valueSet.add(Float.hashCode(value));
          }
        }
      case DOUBLE:
        double[][] doubleValues = blockValSets[0].getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          for (double value : doubleValues[i]) {
            valueSet.add(Double.hashCode(value));
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
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT_MV aggregation function: " + valueType);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      BlockValSet... blockValSets) {
    FieldSpec.DataType valueType = blockValSets[0].getValueType();
    switch (valueType) {
      case INT:
        int[][] intValues = blockValSets[0].getIntValuesMV();
        for (int i = 0; i < length; i++) {
          IntOpenHashSet valueSet = getValueSet(groupByResultHolder, groupKeyArray[i]);
          for (int value : intValues[i]) {
            valueSet.add(value);
          }
        }
        break;
      case LONG:
        long[][] longValues = blockValSets[0].getLongValuesMV();
        for (int i = 0; i < length; i++) {
          IntOpenHashSet valueSet = getValueSet(groupByResultHolder, groupKeyArray[i]);
          for (long value : longValues[i]) {
            valueSet.add(Long.hashCode(value));
          }
        }
        break;
      case FLOAT:
        float[][] floatValues = blockValSets[0].getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          IntOpenHashSet valueSet = getValueSet(groupByResultHolder, groupKeyArray[i]);
          for (float value : floatValues[i]) {
            valueSet.add(Float.hashCode(value));
          }
        }
        break;
      case DOUBLE:
        double[][] doubleValues = blockValSets[0].getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          IntOpenHashSet valueSet = getValueSet(groupByResultHolder, groupKeyArray[i]);
          for (double value : doubleValues[i]) {
            valueSet.add(Double.hashCode(value));
          }
        }
        break;
      case STRING:
        String[][] stringValues = blockValSets[0].getStringValuesMV();
        for (int i = 0; i < length; i++) {
          IntOpenHashSet valueSet = getValueSet(groupByResultHolder, groupKeyArray[i]);
          for (String value : stringValues[i]) {
            valueSet.add(value.hashCode());
          }
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT_MV aggregation function: " + valueType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      BlockValSet... blockValSets) {
    FieldSpec.DataType valueType = blockValSets[0].getValueType();
    switch (valueType) {
      case INT:
        int[][] intValues = blockValSets[0].getIntValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            IntOpenHashSet valueSet = getValueSet(groupByResultHolder, groupKey);
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
            IntOpenHashSet valueSet = getValueSet(groupByResultHolder, groupKey);
            for (long value : longValues[i]) {
              valueSet.add(Long.hashCode(value));
            }
          }
        }
        break;
      case FLOAT:
        float[][] floatValues = blockValSets[0].getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            IntOpenHashSet valueSet = getValueSet(groupByResultHolder, groupKey);
            for (float value : floatValues[i]) {
              valueSet.add(Float.hashCode(value));
            }
          }
        }
        break;
      case DOUBLE:
        double[][] doubleValues = blockValSets[0].getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            IntOpenHashSet valueSet = getValueSet(groupByResultHolder, groupKey);
            for (double value : doubleValues[i]) {
              valueSet.add(Double.hashCode(value));
            }
          }
        }
        break;
      case STRING:
        String[][] stringValues = blockValSets[0].getStringValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            IntOpenHashSet valueSet = getValueSet(groupByResultHolder, groupKey);
            for (String value : stringValues[i]) {
              valueSet.add(value.hashCode());
            }
          }
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT_MV aggregation function: " + valueType);
    }
  }
}
