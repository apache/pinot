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
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.spi.data.FieldSpec;


public class DistinctCountMVAggregationFunction extends DistinctCountAggregationFunction {

  public DistinctCountMVAggregationFunction(ExpressionContext expression) {
    super(expression);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTMV;
  }

  @Override
  public void accept(AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    IntOpenHashSet valueSet = getValueSet(aggregationResultHolder);

    BlockValSet blockValSet = blockValSetMap.get(_expression);
    FieldSpec.DataType valueType = blockValSet.getValueType();
    switch (valueType) {
      case INT:
        int[][] intValues = blockValSet.getIntValuesMV();
        for (int i = 0; i < length; i++) {
          for (int value : intValues[i]) {
            valueSet.add(value);
          }
        }
        break;
      case LONG:
        long[][] longValues = blockValSet.getLongValuesMV();
        for (int i = 0; i < length; i++) {
          for (long value : longValues[i]) {
            valueSet.add(Long.hashCode(value));
          }
        }
        break;
      case FLOAT:
        float[][] floatValues = blockValSet.getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          for (float value : floatValues[i]) {
            valueSet.add(Float.hashCode(value));
          }
        }
      case DOUBLE:
        double[][] doubleValues = blockValSet.getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          for (double value : doubleValues[i]) {
            valueSet.add(Double.hashCode(value));
          }
        }
        break;
      case STRING:
        String[][] stringValues = blockValSet.getStringValuesMV();
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
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    FieldSpec.DataType valueType = blockValSet.getValueType();

    switch (valueType) {
      case INT:
        int[][] intValues = blockValSet.getIntValuesMV();
        for (int i = 0; i < length; i++) {
          IntOpenHashSet valueSet = getValueSet(groupByResultHolder, groupKeyArray[i]);
          for (int value : intValues[i]) {
            valueSet.add(value);
          }
        }
        break;
      case LONG:
        long[][] longValues = blockValSet.getLongValuesMV();
        for (int i = 0; i < length; i++) {
          IntOpenHashSet valueSet = getValueSet(groupByResultHolder, groupKeyArray[i]);
          for (long value : longValues[i]) {
            valueSet.add(Long.hashCode(value));
          }
        }
        break;
      case FLOAT:
        float[][] floatValues = blockValSet.getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          IntOpenHashSet valueSet = getValueSet(groupByResultHolder, groupKeyArray[i]);
          for (float value : floatValues[i]) {
            valueSet.add(Float.hashCode(value));
          }
        }
        break;
      case DOUBLE:
        double[][] doubleValues = blockValSet.getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          IntOpenHashSet valueSet = getValueSet(groupByResultHolder, groupKeyArray[i]);
          for (double value : doubleValues[i]) {
            valueSet.add(Double.hashCode(value));
          }
        }
        break;
      case STRING:
        String[][] stringValues = blockValSet.getStringValuesMV();
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
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    FieldSpec.DataType valueType = blockValSet.getValueType();

    switch (valueType) {
      case INT:
        int[][] intValues = blockValSet.getIntValuesMV();
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
        long[][] longValues = blockValSet.getLongValuesMV();
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
        float[][] floatValues = blockValSet.getFloatValuesMV();
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
        double[][] doubleValues = blockValSet.getDoubleValuesMV();
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
        String[][] stringValues = blockValSet.getStringValuesMV();
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
