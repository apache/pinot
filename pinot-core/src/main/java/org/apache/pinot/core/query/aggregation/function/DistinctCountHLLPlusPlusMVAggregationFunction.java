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

import com.google.zetasketch.HyperLogLogPlusPlus;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class DistinctCountHLLPlusPlusMVAggregationFunction extends DistinctCountHLLPlusPlusAggregationFunction {

  public DistinctCountHLLPlusPlusMVAggregationFunction(List<ExpressionContext> arguments) {
    super(arguments);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTHLLPLUSPLUSMV;
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {

    BlockValSet blockValSet = blockValSetMap.get(_expression);
    DataType storedType = blockValSet.getValueType().getStoredType();
    HyperLogLogPlusPlus hyperLogLog = getDefaultHyperLogLogPlusPlus(aggregationResultHolder, storedType);
    switch (storedType) {
      case INT:
        int[][] intValuesArray = blockValSet.getIntValuesMV();
        for (int i = 0; i < length; i++) {
          for (int value : intValuesArray[i]) {
            hyperLogLog.add(value);
          }
        }
        break;
      case LONG:
        long[][] longValuesArray = blockValSet.getLongValuesMV();
        for (int i = 0; i < length; i++) {
          for (long value : longValuesArray[i]) {
            hyperLogLog.add(value);
          }
        }
        break;
      case FLOAT:
        float[][] floatValuesArray = blockValSet.getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          for (float value : floatValuesArray[i]) {
            hyperLogLog.add(value);
          }
        }
        break;
      case DOUBLE:
        double[][] doubleValuesArray = blockValSet.getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          for (double value : doubleValuesArray[i]) {
            hyperLogLog.add(value);
          }
        }
        break;
      case STRING:
        String[][] stringValuesArray = blockValSet.getStringValuesMV();
        for (int i = 0; i < length; i++) {
          for (String value : stringValuesArray[i]) {
            hyperLogLog.add(value);
          }
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_HLL_PLUS_PLUS_MV aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[][] intValuesArray = blockValSet.getIntValuesMV();
        for (int i = 0; i < length; i++) {
          HyperLogLogPlusPlus hyperLogLog = getDefaultHyperLogLogPlusPlus(groupByResultHolder, groupKeyArray[i], storedType);
          for (int value : intValuesArray[i]) {
            hyperLogLog.add(value);
          }
        }
        break;
      case LONG:
        long[][] longValuesArray = blockValSet.getLongValuesMV();
        for (int i = 0; i < length; i++) {
          HyperLogLogPlusPlus hyperLogLog = getDefaultHyperLogLogPlusPlus(groupByResultHolder, groupKeyArray[i], storedType);
          for (long value : longValuesArray[i]) {
            hyperLogLog.add(value);
          }
        }
        break;
      case FLOAT:
        float[][] floatValuesArray = blockValSet.getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          HyperLogLogPlusPlus hyperLogLog = getDefaultHyperLogLogPlusPlus(groupByResultHolder, groupKeyArray[i], storedType);
          for (float value : floatValuesArray[i]) {
            hyperLogLog.add(value);
          }
        }
        break;
      case DOUBLE:
        double[][] doubleValuesArray = blockValSet.getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          HyperLogLogPlusPlus hyperLogLog = getDefaultHyperLogLogPlusPlus(groupByResultHolder, groupKeyArray[i], storedType);
          for (double value : doubleValuesArray[i]) {
            hyperLogLog.add(value);
          }
        }
        break;
      case STRING:
        String[][] stringValuesArray = blockValSet.getStringValuesMV();
        for (int i = 0; i < length; i++) {
          HyperLogLogPlusPlus hyperLogLog = getDefaultHyperLogLogPlusPlus(groupByResultHolder, groupKeyArray[i], storedType);
          for (String value : stringValuesArray[i]) {
            hyperLogLog.add(value);
          }
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_HLL_PLUS_PLUS_MV aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[][] intValuesArray = blockValSet.getIntValuesMV();
        for (int i = 0; i < length; i++) {
          int[] intValues = intValuesArray[i];
          for (int groupKey : groupKeysArray[i]) {
            HyperLogLogPlusPlus hyperLogLog = getDefaultHyperLogLogPlusPlus(groupByResultHolder, groupKey, storedType);
            for (int value : intValues) {
              hyperLogLog.add(value);
            }
          }
        }
        break;
      case LONG:
        long[][] longValuesArray = blockValSet.getLongValuesMV();
        for (int i = 0; i < length; i++) {
          long[] longValues = longValuesArray[i];
          for (int groupKey : groupKeysArray[i]) {
            HyperLogLogPlusPlus hyperLogLog = getDefaultHyperLogLogPlusPlus(groupByResultHolder, groupKey, storedType);
            for (long value : longValues) {
              hyperLogLog.add(value);
            }
          }
        }
        break;
      case FLOAT:
        float[][] floatValuesArray = blockValSet.getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          float[] floatValues = floatValuesArray[i];
          for (int groupKey : groupKeysArray[i]) {
            HyperLogLogPlusPlus hyperLogLog = getDefaultHyperLogLogPlusPlus(groupByResultHolder, groupKey, storedType);
            for (float value : floatValues) {
              hyperLogLog.add(value);
            }
          }
        }
        break;
      case DOUBLE:
        double[][] doubleValuesArray = blockValSet.getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          double[] doubleValues = doubleValuesArray[i];
          for (int groupKey : groupKeysArray[i]) {
            HyperLogLogPlusPlus hyperLogLog = getDefaultHyperLogLogPlusPlus(groupByResultHolder, groupKey, storedType);
            for (double value : doubleValues) {
              hyperLogLog.add(value);
            }
          }
        }
        break;
      case STRING:
        String[][] stringValuesArray = blockValSet.getStringValuesMV();
        for (int i = 0; i < length; i++) {
          String[] stringValues = stringValuesArray[i];
          for (int groupKey : groupKeysArray[i]) {
            HyperLogLogPlusPlus hyperLogLog = getDefaultHyperLogLogPlusPlus(groupByResultHolder, groupKey, storedType);
            for (String value : stringValues) {
              hyperLogLog.add(value);
            }
          }
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_HLL_PLUS_PLUS_MV aggregation function: " + storedType);
    }
  }
}
