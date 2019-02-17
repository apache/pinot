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

import javax.annotation.Nonnull;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.customobject.VarianceTuple;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;

/**
 * Aggregation function which returns the population standard variance of an expression.
 * var = ((x1-mean)^2 + ... + (xn-mean)^2)/n = sum2/n - mean^2
 * in which sum2 = x1^2 + x2^2 + ... + xn^2
 */
public class VarPopAggregationFunction implements AggregationFunction<VarianceTuple, Double> {
  private static final double DEFAULT_FINAL_RESULT = Double.NEGATIVE_INFINITY;

  @Nonnull
  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.VARPOP;
  }

  @Nonnull
  @Override
  public String getColumnName(@Nonnull String column) {
    return AggregationFunctionType.VARPOP.getName() + "_" + column;
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
    FieldSpec.DataType valueType = blockValSets[0].getValueType();
    switch (valueType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        double[] valueArray = blockValSets[0].getDoubleValuesSV();
        double sum2 = 0.0;
        double sum = 0.0;
        for (int i = 0; i < length; i++) {
          sum2 += Math.pow(valueArray[i], 2);
          sum += valueArray[i];
        }
        setAggregationResult(aggregationResultHolder, sum2, sum, (long) length);
        break;
      case BYTES:
        // Serialized VarianceTuple
        byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
        sum2 = 0.0;
        sum = 0.0;
        long count = 0L;
        for (int i = 0; i < length; i++) {
          VarianceTuple value = ObjectSerDeUtils.VARIANCE_TUPLE_SER_DE.deserialize(bytesValues[i]);
          sum2 += value.getSum2();
          sum += value.getSum();
          count += value.getCount();
        }
        setAggregationResult(aggregationResultHolder, sum2, sum, count);
        break;
      default:
        throw new IllegalStateException("Illegal data type for VARPOP aggregation function: " + valueType);
    }
  }

  protected void setAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder, double sum2,
      double sum, long count) {
    VarianceTuple varTuple = aggregationResultHolder.getResult();
    if (varTuple == null) {
      aggregationResultHolder.setValue(new VarianceTuple(sum2, sum, count));
    } else {
      varTuple.apply(sum2, sum, count);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    FieldSpec.DataType valueType = blockValSets[0].getValueType();
    switch (valueType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        double[] valueArray = blockValSets[0].getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          setGroupByResult(groupKeyArray[i], groupByResultHolder, Math.pow(valueArray[i], 2), 
              valueArray[i], 1L);
        }
        break;
      case BYTES:
        // Serialized VarianceTuple
        byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          VarianceTuple value = ObjectSerDeUtils.VARIANCE_TUPLE_SER_DE.deserialize(bytesValues[i]);
          setGroupByResult(groupKeyArray[i], groupByResultHolder, Math.pow(value.getSum(), 2),
              value.getSum(), value.getCount());
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for VARPOP aggregation function: " + valueType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    FieldSpec.DataType valueType = blockValSets[0].getValueType();
    switch (valueType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        double[] valueArray = blockValSets[0].getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          double value = valueArray[i];
          for (int groupKey : groupKeysArray[i]) {
            setGroupByResult(groupKey, groupByResultHolder, Math.pow(value, 2), value, 1L);
          }
        }
        break;
      case BYTES:
        // Serialized VarianceTuple
        byte[][] bytesValues = blockValSets[0].getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          VarianceTuple value = ObjectSerDeUtils.VARIANCE_TUPLE_SER_DE.deserialize(bytesValues[i]);
          double sum = value.getSum();
          long count = value.getCount();
          for (int groupKey : groupKeysArray[i]) {
            setGroupByResult(groupKey, groupByResultHolder, Math.pow(sum, 2), sum, count);
          }
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for VARPOP aggregation function: " + valueType);
    }
  }

  protected void setGroupByResult(int groupKey, @Nonnull GroupByResultHolder groupByResultHolder, double sum2,
      double sum, long count) {
    VarianceTuple varTuple = groupByResultHolder.getResult(groupKey);
    if (varTuple == null) {
      groupByResultHolder.setValueForKey(groupKey, new VarianceTuple(sum2, sum, count));
    } else {
      varTuple.apply(sum2, sum, count);
    }
  }

  @Nonnull
  @Override
  public VarianceTuple extractAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder) {
    VarianceTuple varTuple = aggregationResultHolder.getResult();
    if (varTuple == null) {
      return new VarianceTuple(0.0, 0.0, 0L);
    } else {
      return varTuple;
    }
  }

  @Nonnull
  @Override
  public VarianceTuple extractGroupByResult(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    VarianceTuple varTuple = groupByResultHolder.getResult(groupKey);
    if (varTuple == null) {
      return new VarianceTuple(0.0, 0.0, 0L);
    } else {
      return varTuple;
    }
  }

  @Nonnull
  @Override
  public VarianceTuple merge(@Nonnull VarianceTuple intermediateResult1, @Nonnull VarianceTuple intermediateResult2) {
    intermediateResult1.apply(intermediateResult2);
    return intermediateResult1;
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return true;
  }

  @Nonnull
  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Nonnull
  @Override
  public Double extractFinalResult(@Nonnull VarianceTuple intermediateResult) {
    long count = intermediateResult.getCount();
    if (count == 0L) {
      return DEFAULT_FINAL_RESULT;
    } else {
      return intermediateResult.getSum2() / count - Math.pow((intermediateResult.getSum() / count), 2);
    }
  }
}
