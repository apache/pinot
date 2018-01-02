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
import javax.annotation.Nonnull;


public class MinAggregationFunction implements AggregationFunction<Comparable, Comparable> {
  private static final String NAME = AggregationFunctionFactory.AggregationFunctionType.MIN.getName();

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
    FieldSpec.DataType dataType = blockValSets[0].getValueType();
    switch (dataType) {
      case FLOAT:
      case INT:
      case LONG:
      case SHORT:
      case DOUBLE:
        double[] doubleValueArray = blockValSets[0].getDoubleValuesSV();
        double minDouble = findMinDouble(length, doubleValueArray);
        setAggregationResult(aggregationResultHolder, minDouble);
        break;

      case STRING:
        String[] stringValueArray = blockValSets[0].getStringValuesSV();
        String minString = findMinString(length, stringValueArray);
        setAggregationResult(aggregationResultHolder, minString);
        break;

      default:
        throw new IllegalArgumentException("Min operation not supported on datatype " + dataType);
    }
  }

  protected double findMinDouble(int length, double[] doubleValueArray) {
    double minDouble = Double.POSITIVE_INFINITY;
    for (int i = 0; i < length; i++) {
      double value = doubleValueArray[i];
      minDouble = Math.min(minDouble, value);
    }
    return minDouble;
  }

  protected String findMinString(int length, String[] stringValueArray) {
    String minString = null;
    for (int i = 0; i < length; i++) {
      String value = stringValueArray[i];
      if (minString == null || value.compareTo(minString) < 0) {
        minString = value;
      }
    }
    return minString;
  }

  protected void setAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder, Comparable minValue) {
    Comparable prevMinValue = aggregationResultHolder.getResult();
    if (prevMinValue == null || prevMinValue.compareTo(minValue) > 0) {
      aggregationResultHolder.setValue(minValue);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    FieldSpec.DataType dataType = blockValSets[0].getValueType();
    switch (dataType) {
      case FLOAT:
      case INT:
      case LONG:
      case SHORT:
      case DOUBLE:
        double[] doubleValueArray = blockValSets[0].getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          setGroupByResult(groupKeyArray[i], groupByResultHolder, doubleValueArray[i]);
        }
        break;

      case STRING:
        String[] stringValueArray = blockValSets[0].getStringValuesSV();
        for (int i = 0; i < length; i++) {
          setGroupByResult(groupKeyArray[i], groupByResultHolder, stringValueArray[i]);
        }
        break;

      default:
        throw new IllegalArgumentException("Min operation not supported on datatype " + dataType);
    }
  }

  protected void setGroupByResult(int groupKey, @Nonnull GroupByResultHolder groupByResultHolder, Comparable value) {
    Comparable prevMinValue = groupByResultHolder.getResult(groupKey);
    if (prevMinValue == null || prevMinValue.compareTo(value) > 0) {
      groupByResultHolder.setValueForKey(groupKey, value);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    FieldSpec.DataType dataType = blockValSets[0].getValueType();
    switch (dataType) {
      case FLOAT:
      case INT:
      case LONG:
      case SHORT:
      case DOUBLE:
        double[] doubleValueArray = blockValSets[0].getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          double value = doubleValueArray[i];
          for (int groupKey : groupKeysArray[i]) {
            setGroupByResult(groupKey, groupByResultHolder, value);
          }
        }
        break;

      case STRING:
        String[] stringValueArray = blockValSets[0].getStringValuesSV();
        for (int i = 0; i < length; i++) {
          String value = stringValueArray[i];
          for (int groupKey : groupKeysArray[i]) {
            setGroupByResult(groupKey, groupByResultHolder, value);
          }
        }
        break;

      default:
        throw new IllegalArgumentException("Min operation not supported on datatype " + dataType);
    }
  }

  @Nonnull
  @Override
  public Comparable extractAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder) {
    Comparable minValue = aggregationResultHolder.getResult();
    if (minValue == null) {
      return aggregationResultHolder.getDefaultValue();
    } else {
      return minValue;
    }
  }

  @Nonnull
  @Override
  public Comparable extractGroupByResult(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Nonnull
  @Override
  public Comparable merge(@Nonnull Comparable intermediateResult1, @Nonnull Comparable intermediateResult2) {
    if (intermediateResult1.compareTo(intermediateResult2) < 0) {
      return intermediateResult1;
    } else {
      return intermediateResult2;
    }
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return true;
  }

  @Nonnull
  @Override
  public FieldSpec.DataType getIntermediateResultDataType() {
    return FieldSpec.DataType.OBJECT;
  }

  @Nonnull
  @Override
  public Comparable extractFinalResult(@Nonnull Comparable intermediateResult) {
    return intermediateResult;
  }
}
