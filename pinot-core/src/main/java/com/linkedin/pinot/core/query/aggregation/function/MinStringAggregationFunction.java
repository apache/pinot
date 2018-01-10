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


public class MinStringAggregationFunction implements AggregationFunction<String, String> {
  private static final String NAME = AggregationFunctionFactory.AggregationFunctionType.MINSTRING.getName();
  private static final String DEFAULT_STRING_VALUE = "";

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
    String[] stringValueArray = blockValSets[0].getStringValuesSV();
    String minString = findMinString(length, stringValueArray);
    setAggregationResult(aggregationResultHolder, minString);
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

  protected void setAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder, String value) {
    String prevMinValue = aggregationResultHolder.getResult();
    if (prevMinValue == null || prevMinValue.compareTo(value) > 0) {
      aggregationResultHolder.setValue(value);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    String[] stringValueArray = blockValSets[0].getStringValuesSV();
    for (int i = 0; i < length; i++) {
      setGroupByResult(groupKeyArray[i], groupByResultHolder, stringValueArray[i]);
    }
  }

  protected void setGroupByResult(int groupKey, @Nonnull GroupByResultHolder groupByResultHolder, String value) {
    String prevMinValue = groupByResultHolder.getResult(groupKey);
    if (prevMinValue == null || prevMinValue.compareTo(value) > 0) {
      groupByResultHolder.setValueForKey(groupKey, value);
    }
  }


  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    String[] stringValueArray = blockValSets[0].getStringValuesSV();
    for (int i = 0; i < length; i++) {
      String value = stringValueArray[i];
      for (int groupKey : groupKeysArray[i]) {
        setGroupByResult(groupKey, groupByResultHolder, value);
      }
    }
  }

  @Nonnull
  @Override
  public String extractAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder) {
    String minString = aggregationResultHolder.getResult();
    if (minString == null) {
      return DEFAULT_STRING_VALUE;
    } else {
      return minString;
    }
  }

  @Nonnull
  @Override
  public String extractGroupByResult(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    String minString = groupByResultHolder.getResult(groupKey);
    if (minString == null) {
      return DEFAULT_STRING_VALUE;
    } else {
      return minString;
    }
  }

  @Nonnull
  @Override
  public String merge(@Nonnull String intermediateResult1, @Nonnull String intermediateResult2) {
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
    return FieldSpec.DataType.STRING;
  }

  @Nonnull
  @Override
  public String extractFinalResult(@Nonnull String intermediateResult) {
    return intermediateResult;
  }
}
