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

import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.query.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import javax.annotation.Nonnull;


public class MinMVStringAggregationFunction extends MinStringAggregationFunction {
  private static final String NAME = AggregationFunctionFactory.AggregationFunctionType.MINMVSTRING.getName();

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
    String[][] stringValuesArray = blockValSets[0].getStringValuesMV();
    String minString = null;
    for (int i = 0; i < length; i++) {
      for (String value : stringValuesArray[i]) {
        if (minString == null || value.compareTo(minString) < 0) {
          minString = value;
        }
      }
    }
    setAggregationResult(aggregationResultHolder, minString);
  }

  @Override
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    String[][] stringValuesArray = blockValSets[0].getStringValuesMV();
    for (int i = 0; i < length; i++) {
      String minString = findMinString(stringValuesArray[i].length, stringValuesArray[i]);
      setGroupByResult(groupKeyArray[i], groupByResultHolder, minString);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    String[][] stringValuesArray = blockValSets[0].getStringValuesMV();
    for (int i = 0; i < length; i++) {
      String minString = findMinString(stringValuesArray[i].length, stringValuesArray[i]);
      for (int groupKey : groupKeysArray[i]) {
        setGroupByResult(groupKey, groupByResultHolder, minString);
      }
    }
  }
}
