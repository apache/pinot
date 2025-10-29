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

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.LongAggregateResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.LongGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Specialized INT sum aggregation function that avoids type conversion overhead.
 * This function is optimized for INT columns and uses native INT arithmetic.
 *
 * Performance optimizations:
 * - Direct INT arithmetic without DOUBLE conversion
 * - Vectorized operations for better CPU utilization
 * - Minimal object allocations
 * - Optimized for the specific case of INT column aggregation
 * - Proper null handling support using foldNotNull and forEachNotNull
 */
public class SumIntAggregationFunction extends NullableSingleInputAggregationFunction<Long, Long> {
  public static final String FUNCTION_NAME = "sumInt";
  private static final long DEFAULT_VALUE = 0L;

  public SumIntAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(arguments.get(0), nullHandlingEnabled);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.SUMINT;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    if (_nullHandlingEnabled) {
      return new ObjectAggregationResultHolder();
    } else {
      return new LongAggregateResultHolder(DEFAULT_VALUE);
    }
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    if (_nullHandlingEnabled) {
      return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
    } else {
      return new LongGroupByResultHolder(initialCapacity, maxCapacity, DEFAULT_VALUE);
    }
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    if (blockValSet.getValueType().getStoredType() != DataType.INT) {
      throw new IllegalArgumentException("SumIntAggregationFunction only supports INT columns");
    }

    int[] values = blockValSet.getIntValuesSV();

    // Use foldNotNull with null as initial value - this will return null if no non-null values are processed
    Long sum = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
      long innerSum = 0;
      for (int i = from; i < to; i++) {
        innerSum += values[i];
      }
      return acum == null ? innerSum : acum + innerSum;
    });

    updateAggregationResultHolder(aggregationResultHolder, sum);
  }

  private void updateAggregationResultHolder(AggregationResultHolder aggregationResultHolder, Long sum) {
    if (sum != null) {
      if (_nullHandlingEnabled) {
        Long otherSum = aggregationResultHolder.getResult();
        aggregationResultHolder.setValue(otherSum == null ? sum : otherSum + sum);
      } else {
        long otherSum = aggregationResultHolder.getLongResult();
        aggregationResultHolder.setValue(otherSum + sum);
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    int[] values = blockValSet.getIntValuesSV();

    if (_nullHandlingEnabled) {
      // Use forEachNotNull to handle nulls properly
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          int groupKey = groupKeyArray[i];
          Long existingResult = groupByResultHolder.getResult(groupKey);
          groupByResultHolder.setValueForKey(groupKey,
              (existingResult == null ? values[i] : values[i] + existingResult));
        }
      });
    } else {
      // Process all values when null handling is disabled
      for (int i = 0; i < length; i++) {
        int groupKey = groupKeyArray[i];
        groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getLongResult(groupKey) + values[i]);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    int[] values = blockValSet.getIntValuesSV();

    if (_nullHandlingEnabled) {
      // Use forEachNotNull to handle nulls properly
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          int value = values[i];
          for (int groupKey : groupKeysArray[i]) {
            Long existingResult = groupByResultHolder.getResult(groupKey);
            groupByResultHolder.setValueForKey(groupKey, existingResult == null ? value : existingResult + value);
          }
        }
      });
    } else {
      // Process all values when null handling is disabled
      for (int i = 0; i < length; i++) {
        int value = values[i];
        for (int groupKey : groupKeysArray[i]) {
          groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getLongResult(groupKey) + value);
        }
      }
    }
  }

  @Override
  public Long extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    if (_nullHandlingEnabled) {
      return aggregationResultHolder.getResult();
    }
    return aggregationResultHolder.getLongResult();
  }

  @Override
  public Long extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    if (_nullHandlingEnabled) {
      return groupByResultHolder.getResult(groupKey);
    }
    return groupByResultHolder.getLongResult(groupKey);
  }

  @Override
  public Long merge(Long intermediateResult1, Long intermediateResult2) {
    if (_nullHandlingEnabled) {
      if (intermediateResult1 == null) {
        return intermediateResult2;
      }
      if (intermediateResult2 == null) {
        return intermediateResult1;
      }
      // Both are non-null
      return intermediateResult1 + intermediateResult2;
    } else {
      long val1 = (intermediateResult1 != null) ? intermediateResult1 : 0L;
      long val2 = (intermediateResult2 != null) ? intermediateResult2 : 0L;
      return val1 + val2;
    }
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.LONG;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.LONG;
  }

  @Override
  public Long extractFinalResult(Long intermediateResult) {
    return intermediateResult;
  }

  @Override
  public Long mergeFinalResult(Long finalResult1, Long finalResult2) {
    return merge(finalResult1, finalResult2);
  }
}
