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
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.LongAggregateResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.LongGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class MaxLongAggregationFunction extends NullableSingleInputAggregationFunction<Long, Long> {
  protected static final long DEFAULT_INITIAL_VALUE = Long.MIN_VALUE;

  public MaxLongAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    this(verifySingleArgument(arguments, "MAXLONG"), nullHandlingEnabled);
  }

  protected MaxLongAggregationFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression, nullHandlingEnabled);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.MAXLONG;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    if (_nullHandlingEnabled) {
      return new ObjectAggregationResultHolder();
    }
    return new LongAggregateResultHolder(DEFAULT_INITIAL_VALUE);
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    if (_nullHandlingEnabled) {
      return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
    }
    return new LongGroupByResultHolder(initialCapacity, maxCapacity, DEFAULT_INITIAL_VALUE);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    long[] values = blockValSet.getLongValuesSV();

    Long max = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
      long innerMax = values[from];
      for (int i = from; i < to; i++) {
        innerMax = Math.max(innerMax, values[i]);
      }
      return acum == null ? innerMax : Math.max(acum, innerMax);
    });

    updateAggregationResultHolder(aggregationResultHolder, max);
  }

  protected void updateAggregationResultHolder(AggregationResultHolder aggregationResultHolder, Long max) {
    if (max != null) {
      if (_nullHandlingEnabled) {
        Long otherMax = aggregationResultHolder.getResult();
        aggregationResultHolder.setValue(otherMax == null ? max : Math.max(max, otherMax));
      } else {
        long otherMax = aggregationResultHolder.getLongResult();
        aggregationResultHolder.setValue(Math.max(max, otherMax));
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    long[] valueArray = blockValSet.getLongValuesSV();

    if (_nullHandlingEnabled) {
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          long value = valueArray[i];
          int groupKey = groupKeyArray[i];
          Long result = groupByResultHolder.getResult(groupKey);
          if (result == null || value > result) {
            groupByResultHolder.setValueForKey(groupKey, value);
          }
        }
      });
    } else {
      for (int i = 0; i < length; i++) {
        long value = valueArray[i];
        int groupKey = groupKeyArray[i];
        if (value > groupByResultHolder.getLongResult(groupKey)) {
          groupByResultHolder.setValueForKey(groupKey, value);
        }
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    long[] valueArray = blockValSet.getLongValuesSV();

    if (_nullHandlingEnabled) {
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          long value = valueArray[i];
          for (int groupKey : groupKeysArray[i]) {
            Long result = groupByResultHolder.getResult(groupKey);
            if (result == null || value > result) {
              groupByResultHolder.setValueForKey(groupKey, value);
            }
          }
        }
      });
    } else {
      for (int i = 0; i < length; i++) {
        long value = valueArray[i];
        for (int groupKey : groupKeysArray[i]) {
          if (value > groupByResultHolder.getLongResult(groupKey)) {
            groupByResultHolder.setValueForKey(groupKey, value);
          }
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
  public Long merge(Long intermediateMaxResult1, Long intermediateMaxResult2) {
    if (_nullHandlingEnabled) {
      if (intermediateMaxResult1 == null) {
        return intermediateMaxResult2;
      }
      if (intermediateMaxResult2 == null) {
        return intermediateMaxResult1;
      }
    }

    if (intermediateMaxResult1 > intermediateMaxResult2) {
      return intermediateMaxResult1;
    }
    return intermediateMaxResult2;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.LONG;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.LONG;
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
