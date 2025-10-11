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


public class MinLongAggregationFunction extends NullableSingleInputAggregationFunction<Long, Long> {
  protected static final Long DEFAULT_VALUE = Long.MAX_VALUE;

  public MinLongAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    this(verifySingleArgument(arguments, "MINLONG"), nullHandlingEnabled);
  }

  protected MinLongAggregationFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression, nullHandlingEnabled);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.MINLONG;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    if (_nullHandlingEnabled) {
      return new ObjectAggregationResultHolder();
    }
    return new LongAggregateResultHolder(DEFAULT_VALUE);
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    if (_nullHandlingEnabled) {
      return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
    }
    return new LongGroupByResultHolder(initialCapacity, maxCapacity, DEFAULT_VALUE);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    long[] values = blockValSet.getLongValuesSV();

    Long min = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
      long innerMin = values[from];
      for (int i = from; i < to; i++) {
        innerMin = Math.min(innerMin, values[i]);
      }
      return acum == null ? innerMin : Math.min(acum, innerMin);
    });

    updateAggregationResultHolder(aggregationResultHolder, min);
  }

  protected void updateAggregationResultHolder(AggregationResultHolder aggregationResultHolder, Long min) {
    if (min != null) {
      if (_nullHandlingEnabled) {
        Long otherMin = aggregationResultHolder.getResult();
        aggregationResultHolder.setValue(otherMin == null ? min : Math.min(min, otherMin));
      } else {
        long otherMin = aggregationResultHolder.getLongResult();
        aggregationResultHolder.setValue(Math.min(min, otherMin));
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
          if (result == null || value < result) {
            groupByResultHolder.setValueForKey(groupKey, value);
          }
        }
      });
    } else {
      for (int i = 0; i < length; i++) {
        long value = valueArray[i];
        int groupKey = groupKeyArray[i];
        if (value < groupByResultHolder.getLongResult(groupKey)) {
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
            if (result == null || value < result) {
              groupByResultHolder.setValueForKey(groupKey, value);
            }
          }
        }
      });
    } else {
      for (int i = 0; i < length; i++) {
        long value = valueArray[i];
        for (int groupKey : groupKeysArray[i]) {
          if (value < groupByResultHolder.getLongResult(groupKey)) {
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
  public Long merge(Long intermediateMinResult1, Long intermediateMinResult2) {
    if (_nullHandlingEnabled) {
      if (intermediateMinResult1 == null) {
        return intermediateMinResult2;
      }
      if (intermediateMinResult2 == null) {
        return intermediateMinResult1;
      }
    }

    if (intermediateMinResult1 < intermediateMinResult2) {
      return intermediateMinResult1;
    }
    return intermediateMinResult2;
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
