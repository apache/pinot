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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.DoubleAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.DoubleGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.roaringbitmap.RoaringBitmap;


public class CountAggregationFunction extends BaseSingleInputAggregationFunction<Long, Long> {
  private static final String COUNT_STAR_RESULT_COLUMN_NAME = "count(*)";
  private static final double DEFAULT_INITIAL_VALUE = 0.0;
  // Special expression used by star-tree to pass in BlockValSet
  private static final ExpressionContext STAR_TREE_COUNT_STAR_EXPRESSION =
      ExpressionContext.forIdentifier(AggregationFunctionColumnPair.STAR);

  private final boolean _nullHandlingEnabled;

  public CountAggregationFunction(ExpressionContext expression) {
    this(expression, false);
  }

  public CountAggregationFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression);
    // Consider null values only when null handling is enabled and function is not COUNT(*)
    // Note COUNT on any literal gives same result as COUNT(*)
    // So allow for identifiers that are not * and functions, disable for literals and *
    _nullHandlingEnabled = nullHandlingEnabled
            && ((expression.getType() == ExpressionContext.Type.IDENTIFIER && !expression.getIdentifier().equals("*"))
            || (expression.getType() == ExpressionContext.Type.FUNCTION));
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.COUNT;
  }

  @Override
  public String getResultColumnName() {
    return _nullHandlingEnabled ? super.getResultColumnName() : COUNT_STAR_RESULT_COLUMN_NAME;
  }

  @Override
  public List<ExpressionContext> getInputExpressions() {
    return _nullHandlingEnabled ? super.getInputExpressions() : Collections.emptyList();
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new DoubleAggregationResultHolder(DEFAULT_INITIAL_VALUE);
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new DoubleGroupByResultHolder(initialCapacity, maxCapacity, DEFAULT_INITIAL_VALUE);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    if (blockValSetMap.isEmpty()) {
      aggregationResultHolder.setValue(aggregationResultHolder.getDoubleResult() + length);
    } else if (_nullHandlingEnabled) {
      assert blockValSetMap.size() == 1;
      BlockValSet blockValSet = blockValSetMap.values().iterator().next();
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      int numNulls = 0;
      if (nullBitmap != null) {
        numNulls = nullBitmap.getCardinality();
      }
      assert numNulls <= length;
      aggregationResultHolder.setValue(aggregationResultHolder.getDoubleResult() + (length - numNulls));
    } else {
      // Star-tree pre-aggregated values
      long[] valueArray = blockValSetMap.get(STAR_TREE_COUNT_STAR_EXPRESSION).getLongValuesSV();
      long count = 0;
      for (int i = 0; i < length; i++) {
        count += valueArray[i];
      }
      aggregationResultHolder.setValue(aggregationResultHolder.getDoubleResult() + count);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    if (blockValSetMap.isEmpty()) {
      for (int i = 0; i < length; i++) {
        int groupKey = groupKeyArray[i];
        groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + 1);
      }
    } else if (_nullHandlingEnabled) {
      // In Presto, null values are not counted:
      // SELECT count(id) as count, key FROM (VALUES (null, 1), (null, 1), (null, 2), (1, 3), (null, 3)) AS t(id, key)
      // GROUP BY key ORDER BY key DESC;
      // count | key
      //-------+-----
      //     1 |   3
      //     0 |   2
      //     0 |   1
      assert blockValSetMap.size() == 1;
      BlockValSet blockValSet = blockValSetMap.values().iterator().next();
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        if (nullBitmap.getCardinality() == length) {
          return;
        }
        for (int i = 0; i < length; i++) {
          if (!nullBitmap.contains(i)) {
            int groupKey = groupKeyArray[i];
            groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + 1);
          }
        }
      } else {
        for (int i = 0; i < length; i++) {
          int groupKey = groupKeyArray[i];
          groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + 1);
        }
      }
    } else {
      // Star-tree pre-aggregated values
      long[] valueArray = blockValSetMap.get(STAR_TREE_COUNT_STAR_EXPRESSION).getLongValuesSV();
      for (int i = 0; i < length; i++) {
        int groupKey = groupKeyArray[i];
        groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + valueArray[i]);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    if (blockValSetMap.isEmpty() || !blockValSetMap.containsKey(STAR_TREE_COUNT_STAR_EXPRESSION)) {
      for (int i = 0; i < length; i++) {
        for (int groupKey : groupKeysArray[i]) {
          groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + 1);
        }
      }
    } else {
      // Star-tree pre-aggregated values
      long[] valueArray = blockValSetMap.get(STAR_TREE_COUNT_STAR_EXPRESSION).getLongValuesSV();
      for (int i = 0; i < length; i++) {
        long value = valueArray[i];
        for (int groupKey : groupKeysArray[i]) {
          groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + value);
        }
      }
    }
  }

  @Override
  public Long extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return (long) aggregationResultHolder.getDoubleResult();
  }

  @Override
  public Long extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return (long) groupByResultHolder.getDoubleResult(groupKey);
  }

  @Override
  public Long merge(Long intermediateResult1, Long intermediateResult2) {
    return intermediateResult1 + intermediateResult2;
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
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(getType().getName()).append('(');
    int numArguments = getInputExpressions().size();
    if (numArguments > 0) {
      stringBuilder.append(getInputExpressions().get(0).toString());
      for (int i = 1; i < numArguments; i++) {
        stringBuilder.append(", ").append(getInputExpressions().get(i).toString());
      }
    } else {
      stringBuilder.append("*");
    }
    return stringBuilder.append(')').toString();
  }
}
