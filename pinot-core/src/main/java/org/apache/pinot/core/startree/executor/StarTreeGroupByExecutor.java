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
package org.apache.pinot.core.startree.executor;

import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.groupby.DefaultGroupByExecutor;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;


/**
 * The <code>StarTreeGroupByExecutor</code> class is the group-by executor for star-tree index.
 * <ul>
 *   <li>The column in function context is function-column pair</li>
 *   <li>No transform function in aggregation</li>
 *   <li>For <code>COUNT</code> aggregation function, we need to aggregate on the pre-aggregated column</li>
 * </ul>
 */
public class StarTreeGroupByExecutor extends DefaultGroupByExecutor {
  private final AggregationFunctionColumnPair[] _aggregationFunctionColumnPairs;

  public StarTreeGroupByExecutor(AggregationFunction[] aggregationFunctions, ExpressionContext[] groupByExpressions,
      int maxInitialResultHolderCapacity, int numGroupsLimit, TransformOperator transformOperator) {
    super(aggregationFunctions, groupByExpressions, maxInitialResultHolderCapacity, numGroupsLimit, transformOperator);

    int numAggregationFunctions = aggregationFunctions.length;
    _aggregationFunctionColumnPairs = new AggregationFunctionColumnPair[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      _aggregationFunctionColumnPairs[i] =
          AggregationFunctionUtils.getAggregationFunctionColumnPair(aggregationFunctions[i]);
    }
  }

  @Override
  protected void aggregate(TransformBlock transformBlock, int length, int functionIndex) {
    AggregationFunction aggregationFunction = _aggregationFunctions[functionIndex];
    GroupByResultHolder groupByResultHolder = _groupByResultHolders[functionIndex];
    Map<ExpressionContext, BlockValSet> blockValSetMap =
        AggregationFunctionUtils.getBlockValSetMap(_aggregationFunctionColumnPairs[functionIndex], transformBlock);
    if (_hasMVGroupByExpression) {
      aggregationFunction.aggregateGroupByMV(length, _mvGroupKeys, groupByResultHolder, blockValSetMap);
    } else {
      aggregationFunction.aggregateGroupBySV(length, _svGroupKeys, groupByResultHolder, blockValSetMap);
    }
  }
}
