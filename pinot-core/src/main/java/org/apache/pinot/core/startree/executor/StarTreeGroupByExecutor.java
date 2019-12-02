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

import javax.annotation.Nonnull;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.aggregation.AggregationFunctionContext;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.DefaultGroupByExecutor;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.startree.StarTreeUtils;
import org.apache.pinot.core.startree.v2.AggregationFunctionColumnPair;


/**
 * The <code>StarTreeGroupByExecutor</code> class is the group-by executor for star-tree index.
 * <ul>
 *   <li>The column in function context is function-column pair</li>
 *   <li>No UDF in aggregation</li>
 *   <li>For <code>COUNT</code> aggregation function, we need to aggregate on the pre-aggregated column</li>
 * </ul>
 */
public class StarTreeGroupByExecutor extends DefaultGroupByExecutor {

  public StarTreeGroupByExecutor(@Nonnull AggregationFunctionContext[] functionContexts, @Nonnull GroupBy groupBy,
      int maxInitialResultHolderCapacity, int numGroupsLimit, @Nonnull TransformOperator transformOperator) {
    super(StarTreeUtils.createStarTreeFunctionContexts(functionContexts), groupBy, maxInitialResultHolderCapacity,
        numGroupsLimit, transformOperator);
  }

  @Override
  protected void aggregate(@Nonnull TransformBlock transformBlock, int length, int functionIndex) {
    AggregationFunction function = _functions[functionIndex];
    GroupByResultHolder resultHolder = _resultHolders[functionIndex];

    if (function.getType() == AggregationFunctionType.COUNT) {
      BlockValSet blockValueSet = transformBlock.getBlockValueSet(AggregationFunctionColumnPair.COUNT_STAR_COLUMN_NAME);
      if (_hasMVGroupByExpression) {
        function.aggregateGroupByMV(length, _mvGroupKeys, resultHolder, blockValueSet);
      } else {
        function.aggregateGroupBySV(length, _svGroupKeys, resultHolder, blockValueSet);
      }
    } else {
      BlockValSet blockValueSet = transformBlock.getBlockValueSet(_aggregationExpressions[functionIndex].getValue());
      if (_hasMVGroupByExpression) {
        function.aggregateGroupByMV(length, _mvGroupKeys, resultHolder, blockValueSet);
      } else {
        function.aggregateGroupBySV(length, _svGroupKeys, resultHolder, blockValueSet);
      }
    }
  }
}
