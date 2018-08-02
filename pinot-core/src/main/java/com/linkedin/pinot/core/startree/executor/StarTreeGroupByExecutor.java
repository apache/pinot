/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.startree.executor;

import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.blocks.TransformBlock;
import com.linkedin.pinot.core.operator.transform.TransformOperator;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionType;
import com.linkedin.pinot.core.query.aggregation.groupby.DefaultGroupByExecutor;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.startree.StarTreeUtils;
import com.linkedin.pinot.core.startree.v2.AggregationFunctionColumnPair;
import javax.annotation.Nonnull;


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
