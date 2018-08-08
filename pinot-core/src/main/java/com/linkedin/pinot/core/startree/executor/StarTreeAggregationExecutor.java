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

import com.linkedin.pinot.core.operator.blocks.TransformBlock;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.query.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.DefaultAggregationExecutor;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionType;
import com.linkedin.pinot.core.startree.StarTreeUtils;
import com.linkedin.pinot.core.startree.v2.AggregationFunctionColumnPair;
import javax.annotation.Nonnull;


/**
 * The <code>StarTreeAggregationExecutor</code> class is the aggregation executor for star-tree index.
 * <ul>
 *   <li>The column in function context is function-column pair</li>
 *   <li>No UDF in aggregation</li>
 *   <li>For <code>COUNT</code> aggregation function, we need to aggregate on the pre-aggregated column</li>
 * </ul>
 */
public class StarTreeAggregationExecutor extends DefaultAggregationExecutor {

  public StarTreeAggregationExecutor(@Nonnull AggregationFunctionContext[] functionContexts) {
    super(StarTreeUtils.createStarTreeFunctionContexts(functionContexts));
  }

  @Override
  public void aggregate(@Nonnull TransformBlock transformBlock) {
    int length = transformBlock.getNumDocs();
    for (int i = 0; i < _numFunctions; i++) {
      AggregationFunction function = _functions[i];
      AggregationResultHolder resultHolder = _resultHolders[i];

      if (function.getType() == AggregationFunctionType.COUNT) {
        function.aggregate(length, resultHolder,
            transformBlock.getBlockValueSet(AggregationFunctionColumnPair.COUNT_STAR_COLUMN_NAME));
      } else {
        function.aggregate(length, resultHolder, transformBlock.getBlockValueSet(_expressions[i].getValue()));
      }
    }
  }
}
