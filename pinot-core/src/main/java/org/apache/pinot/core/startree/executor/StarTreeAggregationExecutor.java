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

import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.query.aggregation.AggregationFunctionContext;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.DefaultAggregationExecutor;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.startree.StarTreeUtils;
import org.apache.pinot.core.startree.v2.AggregationFunctionColumnPair;


/**
 * The <code>StarTreeAggregationExecutor</code> class is the aggregation executor for star-tree index.
 * <ul>
 *   <li>The column in function context is function-column pair</li>
 *   <li>No UDF in aggregation</li>
 *   <li>For <code>COUNT</code> aggregation function, we need to aggregate on the pre-aggregated column</li>
 * </ul>
 */
public class StarTreeAggregationExecutor extends DefaultAggregationExecutor {

  public StarTreeAggregationExecutor(AggregationFunctionContext[] functionContexts) {
    super(StarTreeUtils.createStarTreeFunctionContexts(functionContexts));
  }

  @Override
  public void aggregate(TransformBlock transformBlock) {
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
