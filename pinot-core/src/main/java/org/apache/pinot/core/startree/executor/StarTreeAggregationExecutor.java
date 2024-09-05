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

import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.aggregation.DefaultAggregationExecutor;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;


/**
 * The <code>StarTreeAggregationExecutor</code> class is the aggregation executor for star-tree index.
 * <ul>
 *   <li>The column in function context is function-column pair</li>
 *   <li>No transform function in aggregation</li>
 *   <li>For <code>COUNT</code> aggregation function, we need to aggregate on the pre-aggregated column</li>
 * </ul>
 */
public class StarTreeAggregationExecutor extends DefaultAggregationExecutor {
  private final AggregationFunctionColumnPair[] _aggregationFunctionColumnPairs;

  public StarTreeAggregationExecutor(AggregationFunction[] aggregationFunctions) {
    super(aggregationFunctions);

    int numAggregationFunctions = aggregationFunctions.length;
    _aggregationFunctionColumnPairs = new AggregationFunctionColumnPair[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      _aggregationFunctionColumnPairs[i] =
          AggregationFunctionUtils.getStoredFunctionColumnPair(aggregationFunctions[i]);
    }
  }

  @Override
  public void aggregate(ValueBlock valueBlock) {
    int numAggregationFunctions = _aggregationFunctions.length;
    int length = valueBlock.getNumDocs();
    for (int i = 0; i < numAggregationFunctions; i++) {
      _aggregationFunctions[i].aggregate(length, _aggregationResultHolders[i],
          AggregationFunctionUtils.getBlockValSetMap(_aggregationFunctionColumnPairs[i], valueBlock));
    }
  }
}
