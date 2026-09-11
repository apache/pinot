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


/// The `StarTreeAggregationExecutor` class is the aggregation executor for star-tree index.
///
/// - The column in function context is function-column pair
/// - No transform function in aggregation
/// - For `COUNT` aggregation function, we need to aggregate on the pre-aggregated column
@SuppressWarnings({"rawtypes", "unchecked"})
public class StarTreeAggregationExecutor extends DefaultAggregationExecutor {
  private final AggregationFunctionColumnPair[] _aggregationFunctionColumnPairs;

  /// Creates an executor over the pre-aggregated columns the query was routed to.
  ///
  /// `aggregationFunctionColumnPairs` must be the pairs the star-tree project operator was built with, because they
  /// depend on which star-tree was picked: a null-aware star-tree resolves `COUNT(column)` to `count__column` while a
  /// regular one resolves it to `count__*`.
  public StarTreeAggregationExecutor(AggregationFunction[] aggregationFunctions,
      AggregationFunctionColumnPair[] aggregationFunctionColumnPairs) {
    // StarTreeAggregationExecutor doesn't support pre-aggregated results.
    // So, we don't need to pass pre-aggregated results to the super class.
    super(aggregationFunctions);
    _aggregationFunctionColumnPairs = aggregationFunctionColumnPairs;
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
