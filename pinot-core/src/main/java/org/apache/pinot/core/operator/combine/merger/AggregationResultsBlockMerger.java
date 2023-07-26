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
package org.apache.pinot.core.operator.combine.merger;

import java.util.List;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;


@SuppressWarnings({"rawtypes", "unchecked"})
public class AggregationResultsBlockMerger implements ResultsBlockMerger<AggregationResultsBlock> {

  public AggregationResultsBlockMerger(QueryContext queryContext) {
  }

  @Override
  public void mergeResultsBlocks(AggregationResultsBlock mergedBlock, AggregationResultsBlock blockToMerge) {
    AggregationFunction[] aggregationFunctions = mergedBlock.getAggregationFunctions();
    List<Object> mergedResults = mergedBlock.getResults();
    List<Object> resultsToMerge = blockToMerge.getResults();
    assert aggregationFunctions != null && mergedResults != null && resultsToMerge != null;

    int numAggregationFunctions = aggregationFunctions.length;
    for (int i = 0; i < numAggregationFunctions; i++) {
      mergedResults.set(i, aggregationFunctions[i].merge(mergedResults.get(i), resultsToMerge.get(i)));
    }
  }
}
