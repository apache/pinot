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
package org.apache.pinot.core.operator.combine;

import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * Combine operator for aggregation only queries.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class AggregationOnlyCombineOperator extends BaseCombineOperator {
  private static final String OPERATOR_NAME = "AggregationOnlyCombineOperator";

  public AggregationOnlyCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService, long endTimeMs) {
    super(operators, queryContext, executorService, endTimeMs);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  protected void mergeResultsBlocks(IntermediateResultsBlock mergedBlock, IntermediateResultsBlock blockToMerge) {
    AggregationFunction[] aggregationFunctions = mergedBlock.getAggregationFunctions();
    List<Object> mergedResults = mergedBlock.getAggregationResult();
    List<Object> resultsToMerge = blockToMerge.getAggregationResult();
    assert aggregationFunctions != null && mergedResults != null && resultsToMerge != null;

    int numAggregationFunctions = aggregationFunctions.length;
    for (int i = 0; i < numAggregationFunctions; i++) {
      mergedResults.set(i, aggregationFunctions[i].merge(mergedResults.get(i), resultsToMerge.get(i)));
    }
  }
}
