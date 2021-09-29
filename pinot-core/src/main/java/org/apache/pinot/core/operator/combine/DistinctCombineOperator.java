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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * Combine operator for distinct queries.
 */
@SuppressWarnings("rawtypes")
public class DistinctCombineOperator extends BaseCombineOperator {
  private static final String OPERATOR_NAME = "DistinctCombineOperator";

  private final boolean _hasOrderBy;

  public DistinctCombineOperator(List<Operator> operators, QueryContext queryContext, ExecutorService executorService,
      long endTimeMs, int maxExecutionThreads) {
    super(operators, queryContext, executorService, endTimeMs, maxExecutionThreads);
    _hasOrderBy = queryContext.getOrderByExpressions() != null;
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  protected boolean isQuerySatisfied(IntermediateResultsBlock resultsBlock) {
    if (_hasOrderBy) {
      return false;
    }
    List<Object> result = resultsBlock.getAggregationResult();
    assert result != null && result.size() == 1 && result.get(0) instanceof DistinctTable;
    DistinctTable distinctTable = (DistinctTable) result.get(0);
    return distinctTable.size() >= _queryContext.getLimit();
  }

  @Override
  protected void mergeResultsBlocks(IntermediateResultsBlock mergedBlock, IntermediateResultsBlock blockToMerge) {
    // TODO: Use a separate way to represent DISTINCT instead of aggregation.
    List<Object> mergedResults = mergedBlock.getAggregationResult();
    assert mergedResults != null && mergedResults.size() == 1 && mergedResults.get(0) instanceof DistinctTable;
    DistinctTable mergedDistinctTable = (DistinctTable) mergedResults.get(0);

    List<Object> resultsToMerge = blockToMerge.getAggregationResult();
    assert resultsToMerge != null && resultsToMerge.size() == 1 && resultsToMerge.get(0) instanceof DistinctTable;
    DistinctTable distinctTableToMerge = (DistinctTable) resultsToMerge.get(0);

    // Convert the merged table into a main table if necessary in order to merge other tables
    if (!mergedDistinctTable.isMainTable()) {
      DistinctTable mainDistinctTable =
          new DistinctTable(distinctTableToMerge.getDataSchema(), _queryContext.getOrderByExpressions(),
              _queryContext.getLimit());
      mainDistinctTable.mergeTable(mergedDistinctTable);
      mergedBlock.setAggregationResults(Collections.singletonList(mainDistinctTable));
      mergedDistinctTable = mainDistinctTable;
    }

    mergedDistinctTable.mergeTable(distinctTableToMerge);
  }
}
