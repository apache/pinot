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
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * Combine operator for distinct queries.
 */
@SuppressWarnings("rawtypes")
public class DistinctCombineOperator extends BaseCombineOperator<DistinctResultsBlock> {
  private static final String EXPLAIN_NAME = "COMBINE_DISTINCT";

  private final boolean _hasOrderBy;

  public DistinctCombineOperator(List<Operator> operators, QueryContext queryContext, ExecutorService executorService) {
    super(operators, queryContext, executorService);
    _hasOrderBy = queryContext.getOrderByExpressions() != null;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected boolean isQuerySatisfied(DistinctResultsBlock resultsBlock) {
    if (_hasOrderBy) {
      return false;
    }
    return resultsBlock.getDistinctTable().size() >= _queryContext.getLimit();
  }

  @Override
  protected void mergeResultsBlocks(DistinctResultsBlock mergedBlock, DistinctResultsBlock blockToMerge) {
    DistinctTable mergedDistinctTable = mergedBlock.getDistinctTable();
    DistinctTable distinctTableToMerge = blockToMerge.getDistinctTable();
    assert mergedDistinctTable != null && distinctTableToMerge != null;

    // Convert the merged table into a main table if necessary in order to merge other tables
    if (!mergedDistinctTable.isMainTable()) {
      DistinctTable mainDistinctTable =
          new DistinctTable(distinctTableToMerge.getDataSchema(), _queryContext.getOrderByExpressions(),
              _queryContext.getLimit(), _queryContext.isNullHandlingEnabled());
      mainDistinctTable.mergeTable(mergedDistinctTable);
      mergedBlock.setDistinctTable(mainDistinctTable);
      mergedDistinctTable = mainDistinctTable;
    }

    mergedDistinctTable.mergeTable(distinctTableToMerge);
  }
}
