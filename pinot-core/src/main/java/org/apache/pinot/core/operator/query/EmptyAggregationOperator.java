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
package org.apache.pinot.core.operator.query;

import java.util.Collections;
import java.util.List;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * The <code>EmptyAggregationOperator</code> provides a way to short circuit aggregation only queries (no group by)
 * with a LIMIT of zero.
 */
public class EmptyAggregationOperator extends BaseOperator<AggregationResultsBlock> {

  private static final String EXPLAIN_NAME = "AGGREGATE_EMPTY";
  private final QueryContext _queryContext;
  private final ExecutionStatistics _executionStatistics;

  public EmptyAggregationOperator(QueryContext queryContext, int numTotalDocs) {
    _queryContext = queryContext;
    _executionStatistics = new ExecutionStatistics(0, 0, 0, numTotalDocs);
  }

  @Override
  protected AggregationResultsBlock getNextBlock() {
    return new AggregationResultsBlock(_queryContext.getAggregationFunctions(), Collections.emptyList(), _queryContext);
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _executionStatistics;
  }
}
