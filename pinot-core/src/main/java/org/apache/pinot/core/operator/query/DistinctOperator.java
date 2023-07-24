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
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.core.query.distinct.DistinctExecutorFactory;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * Operator for distinct queries on a single segment.
 */
public class DistinctOperator extends BaseOperator<DistinctResultsBlock> {
  private static final String EXPLAIN_NAME = "DISTINCT";

  private final IndexSegment _indexSegment;
  private final BaseProjectOperator<?> _projectOperator;
  private final QueryContext _queryContext;

  private int _numDocsScanned = 0;

  public DistinctOperator(IndexSegment indexSegment, BaseProjectOperator<?> projectOperator,
      QueryContext queryContext) {
    _indexSegment = indexSegment;
    _projectOperator = projectOperator;
    _queryContext = queryContext;
  }

  @Override
  protected DistinctResultsBlock getNextBlock() {
    DistinctExecutor executor = DistinctExecutorFactory.getDistinctExecutor(_projectOperator, _queryContext);
    ValueBlock valueBlock;
    while ((valueBlock = _projectOperator.nextBlock()) != null) {
      _numDocsScanned += valueBlock.getNumDocs();
      if (executor.process(valueBlock)) {
        break;
      }
    }
    return new DistinctResultsBlock(executor.getResult());
  }

  @Override
  public List<BaseProjectOperator<?>> getChildOperators() {
    return Collections.singletonList(_projectOperator);
  }

  @Override
  public IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    long numEntriesScannedInFilter = _projectOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = (long) _numDocsScanned * _projectOperator.getNumColumnsProjected();
    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    return new ExecutionStatistics(_numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
        numTotalDocs);
  }

  @Override
  public String toExplainString() {
    List<ExpressionContext> expressions = _queryContext.getSelectExpressions();
    int numExpressions = expressions.size();
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(keyColumns:");
    stringBuilder.append(expressions.get(0).toString());
    for (int i = 1; i < numExpressions; i++) {
      stringBuilder.append(", ").append(expressions.get(i).toString());
    }
    return stringBuilder.append(')').toString();
  }
}
