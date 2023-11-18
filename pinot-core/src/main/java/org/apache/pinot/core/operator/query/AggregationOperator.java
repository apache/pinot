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
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.query.aggregation.AggregationExecutor;
import org.apache.pinot.core.query.aggregation.DefaultAggregationExecutor;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils.AggregationInfo;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.startree.executor.StarTreeAggregationExecutor;


/**
 * The <code>AggregationOperator</code> class provides the operator for aggregation only query on a single segment.
 */
@SuppressWarnings("rawtypes")
public class AggregationOperator extends BaseOperator<AggregationResultsBlock> {
  private static final String EXPLAIN_NAME = "AGGREGATE";

  private final QueryContext _queryContext;
  private final AggregationFunction[] _aggregationFunctions;
  private final BaseProjectOperator<?> _projectOperator;
  private final boolean _useStarTree;
  private final long _numTotalDocs;

  private int _numDocsScanned = 0;

  public AggregationOperator(QueryContext queryContext, AggregationInfo aggregationInfo, long numTotalDocs) {
    _queryContext = queryContext;
    _aggregationFunctions = queryContext.getAggregationFunctions();
    _projectOperator = aggregationInfo.getProjectOperator();
    _useStarTree = aggregationInfo.isUseStarTree();
    _numTotalDocs = numTotalDocs;
  }

  @Override
  protected AggregationResultsBlock getNextBlock() {
    // Perform aggregation on all the transform blocks
    AggregationExecutor aggregationExecutor;
    if (_useStarTree) {
      aggregationExecutor = new StarTreeAggregationExecutor(_aggregationFunctions);
    } else {
      aggregationExecutor = new DefaultAggregationExecutor(_aggregationFunctions);
    }
    ValueBlock valueBlock;
    while ((valueBlock = _projectOperator.nextBlock()) != null) {
      _numDocsScanned += valueBlock.getNumDocs();
      aggregationExecutor.aggregate(valueBlock);
    }

    // Build intermediate result block based on aggregation result from the executor
    return new AggregationResultsBlock(_aggregationFunctions, aggregationExecutor.getResult(), _queryContext);
  }

  @Override
  public List<BaseProjectOperator<?>> getChildOperators() {
    return Collections.singletonList(_projectOperator);
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    long numEntriesScannedInFilter = _projectOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = (long) _numDocsScanned * _projectOperator.getNumColumnsProjected();
    return new ExecutionStatistics(_numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
        _numTotalDocs);
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(aggregations:");
    if (_aggregationFunctions.length > 0) {
      stringBuilder.append(_aggregationFunctions[0].toExplainString());
      for (int i = 1; i < _aggregationFunctions.length; i++) {
        stringBuilder.append(", ").append(_aggregationFunctions[i].toExplainString());
      }
    }

    return stringBuilder.append(')').toString();
  }
}
