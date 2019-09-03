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

import javax.annotation.Nonnull;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.aggregation.AggregationFunctionContext;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.DefaultGroupByExecutor;
import org.apache.pinot.core.query.aggregation.groupby.GroupByExecutor;
import org.apache.pinot.core.startree.executor.StarTreeGroupByExecutor;


/**
 * The <code>AggregationOperator</code> class provides the operator for aggregation group-by query on a single segment.
 */
public class AggregationGroupByOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final String OPERATOR_NAME = "AggregationGroupByOperator";

  private final AggregationFunctionContext[] _functionContexts;
  private final GroupBy _groupBy;
  private final int _maxInitialResultHolderCapacity;
  private final int _numGroupsLimit;
  private final TransformOperator _transformOperator;
  private final long _numTotalRawDocs;
  private final boolean _useStarTree;

  private ExecutionStatistics _executionStatistics;

  public AggregationGroupByOperator(@Nonnull AggregationFunctionContext[] functionContexts, @Nonnull GroupBy groupBy,
      int maxInitialResultHolderCapacity, int numGroupsLimit, @Nonnull TransformOperator transformOperator,
      long numTotalRawDocs, boolean useStarTree) {
    _functionContexts = functionContexts;
    _groupBy = groupBy;
    _maxInitialResultHolderCapacity = maxInitialResultHolderCapacity;
    _numGroupsLimit = numGroupsLimit;
    _transformOperator = transformOperator;
    _numTotalRawDocs = numTotalRawDocs;
    _useStarTree = useStarTree;
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    int numDocsScanned = 0;

    // Perform aggregation group-by on all the blocks
    GroupByExecutor groupByExecutor;
    if (_useStarTree) {
      groupByExecutor =
          new StarTreeGroupByExecutor(_functionContexts, _groupBy, _maxInitialResultHolderCapacity, _numGroupsLimit,
              _transformOperator);
    } else {
      groupByExecutor =
          new DefaultGroupByExecutor(_functionContexts, _groupBy, _maxInitialResultHolderCapacity, _numGroupsLimit,
              _transformOperator);
    }
    TransformBlock transformBlock;
    while ((transformBlock = _transformOperator.nextBlock()) != null) {
      numDocsScanned += transformBlock.getNumDocs();
      groupByExecutor.process(transformBlock);
    }
    AggregationGroupByResult groupByResult = groupByExecutor.getResult();

    // Gather execution statistics
    long numEntriesScannedInFilter = _transformOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = numDocsScanned * _transformOperator.getNumColumnsProjected();
    _executionStatistics =
        new ExecutionStatistics(numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
            _numTotalRawDocs);

    // Build intermediate result block based on aggregation group-by result from the executor
    return new IntermediateResultsBlock(_functionContexts, groupByResult);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _executionStatistics;
  }
}
