/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.aggregation;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.ExecutionStatistics;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;
import java.io.Serializable;
import java.util.List;


/**
 * This class implements the aggregation operator, extends BaseOperator.
 */
public class AggregationOperator extends BaseOperator {

  private final AggregationExecutor _aggregationExecutor;
  private final List<AggregationInfo> _aggregationInfoList;
  private final MProjectionOperator _projectionOperator;
  private final long _numTotalRawDocs;
  private int _nextBlockCallCounter = 0;
  private ExecutionStatistics _executionStatistics;

  /**
   * Constructor for the class.
   *
   * @param aggregationsInfoList List of AggregationInfo (contains context for applying aggregation functions).
   * @param executor Aggregation Executor
   * @param projectionOperator Projection operator.
   * @param numTotalRawDocs Number of total raw documents.
   */
  public AggregationOperator(List<AggregationInfo> aggregationsInfoList, AggregationExecutor executor,
      MProjectionOperator projectionOperator,long numTotalRawDocs) {
    Preconditions.checkArgument((aggregationsInfoList != null) && (aggregationsInfoList.size() > 0));
    Preconditions.checkNotNull(executor);
    Preconditions.checkNotNull(projectionOperator);
    this._aggregationInfoList = aggregationsInfoList;
    _aggregationExecutor = executor;
    _projectionOperator = projectionOperator;
    _numTotalRawDocs = numTotalRawDocs;
  }

  /**
   * Returns the next ResultBlock containing the result of aggregation group by.
   */
  @Override
  public Block getNextBlock() {
    return getNextBlock(new BlockId(_nextBlockCallCounter++));
  }

  /**
   * {@inheritDoc}
   * Returns the nextBlock for the given docId.
   */
  @Override
  public Block getNextBlock(BlockId blockId) {
    if (blockId.getId() > 0) {
      return null;
    }

    int numDocsScanned = 0;

    // Perform aggregation on all the blocks.
    _aggregationExecutor.init();
    ProjectionBlock projectionBlock;
    while ((projectionBlock = (ProjectionBlock) _projectionOperator.nextBlock()) != null) {
      numDocsScanned += projectionBlock.getNumDocs();
      _aggregationExecutor.aggregate(projectionBlock);
    }
    _aggregationExecutor.finish();

    // Create execution statistics.
    long numEntriesScannedInFilter = _projectionOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = numDocsScanned * _projectionOperator.getNumProjectionColumns();
    _executionStatistics =
        new ExecutionStatistics(numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
            _numTotalRawDocs);

    // Build intermediate result block based on aggregation result from the executor.
    List<Serializable> aggregationResults = _aggregationExecutor.getResult();
    return new IntermediateResultsBlock(AggregationFunctionFactory.getAggregationFunction(_aggregationInfoList),
        aggregationResults);
  }

  @Override
  public boolean open() {
    _projectionOperator.open();
    return true;
  }

  @Override
  public boolean close() {
    _projectionOperator.close();
    return true;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _executionStatistics;
  }
}
