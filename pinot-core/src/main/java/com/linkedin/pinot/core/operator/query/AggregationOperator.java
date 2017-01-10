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
package com.linkedin.pinot.core.operator.query;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.ExecutionStatistics;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.aggregation.AggregationExecutor;
import com.linkedin.pinot.core.operator.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.operator.aggregation.DefaultAggregationExecutor;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import javax.annotation.Nonnull;


/**
 * The <code>AggregationOperator</code> class provides the operator for aggregation only query on a single segment.
 */
public class AggregationOperator extends BaseOperator {
  private final AggregationFunctionContext[] _aggregationFunctionContexts;
  private final MProjectionOperator _projectionOperator;
  private final long _numTotalRawDocs;
  private ExecutionStatistics _executionStatistics;

  public AggregationOperator(@Nonnull AggregationFunctionContext[] aggregationFunctionContexts,
      @Nonnull MProjectionOperator projectionOperator, long numTotalRawDocs) {
    _aggregationFunctionContexts = aggregationFunctionContexts;
    _projectionOperator = projectionOperator;
    _numTotalRawDocs = numTotalRawDocs;
  }

  @Override
  public boolean open() {
    _projectionOperator.open();
    return true;
  }

  @Override
  public Block getNextBlock() {
    int numDocsScanned = 0;

    // Perform aggregation on all the blocks.
    AggregationExecutor aggregationExecutor = new DefaultAggregationExecutor(_aggregationFunctionContexts);
    aggregationExecutor.init();
    ProjectionBlock projectionBlock;
    while ((projectionBlock = (ProjectionBlock) _projectionOperator.nextBlock()) != null) {
      numDocsScanned += projectionBlock.getNumDocs();
      aggregationExecutor.aggregate(projectionBlock);
    }
    aggregationExecutor.finish();

    // Create execution statistics.
    long numEntriesScannedInFilter = _projectionOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = numDocsScanned * _projectionOperator.getNumProjectionColumns();
    _executionStatistics =
        new ExecutionStatistics(numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
            _numTotalRawDocs);

    // Build intermediate result block based on aggregation result from the executor.
    return new IntermediateResultsBlock(_aggregationFunctionContexts, aggregationExecutor.getResult(), false);
  }

  @Override
  public Block getNextBlock(BlockId blockId) {
    throw new UnsupportedOperationException();
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
