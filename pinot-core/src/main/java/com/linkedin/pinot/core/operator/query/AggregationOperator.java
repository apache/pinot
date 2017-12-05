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

import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.ExecutionStatistics;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.operator.blocks.TransformBlock;
import com.linkedin.pinot.core.operator.transform.TransformExpressionOperator;
import com.linkedin.pinot.core.query.aggregation.AggregationExecutor;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.query.aggregation.DefaultAggregationExecutor;
import javax.annotation.Nonnull;


/**
 * The <code>AggregationOperator</code> class provides the operator for aggregation only query on a single segment.
 */
public class AggregationOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final String OPERATOR_NAME = "AggregationOperator";

  private final AggregationFunctionContext[] _aggregationFunctionContexts;
  private final TransformExpressionOperator _transformOperator;
  private final long _numTotalRawDocs;
  private ExecutionStatistics _executionStatistics;

  public AggregationOperator(@Nonnull AggregationFunctionContext[] aggregationFunctionContexts,
      @Nonnull TransformExpressionOperator transformOperator, long numTotalRawDocs) {
    _aggregationFunctionContexts = aggregationFunctionContexts;
    _transformOperator = transformOperator;
    _numTotalRawDocs = numTotalRawDocs;
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    int numDocsScanned = 0;

    // Perform aggregation on all the blocks.
    AggregationExecutor aggregationExecutor = new DefaultAggregationExecutor(_aggregationFunctionContexts);
    aggregationExecutor.init();
    TransformBlock transformBlock;
    while ((transformBlock = _transformOperator.nextBlock()) != null) {
      numDocsScanned += transformBlock.getNumDocs();
      aggregationExecutor.aggregate(transformBlock);
    }
    aggregationExecutor.finish();

    // Create execution statistics.
    long numEntriesScannedInFilter = _transformOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = numDocsScanned * _transformOperator.getNumProjectionColumns();
    _executionStatistics =
        new ExecutionStatistics(numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
            _numTotalRawDocs);

    // Build intermediate result block based on aggregation result from the executor.
    return new IntermediateResultsBlock(_aggregationFunctionContexts, aggregationExecutor.getResult(), false);
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
