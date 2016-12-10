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
package com.linkedin.pinot.core.operator.aggregation.groupby;

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
import java.util.List;


/**
 * Operator class for implementing aggregation group by functionality.
 * Extends the BaseOperator class.
 *
 */
public class AggregationGroupByOperator extends BaseOperator {

  private final GroupByExecutor _groupByExecutor;
  private final MProjectionOperator _projectionOperator;
  private final long _numTotalRawDocs;
  private final List<AggregationInfo> _aggregationInfoList;
  private int _nextBlockCallCounter = 0;
  private ExecutionStatistics _executionStatistics;

  /**
   * Constructor for the class.
   * @param aggregationsInfoList List of AggregationInfo (contains context for applying aggregation functions).
   * @param defaultGroupByExecutor default group by executor
   * @param defaultGroupByExecutor
   * @param projectionOperator Projection
   * @param numTotalRawDocs Number of total raw documents.
   */
  // aggregationInfoList parameter is required to support legacy API in getNextBlock()
  public AggregationGroupByOperator(List<AggregationInfo> aggregationsInfoList,
      DefaultGroupByExecutor defaultGroupByExecutor, MProjectionOperator projectionOperator,
      long numTotalRawDocs) {
    Preconditions.checkArgument(aggregationsInfoList != null && aggregationsInfoList.size() > 0);
    Preconditions.checkNotNull(defaultGroupByExecutor);
    Preconditions.checkNotNull(projectionOperator);

    _projectionOperator = projectionOperator;
    _aggregationInfoList = aggregationsInfoList;
    _numTotalRawDocs = numTotalRawDocs;
    _groupByExecutor = defaultGroupByExecutor;
  }

  /**
   * Returns the next ResultBlock containing the result of aggregation group by.
   * @return Return next block of aggregation group-by
   */
  @Override
  public Block getNextBlock() {
    return getNextBlock(new BlockId(_nextBlockCallCounter++));
  }

  /**
   * This method is currently not supported.
   */
  @Override
  public Block getNextBlock(BlockId blockId) {
    if (blockId.getId() > 0) {
      return null;
    }

    int numDocsScanned = 0;

    _groupByExecutor.init();
    ProjectionBlock projectionBlock;
    while ((projectionBlock = (ProjectionBlock) _projectionOperator.nextBlock()) != null) {
      numDocsScanned += projectionBlock.getNumDocs();
      _groupByExecutor.process(projectionBlock);
    }
    _groupByExecutor.finish();

    // Create execution statistics.
    long numEntriesScannedInFilter = _projectionOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = numDocsScanned * _projectionOperator.getNumProjectionColumns();
    _executionStatistics =
        new ExecutionStatistics(numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
            _numTotalRawDocs);

    AggregationGroupByResult aggregationGroupByResult = _groupByExecutor.getResult();
    return new IntermediateResultsBlock(AggregationFunctionFactory.getAggregationFunction(_aggregationInfoList),
        aggregationGroupByResult);
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
