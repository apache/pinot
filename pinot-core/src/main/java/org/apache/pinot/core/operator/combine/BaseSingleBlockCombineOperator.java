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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.operator.combine.function.CombineFunction;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * Base implementation of the combine operator.
 * <p>Combine operator uses multiple worker threads to process segments in parallel, and uses the main thread to merge
 * the results blocks from the processed segments. It can early-terminate the query to save the system resources if it
 * detects that the merged results can already satisfy the query, or the query is already errored out or timed out.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class BaseSingleBlockCombineOperator<T extends BaseResultsBlock>
    extends BaseCombineOperator<BaseResultsBlock> {
  protected final CombineFunction<T> _combineOperator;
  // Use an AtomicInteger to track the next operator to execute
  protected final AtomicInteger _nextOperatorId = new AtomicInteger();
  // Use a BlockingQueue to store the intermediate results blocks
  protected final BlockingQueue<BaseResultsBlock> _blockingQueue = new LinkedBlockingQueue<>();
  protected final AtomicLong _totalWorkerThreadCpuTimeNs = new AtomicLong(0);

  protected BaseSingleBlockCombineOperator(CombineFunction<T> combineOperator, List<Operator> operators,
      QueryContext queryContext, ExecutorService executorService) {
    super(operators, queryContext, executorService);
    _combineOperator = combineOperator;
  }

  /**
   * Executes query on one or more segments in a worker thread.
   */
  @Override
  public void processSegments() {
    int operatorId;
    while ((operatorId = _nextOperatorId.getAndIncrement()) < _numOperators) {
      Operator operator = _operators.get(operatorId);
      T resultsBlock;
      try {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).acquire();
        }
        resultsBlock = (T) operator.nextBlock();
      } finally {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).release();
        }
      }

      if (_combineOperator.isQuerySatisfied(resultsBlock)) {
        // Query is satisfied, skip processing the remaining segments
        _blockingQueue.offer(resultsBlock);
        return;
      } else {
        _blockingQueue.offer(resultsBlock);
      }
    }
  }

  /**
   * Invoked when {@link #processSegments()} throws exception/error.
   */
  public void onException(Throwable t) {
    _blockingQueue.offer(new ExceptionResultsBlock(t));
  }

  /**
   * Invoked when {@link #processSegments()} is finished (called in the finally block).
   */
  public void onFinish() {
  }

  /**
   * Merges the results from the worker threads into a results block.
   */
  @Override
  public BaseResultsBlock mergeResults()
      throws Exception {
    T mergedBlock = null;
    int numBlocksMerged = 0;
    long endTimeMs = _queryContext.getEndTimeMs();
    while (numBlocksMerged < _numOperators) {
      // Timeout has reached, shouldn't continue to process. `_blockingQueue.poll` will continue to return blocks even
      // if negative timeout is provided; therefore an extra check is needed
      long waitTimeMs = endTimeMs - System.currentTimeMillis();
      if (waitTimeMs <= 0) {
        return getTimeoutResultsBlock(numBlocksMerged);
      }
      BaseResultsBlock blockToMerge = _blockingQueue.poll(waitTimeMs, TimeUnit.MILLISECONDS);
      if (blockToMerge == null) {
        return getTimeoutResultsBlock(numBlocksMerged);
      }
      if (blockToMerge.getProcessingExceptions() != null) {
        // Caught exception while processing segment, skip merging the remaining results blocks and directly return the
        // exception
        return blockToMerge;
      }
      if (mergedBlock == null) {
        mergedBlock = _combineOperator.convertToMergeableBlock((T) blockToMerge);
      } else {
        _combineOperator.mergeResultsBlocks(mergedBlock, (T) blockToMerge);
      }
      numBlocksMerged++;
      if (_combineOperator.isQuerySatisfied(mergedBlock)) {
        // Query is satisfied, skip merging the remaining results blocks
        return mergedBlock;
      }
    }
    return mergedBlock;
  }
}
