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
package org.apache.pinot.core.operator.streaming;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;
import org.apache.pinot.core.operator.combine.BaseCombineOperator;
import org.apache.pinot.core.operator.combine.CombineOperatorUtils;
import org.apache.pinot.core.operator.combine.merger.ResultsBlockMerger;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class BaseStreamingCombineOperator<T extends BaseResultsBlock>
    extends BaseCombineOperator<BaseResultsBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseStreamingCombineOperator.class);

  /**
   * Special results block to indicate that this is the last results block for a child operator in the list
   */
  public static final MetadataResultsBlock LAST_RESULTS_BLOCK = new MetadataResultsBlock();

  // Use a BlockingQueue to store the intermediate results blocks
  protected final BlockingQueue<BaseResultsBlock> _blockingQueue = new LinkedBlockingQueue<>();
  protected final ResultsBlockMerger<T> _resultsBlockMerger;

  protected int _numOperatorsFinished;
  protected BaseResultsBlock _exceptionBlock;

  public BaseStreamingCombineOperator(ResultsBlockMerger<T> resultsBlockMerger, List<Operator> operators,
      QueryContext queryContext, ExecutorService executorService) {
    super(operators, queryContext, executorService);
    _resultsBlockMerger = resultsBlockMerger;
  }

  @Override
  protected BaseResultsBlock getNextBlock() {
    long endTimeMs = _queryContext.getEndTimeMs();
    // TODO: Early terminate when query is satisfied
    while (_exceptionBlock == null && _numOperatorsFinished < _numOperators) {
      try {
        BaseResultsBlock resultsBlock =
            _blockingQueue.poll(endTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);

        if (resultsBlock == null) {
          // Query times out, skip streaming the remaining results blocks
          LOGGER.error("Timed out while polling results block (query: {})", _queryContext);
          _exceptionBlock = new ExceptionResultsBlock(
              QueryException.getException(QueryException.EXECUTION_TIMEOUT_ERROR,
                  new TimeoutException("Timed out while polling results block")));
          return _exceptionBlock;
        }
        if (resultsBlock.getProcessingExceptions() != null) {
          // Caught exception while processing segment, skip streaming the remaining results blocks and directly return
          // the exception
          _exceptionBlock = resultsBlock;
          return _exceptionBlock;
        }
        if (resultsBlock == LAST_RESULTS_BLOCK) {
          // Caught LAST_RESULTS_BLOCK from a specific task, indicated it has finished.
          // Skip returning this metadata block and continue to process the next from the _blockingQueue.
          _numOperatorsFinished++;
          continue;
        }
        return resultsBlock;
      } catch (Exception e) {
        LOGGER.error("Caught exception while merging results blocks (query: {})", _queryContext, e);
        _exceptionBlock = new ExceptionResultsBlock(QueryException.getException(QueryException.INTERNAL_ERROR, e));
        return _exceptionBlock;
      }
    }
    // Setting the execution stats for the final return
    BaseResultsBlock finalBlock = new MetadataResultsBlock();
    int numServerThreads = Math.min(_numTasks, ResourceManager.DEFAULT_QUERY_WORKER_THREADS);
    CombineOperatorUtils.setExecutionStatistics(finalBlock, _operators, _totalWorkerThreadCpuTimeNs.get(),
        numServerThreads);
    return finalBlock;
  }

  @Override
  protected void processSegments() {
    int operatorId;
    while ((operatorId = _nextOperatorId.getAndIncrement()) < _numOperators) {
      Operator<T> operator = _operators.get(operatorId);
      try {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).acquire();
        }
        T resultsBlock;
        while (!shouldFinishOperators() && (resultsBlock = operator.nextBlock()) != null) {
          if (shouldFinishStream(resultsBlock) || _resultsBlockMerger.isQuerySatisfied(resultsBlock)) {
            _blockingQueue.offer(resultsBlock);
            break;
          } else {
            _blockingQueue.offer(resultsBlock);
          }
        }
      } finally {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).release();
        }
      }
      // offer the LAST_RESULTS_BLOCK indicate finish of the current operator.
      _blockingQueue.offer(LAST_RESULTS_BLOCK);
    }
  }

  @Override
  protected void onProcessSegmentsException(Throwable t) {
    _blockingQueue.offer(new ExceptionResultsBlock(t));
  }

  @Override
  protected void onProcessSegmentsFinish() {
  }

  /**
   * Determine if a result block should finish the stream.
   *
   * @param resultsBlock the result block returned from operator
   * @return true if no more getNextBlock() should be called this particular upstream operator.
   */
  protected boolean shouldFinishStream(T resultsBlock) {
    return resultsBlock == null;
  }

  /**
   * Determine if the aggregated condition of the all the streams should early terminate the operators entirely.
   *
   * @return true if we should not schedule any additional {@link Operator#nextBlock()}
   */
  protected boolean shouldFinishOperators() {
    return false;
  }

  public void start() {
    startProcess();
  }

  public void stop() {
    stopProcess();
  }
}
