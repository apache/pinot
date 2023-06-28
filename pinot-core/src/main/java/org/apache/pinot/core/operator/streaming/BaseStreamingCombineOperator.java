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
import java.util.concurrent.ExecutorService;
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
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class BaseStreamingCombineOperator<T extends BaseResultsBlock>
    extends BaseCombineOperator<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseStreamingCombineOperator.class);

  // Use a special results block to indicate that this is the last results block for a child operator in the list
  public static final MetadataResultsBlock LAST_RESULTS_BLOCK = new MetadataResultsBlock();

  protected int _numOperatorsFinished;
  protected boolean _querySatisfied;

  public BaseStreamingCombineOperator(ResultsBlockMerger<T> resultsBlockMerger, List<Operator> operators,
      QueryContext queryContext, ExecutorService executorService) {
    super(resultsBlockMerger, operators, queryContext, executorService);
  }

  /**
   * {@inheritDoc}
   *
   * When all the results blocks are returned, returns a final metadata block. Caller shouldn't call this method after
   * it returns the metadata block or exception block.
   */
  @Override
  protected BaseResultsBlock getNextBlock() {
    long endTimeMs = _queryContext.getEndTimeMs();
    Object querySatisfiedTracker = createQuerySatisfiedTracker();
    while (!_querySatisfied && _numOperatorsFinished < _numOperators) {
      try {
        BaseResultsBlock resultsBlock =
            _blockingQueue.poll(endTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        if (resultsBlock == null) {
          // Query times out, skip streaming the remaining results blocks
          LOGGER.error("Timed out while polling results block (query: {})", _queryContext);
          return new ExceptionResultsBlock(QueryException.getException(QueryException.EXECUTION_TIMEOUT_ERROR,
              new TimeoutException("Timed out while polling results block")));
        }
        if (resultsBlock.getProcessingExceptions() != null) {
          // Caught exception while processing segment, skip streaming the remaining results blocks and directly return
          // the exception
          return resultsBlock;
        }
        if (resultsBlock == LAST_RESULTS_BLOCK) {
          // Caught LAST_RESULTS_BLOCK from a specific task, indicated it has finished.
          // Skip returning this metadata block and continue to process the next from the _blockingQueue.
          _numOperatorsFinished++;
          continue;
        }
        _querySatisfied = isQuerySatisfied((T) resultsBlock, querySatisfiedTracker);
        return resultsBlock;
      } catch (InterruptedException e) {
        throw new EarlyTerminationException("Interrupted while streaming results blocks", e);
      } catch (Exception e) {
        LOGGER.error("Caught exception while streaming results blocks (query: {})", _queryContext, e);
        return new ExceptionResultsBlock(QueryException.getException(QueryException.INTERNAL_ERROR, e));
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
    Object tracker = createQuerySatisfiedTracker();
    while (_processingException.get() == null && (operatorId = _nextOperatorId.getAndIncrement()) < _numOperators) {
      Operator<T> operator = _operators.get(operatorId);
      try {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).acquire();
        }
        if (isChildOperatorSingleBlock()) {
          T resultsBlock = operator.nextBlock();
          _blockingQueue.offer(resultsBlock);
          // When query is satisfied, skip processing the remaining segments
          if (isQuerySatisfied(resultsBlock, tracker)) {
            _nextOperatorId.set(_numOperators);
            return;
          }
        } else {
          T resultsBlock;
          while ((resultsBlock = operator.nextBlock()) != null) {
            _blockingQueue.offer(resultsBlock);
            // When query is satisfied, skip processing the remaining segments
            if (isQuerySatisfied(resultsBlock, tracker)) {
              _nextOperatorId.set(_numOperators);
              return;
            }
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
    _processingException.compareAndSet(null, t);
    _blockingQueue.offer(new ExceptionResultsBlock(t));
  }

  @Override
  protected void onProcessSegmentsFinish() {
  }

  /**
   * Returns whether the child operator returns only a single block.
   */
  protected boolean isChildOperatorSingleBlock() {
    return true;
  }

  /**
   * Creates a tracker object to track if the query is satisfied.
   */
  protected Object createQuerySatisfiedTracker() {
    return null;
  }

  /**
   * Returns {@code true} if the query is already satisfied with the results block.
   */
  protected boolean isQuerySatisfied(T resultsBlock, Object tracker) {
    return _resultsBlockMerger.isQuerySatisfied(resultsBlock);
  }

  public void start() {
    startProcess();
  }

  public void stop() {
    stopProcess();
  }
}
