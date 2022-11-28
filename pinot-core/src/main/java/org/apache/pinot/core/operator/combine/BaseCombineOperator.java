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
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.exception.QueryCancelledException;
import org.apache.pinot.spi.trace.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base implementation of the combine operator.
 * <p>Combine operator uses multiple worker threads to process segments in parallel, and uses the main thread to merge
 * the results blocks from the processed segments. It can early-terminate the query to save the system resources if it
 * detects that the merged results can already satisfy the query, or the query is already errored out or timed out.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class BaseCombineOperator<T extends BaseResultsBlock> extends BaseOperator<BaseResultsBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseCombineOperator.class);

  protected final List<Operator> _operators;
  protected final int _numOperators;
  protected final QueryContext _queryContext;
  protected final ExecutorService _executorService;
  protected final int _numTasks;
  protected final Future[] _futures;
  // Use an AtomicInteger to track the next operator to execute
  protected final AtomicInteger _nextOperatorId = new AtomicInteger();
  // Use a BlockingQueue to store the intermediate results blocks
  protected final BlockingQueue<BaseResultsBlock> _blockingQueue = new LinkedBlockingQueue<>();
  protected final AtomicLong _totalWorkerThreadCpuTimeNs = new AtomicLong(0);

  protected BaseCombineOperator(List<Operator> operators, QueryContext queryContext, ExecutorService executorService) {
    _operators = operators;
    _numOperators = _operators.size();
    _queryContext = queryContext;
    _executorService = executorService;

    // NOTE: We split the query execution into multiple tasks, where each task handles the query execution on multiple
    //       (>=1) segments. These tasks are assigned to multiple execution threads so that they can run in parallel.
    //       The parallelism is bounded by the task count.
    _numTasks = CombineOperatorUtils.getNumTasksForQuery(operators.size(), queryContext.getMaxExecutionThreads());
    _futures = new Future[_numTasks];
  }

  @Override
  protected BaseResultsBlock getNextBlock() {
    // Use a Phaser to ensure all the Futures are done (not scheduled, finished or interrupted) before the main thread
    // returns. We need to ensure this because the main thread holds the reference to the segments. If a segment is
    // deleted/refreshed, the segment will be released after the main thread returns, which would lead to undefined
    // behavior (even JVM crash) when processing queries against it.
    Phaser phaser = new Phaser(1);
    Tracing.activeRecording().setNumTasks(_numTasks);
    ThreadExecutionContext parentContext = Tracing.getThreadAccountant().getThreadExecutionContext();
    for (int i = 0; i < _numTasks; i++) {
      int taskId = i;
      _futures[i] = _executorService.submit(new TraceRunnable() {
        @Override
        public void runJob() {
          ThreadResourceUsageProvider threadResourceUsageProvider = new ThreadResourceUsageProvider();

          Tracing.ThreadAccountantOps.setupWorker(taskId, threadResourceUsageProvider, parentContext);

          // Register the task to the phaser
          // NOTE: If the phaser is terminated (returning negative value) when trying to register the task, that means
          //       the query execution has finished, and the main thread has deregistered itself and returned the
          //       result. Directly return as no execution result will be taken.
          if (phaser.register() < 0) {
            Tracing.ThreadAccountantOps.clear();
            return;
          }
          try {
            processSegments();
          } catch (EarlyTerminationException e) {
            // Early-terminated by interruption (canceled by the main thread)
          } catch (Throwable t) {
            // Caught exception/error, skip processing the remaining segments
            // NOTE: We need to handle Error here, or the execution threads will die without adding the execution
            //       exception into the query response, and the main thread might wait infinitely (until timeout) or
            //       throw unexpected exceptions (such as NPE).
            if (t instanceof Exception) {
              LOGGER.error("Caught exception while processing query: " + _queryContext, t);
            } else {
              LOGGER.error("Caught serious error while processing query: " + _queryContext, t);
            }
            onException(t);
          } finally {
            onFinish();
            phaser.arriveAndDeregister();
            Tracing.ThreadAccountantOps.clear();
          }

          _totalWorkerThreadCpuTimeNs.getAndAdd(threadResourceUsageProvider.getThreadTimeNs());
        }
      });
    }

    BaseResultsBlock mergedBlock;
    try {
      mergedBlock = mergeResults();
    } catch (InterruptedException | EarlyTerminationException e) {
      Exception killedErrorMsg = Tracing.getThreadAccountant().getErrorStatus();
      throw new QueryCancelledException(
          "Cancelled while merging results blocks"
              + (killedErrorMsg == null ? StringUtils.EMPTY : " " + killedErrorMsg), e);
    } catch (Exception e) {
      LOGGER.error("Caught exception while merging results blocks (query: {})", _queryContext, e);
      mergedBlock = new ExceptionResultsBlock(QueryException.getException(QueryException.INTERNAL_ERROR, e));
    } finally {
      // Cancel all ongoing jobs
      for (Future future : _futures) {
        if (!future.isDone()) {
          future.cancel(true);
        }
      }
      // Deregister the main thread and wait for all threads done
      phaser.awaitAdvance(phaser.arriveAndDeregister());
    }
    /*
     * _numTasks are number of async tasks submitted to the _executorService, but it does not mean Pinot server
     * use those number of threads to concurrently process segments. Instead, if _executorService thread pool has
     * less number of threads than _numTasks, the number of threads that used to concurrently process segments equals
     * to the pool size.
     * TODO: Get the actual number of query worker threads instead of using the default value.
     */
    int numServerThreads = Math.min(_numTasks, ResourceManager.DEFAULT_QUERY_WORKER_THREADS);
    CombineOperatorUtils.setExecutionStatistics(mergedBlock, _operators, _totalWorkerThreadCpuTimeNs.get(),
        numServerThreads);
    return mergedBlock;
  }

  /**
   * Executes query on one or more segments in a worker thread.
   */
  protected void processSegments() {
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

      if (isQuerySatisfied(resultsBlock)) {
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
  protected void onException(Throwable t) {
    _blockingQueue.offer(new ExceptionResultsBlock(t));
  }

  /**
   * Invoked when {@link #processSegments()} is finished (called in the finally block).
   */
  protected void onFinish() {
  }

  /**
   * Merges the results from the worker threads into a results block.
   */
  protected BaseResultsBlock mergeResults()
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
        mergedBlock = convertToMergeableBlock((T) blockToMerge);
      } else {
        mergeResultsBlocks(mergedBlock, (T) blockToMerge);
      }
      numBlocksMerged++;
      if (isQuerySatisfied(mergedBlock)) {
        // Query is satisfied, skip merging the remaining results blocks
        return mergedBlock;
      }
    }
    return mergedBlock;
  }

  private ExceptionResultsBlock getTimeoutResultsBlock(int numBlocksMerged) {
    LOGGER.error("Timed out while polling results block, numBlocksMerged: {} (query: {})", numBlocksMerged,
        _queryContext);
    return new ExceptionResultsBlock(QueryException.EXECUTION_TIMEOUT_ERROR,
        new TimeoutException("Timed out while polling results block"));
  }

  /**
   * Can be overridden for early termination. The input results block might not be mergeable.
   */
  protected boolean isQuerySatisfied(T resultsBlock) {
    return false;
  }

  /**
   * Merges a results block into the main mergeable results block.
   * <p>NOTE: {@code blockToMerge} should contain the result for a segment without any exception. The errored segment
   * result is already handled.
   *
   * @param mergedBlock The block that accumulates previous results. It should be modified to add the information of the
   *                    other block.
   * @param blockToMerge The new block that needs to be merged into the mergedBlock.
   */
  protected abstract void mergeResultsBlocks(T mergedBlock, T blockToMerge);

  /**
   * Converts the given results block into a mergeable results block if necessary.
   */
  protected T convertToMergeableBlock(T resultsBlock) {
    return resultsBlock;
  }

  @Override
  public List<Operator> getChildOperators() {
    return _operators;
  }
}
