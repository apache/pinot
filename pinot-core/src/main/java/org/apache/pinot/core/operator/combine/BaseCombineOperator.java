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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.operator.combine.merger.ResultsBlockMerger;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.QueryMultiThreadingUtils;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryErrorMessage;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.trace.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base implementation of the combine operator.
 * <p>Combine operator uses multiple worker threads to process segments in parallel, and uses the main thread to merge
 * the results blocks from the processed segments. It can early-terminate the query to save the system resources if it
 * detects that the merged results can already satisfy the query, or the query is already errored out or timed out.
 */
@SuppressWarnings({"rawtypes"})
public abstract class BaseCombineOperator<T extends BaseResultsBlock> extends BaseOperator<BaseResultsBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseCombineOperator.class);

  protected final ResultsBlockMerger<T> _resultsBlockMerger;
  protected final List<Operator> _operators;
  protected final int _numOperators;
  protected final QueryContext _queryContext;
  protected final ExecutorService _executorService;
  protected final int _numTasks;
  protected final Phaser _phaser;
  protected final Future[] _futures;

  // Use an AtomicInteger to track the next operator to execute
  protected final AtomicInteger _nextOperatorId = new AtomicInteger();
  // Use an AtomicReference to track the exception/error during segment processing
  protected final AtomicReference<Throwable> _processingException = new AtomicReference<>();

  protected final AtomicLong _totalWorkerThreadCpuTimeNs = new AtomicLong(0);

  protected BaseCombineOperator(ResultsBlockMerger<T> resultsBlockMerger, List<Operator> operators,
      QueryContext queryContext, ExecutorService executorService) {
    _resultsBlockMerger = resultsBlockMerger;
    _operators = operators;
    _numOperators = _operators.size();
    _queryContext = queryContext;
    _executorService = executorService;

    // NOTE: We split the query execution into multiple tasks, where each task handles the query execution on multiple
    //       (>=1) segments. These tasks are assigned to multiple execution threads so that they can run in parallel.
    //       The parallelism is bounded by the task count.
    _numTasks = QueryMultiThreadingUtils.getNumTasksForQuery(operators.size(), queryContext.getMaxExecutionThreads());

    // Use a Phaser to ensure all the Futures are done (not scheduled, finished or interrupted) before the main thread
    // returns. We need to ensure this because the main thread holds the reference to the segments. If a segment is
    // deleted/refreshed, the segment will be released after the main thread returns, which would lead to undefined
    // behavior (even JVM crash) when processing queries against it.
    _phaser = new Phaser(1);
    _futures = new Future[_numTasks];
  }

  /**
   * Start the combine operator process. This will spin up multiple threads to process data segments in parallel.
   */
  protected void startProcess() {
    Tracing.activeRecording().setNumTasks(_numTasks);
    ThreadExecutionContext parentContext = Tracing.getThreadAccountant().getThreadExecutionContext();
    for (int i = 0; i < _numTasks; i++) {
      int taskId = i;
      _futures[i] = _executorService.submit(new TraceRunnable() {
        @Override
        public void runJob() {
          ThreadResourceUsageProvider threadResourceUsageProvider = new ThreadResourceUsageProvider();

          Tracing.ThreadAccountantOps.setupWorker(taskId, parentContext);

          // Register the task to the phaser
          // NOTE: If the phaser is terminated (returning negative value) when trying to register the task, that means
          //       the query execution has finished, and the main thread has deregistered itself and returned the
          //       result. Directly return as no execution result will be taken.
          if (_phaser.register() < 0) {
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
              LOGGER.error("Caught exception while processing query: {}", _queryContext, t);
            } else {
              LOGGER.error("Caught serious error while processing query: {}", _queryContext, t);
            }
            onProcessSegmentsException(t);
          } finally {
            onProcessSegmentsFinish();
            _phaser.arriveAndDeregister();
            Tracing.ThreadAccountantOps.clear();
          }

          _totalWorkerThreadCpuTimeNs.getAndAdd(threadResourceUsageProvider.getThreadTimeNs());
        }
      });
    }
  }

  /**
   * Stop the combine operator process. This will stop all sub-tasks that were spun up to process data segments.
   */
  protected void stopProcess() {
    // Cancel all ongoing jobs
    for (Future future : _futures) {
      if (future != null && !future.isDone()) {
        future.cancel(true);
      }
    }
    // Deregister the main thread and wait for all threads done
    _phaser.awaitAdvance(_phaser.arriveAndDeregister());
  }

  protected ExceptionResultsBlock getTimeoutResultsBlock(int numBlocksMerged) {
    String logMsg = "Timed out while polling results block, numBlocksMerged: " + numBlocksMerged + " (query: "
        + _queryContext + ")";
    LOGGER.error(logMsg);
    QueryErrorCode errCode = QueryErrorCode.EXECUTION_TIMEOUT;
    QueryErrorMessage errMsg = new QueryErrorMessage(errCode, "Timed out while polling results block", logMsg);
    return new ExceptionResultsBlock(errMsg);
  }

  @Override
  public List<Operator> getChildOperators() {
    return _operators;
  }

  /**
   * Executes query on one or more segments in a worker thread.
   */
  protected abstract void processSegments();

  /**
   * Invoked when {@link #processSegments()} throws exception/error.
   */
  protected abstract void onProcessSegmentsException(Throwable t);

  /**
   * Invoked when {@link #processSegments()} is finished (called in the finally block).
   */
  protected abstract void onProcessSegmentsFinish();

  protected static RuntimeException wrapOperatorException(Operator operator, RuntimeException e) {
    // Skip early termination as that's not quite related to the segments.
    if (e instanceof EarlyTerminationException) {
      return e;
    }
    // Otherwise, try to get the segment name to help locate the segment when debugging query errors.
    // Not all operators have associated segment, so do this at best effort.
    // TODO: Do not use class name here but the operator name explain plan. To do so, that method must be moved from
    //  multi and single stage operators to the base operator
    String errorMessage = operator.getIndexSegment() != null
        ? "Caught exception while doing operator: " + operator.getClass()
            + " on segment " + operator.getIndexSegment().getSegmentName()
        : "Caught exception while doing operator: " + operator.getClass();

    QueryErrorCode errorCode;
    if (e instanceof QueryException) {
      QueryException queryException = (QueryException) e;
      errorCode = queryException.getErrorCode();
    } else {
      errorCode = QueryErrorCode.QUERY_EXECUTION;
    }
    // TODO: Only include exception message if it is a QueryException. Otherwise, it might expose sensitive information
    throw errorCode.asException(errorMessage + ": " + e.getMessage(), e);
  }
}
