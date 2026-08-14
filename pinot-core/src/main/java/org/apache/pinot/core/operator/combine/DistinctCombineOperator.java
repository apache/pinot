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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.query.distinct.table.DistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryErrorMessage;
import org.apache.pinot.spi.exception.QueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Combine operator for distinct queries.
///
/// Uses a thread-local accumulation strategy to eliminate per-segment [DistinctTable]
/// allocation and [java.util.concurrent.BlockingQueue] overhead:
///
/// - Each worker task is assigned a dedicated slot and merges every segment it processes
///   directly into that slot's [DistinctTable], rather than posting individual segment
///   results to a shared queue.
/// - After all tasks finish, the main thread merges the (at most `_numTasks`) per-task
///   tables into a single result — far fewer merge operations than the previous
///   one-merge-per-segment approach.
@SuppressWarnings("rawtypes")
public class DistinctCombineOperator extends BaseSingleBlockCombineOperator<DistinctResultsBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistinctCombineOperator.class);
  private static final String EXPLAIN_NAME = "COMBINE_DISTINCT";

  // Assigns each worker task a unique slot index in [0, _numTasks).
  private final AtomicInteger _taskSlotCounter = new AtomicInteger(0);
  // Per-task DistinctTable accumulators; null until the task processes its first segment.
  private final DistinctTable[] _taskTables;
  // Signals when all worker tasks have finished (successfully or with an error).
  private final CountDownLatch _operatorLatch;

  public DistinctCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService) {
    // Pass null merger: this class fully overrides processSegments() and mergeResults(),
    // so the merger from BaseSingleBlockCombineOperator is never invoked.
    super(null, operators, queryContext, executorService);
    _taskTables = new DistinctTable[_numTasks];
    _operatorLatch = new CountDownLatch(_numTasks);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  /// Processes segments for one worker task. Each invocation claims a unique slot and merges
  /// every segment it handles into that slot's accumulated [DistinctTable], avoiding both
  /// per-segment allocation and BlockingQueue contention.
  @Override
  protected void processSegments() {
    int slot = _taskSlotCounter.getAndIncrement();
    int operatorId;
    while (_processingException.get() == null && (operatorId = _nextOperatorId.getAndIncrement()) < _numOperators) {
      Operator operator = _operators.get(operatorId);
      try {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).acquire();
        }
        DistinctResultsBlock resultsBlock = (DistinctResultsBlock) operator.nextBlock();
        DistinctTable segmentTable = resultsBlock.getDistinctTable();
        if (_taskTables[slot] == null) {
          _taskTables[slot] = segmentTable;
        } else {
          _taskTables[slot].mergeDistinctTable(segmentTable);
        }
        if (_taskTables[slot].isSatisfied()) {
          // Enough distinct rows collected — signal all tasks to stop.
          _nextOperatorId.set(_numOperators);
          return;
        }
      } catch (RuntimeException e) {
        throw wrapOperatorException(operator, e);
      } finally {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).release();
        }
      }
    }
  }

  @Override
  public void onProcessSegmentsException(Throwable t) {
    _processingException.compareAndSet(null, t);
  }

  @Override
  public void onProcessSegmentsFinish() {
    _operatorLatch.countDown();
  }

  /// Waits for all worker tasks to complete, then merges the per-task [DistinctTable]s
  /// into a single [DistinctResultsBlock]. At most `_numTasks` merge operations are
  /// required here, regardless of the number of segments.
  @Override
  protected BaseResultsBlock mergeResults()
      throws Exception {
    long timeoutMs = _queryContext.getEndTimeMs() - System.currentTimeMillis();
    boolean opCompleted = timeoutMs > 0 && _operatorLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
    if (!opCompleted) {
      String userError = "Timed out while combining distinct results";
      String logMsg = userError + " after " + timeoutMs + "ms, queryContext = " + _queryContext;
      LOGGER.error(logMsg);
      return new ExceptionResultsBlock(
          new QueryErrorMessage(QueryErrorCode.EXECUTION_TIMEOUT, userError, logMsg));
    }

    Throwable ex = _processingException.get();
    if (ex != null) {
      String userError = "Caught exception while processing distinct query";
      String devError = userError + ": " + ex.getMessage();
      QueryErrorMessage errMsg = ex instanceof QueryException
          ? new QueryErrorMessage(((QueryException) ex).getErrorCode(), devError, devError)
          : new QueryErrorMessage(QueryErrorCode.QUERY_EXECUTION, userError, devError);
      return new ExceptionResultsBlock(errMsg);
    }

    // Merge per-task tables into the final result (at most _numTasks merges).
    DistinctTable mergedTable = null;
    for (DistinctTable table : _taskTables) {
      if (table == null) {
        continue;
      }
      if (mergedTable == null) {
        mergedTable = table;
      } else {
        mergedTable.mergeDistinctTable(table);
      }
      if (mergedTable.isSatisfied()) {
        break;
      }
    }

    if (mergedTable == null) {
      // No segments were processed (e.g. zero-segment table). Should not occur in practice
      // since CombinePlanNode only creates a combine operator when there is at least one segment,
      // but guard defensively to avoid NPE downstream.
      LOGGER.warn("DistinctCombineOperator: no segment produced a result (queryContext: {})", _queryContext);
      return new ExceptionResultsBlock(
          QueryErrorCode.INTERNAL.asException("No segment produced a distinct result"));
    }
    return new DistinctResultsBlock(mergedTable, _queryContext);
  }
}
