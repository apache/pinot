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

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.SortedRecordTable;
import org.apache.pinot.core.data.table.SortedRecords;
import org.apache.pinot.core.data.table.SortedRecordsMerger;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.core.query.utils.OrderByComparatorFactory;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryErrorMessage;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.trace.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>Pair-wise Combine operator for sort-aggregation</p>
 *
 * <p>In this algorithm, an {@link AtomicReference} is used as a "pit" to store
 * the processed {@link SortedRecordTable} to be merged.</p>
 *
 * <p>Each worker thread first processes a segment to a {@link SortedRecordTable},
 * then greedily take waiting tables from the pit and merge them in, until there
 * is no table waiting, then the merged table is placed in the pit. The worker
 * thread then proceed to process the next segment.</p>
 *
 * <p>When there is a table that merged together {@code _numOperators} tables, it
 * is put into {@code _satisfiedTable} as the combine result.</p>
 *
 * <p>This pair-wise approach allows higher level of parallelism for the first rounds
 * of combine, while keeping the processing in a streaming fashion without
 * having to wait for all segments to be ready.</p>
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class SortedGroupByCombineOperator extends BaseSingleBlockCombineOperator<GroupByResultsBlock> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SortedGroupByCombineOperator.class);
  private static final String EXPLAIN_NAME = "COMBINE_GROUP_BY";

  // We use a CountDownLatch to track if all Futures are finished by the query timeout, and cancel the unfinished
  // _futures (try to interrupt the execution if it already started).
  private final CountDownLatch _operatorLatch;

  private volatile boolean _groupsTrimmed;
  private volatile boolean _numGroupsLimitReached;
  private volatile boolean _numGroupsWarningLimitReached;
  private volatile DataSchema _dataSchema;

  private final AtomicReference _waitingRecords;
  private final SortedRecordsMerger _sortedRecordsMerger;

  public SortedGroupByCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService) {
    super(null, operators, overrideMaxExecutionThreads(queryContext, operators.size()), executorService);

    assert (queryContext.shouldSortAggregateUnderSafeTrim());
    _operatorLatch = new CountDownLatch(_numTasks);
    _waitingRecords = new AtomicReference<>();
    Comparator<Record> recordKeyComparator =
        OrderByComparatorFactory.getRecordKeyComparator(queryContext.getOrderByExpressions(),
            queryContext.getGroupByExpressions(), queryContext.isNullHandlingEnabled());
    _sortedRecordsMerger =
        GroupByUtils.getSortedReduceMerger(queryContext, queryContext.getLimit(), recordKeyComparator);
  }

  /**
   * For group-by queries, when maxExecutionThreads is not explicitly configured, override it to create as many tasks as
   * the default number of query worker threads (or the number of operators / segments if that's lower).
   */
  private static QueryContext overrideMaxExecutionThreads(QueryContext queryContext, int numOperators) {
    int maxExecutionThreads = queryContext.getMaxExecutionThreads();
    if (maxExecutionThreads <= 0) {
      queryContext.setMaxExecutionThreads(Math.min(numOperators, ResourceManager.DEFAULT_QUERY_WORKER_THREADS));
    }
    return queryContext;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  /**
   * Executes query on sorted segments in a worker thread and merges the results using the pair-wise combine algorithm.
   */
  @Override
  protected void processSegments() {
    int operatorId;
    while (_processingException.get() == null && (operatorId = _nextOperatorId.getAndIncrement()) < _numOperators) {
      Operator operator = _operators.get(operatorId);
      try {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).acquire();
        }
        GroupByResultsBlock resultsBlock = (GroupByResultsBlock) operator.nextBlock();
        if (resultsBlock.isGroupsTrimmed()) {
          _groupsTrimmed = true;
        }
        // Set groups limit reached flag.
        if (resultsBlock.isNumGroupsLimitReached()) {
          _numGroupsLimitReached = true;
        }
        if (resultsBlock.isNumGroupsWarningLimitReached()) {
          _numGroupsWarningLimitReached = true;
        }
        if (_dataSchema == null) {
          _dataSchema = resultsBlock.getDataSchema();
        }
        // Short-circuit one segment case
        // This is reachable when we have 1 core only, and we have single operator.
        // In case that we have 1 core only, it makes more sense to use pair-wise rather than sequential combine,
        //   since the producer - consumer model in the latter does not guarantee
        //   timely consumption of produced results (depend on thread scheduling),
        //   which might build up to mem pressure.
        // Our default behavior also accounts for this,
        //   pair-wise is used when numOperators >= numAvailableCores.
        if (_numOperators == 1) {
          _waitingRecords.set(GroupByUtils.getAndPopulateSortedRecords(resultsBlock));
          break;
        }
        Object waitingObject = _waitingRecords.getAndUpdate(v -> v == null ? resultsBlock : null);
        if (waitingObject == null) {
          continue;
        }
        SortedRecords records = GroupByUtils.getAndPopulateSortedRecords(resultsBlock);
        // if found waiting block, merge and loop
        if (waitingObject instanceof GroupByResultsBlock) {
          records = mergeBlocks(records, (GroupByResultsBlock) waitingObject);
        } else {
          records = mergeRecords(records, (SortedRecords) waitingObject);
        }
        Tracing.ThreadAccountantOps.sampleAndCheckInterruption();

        while (true) {
          SortedRecords finalRecords = records;
          waitingObject = _waitingRecords.getAndUpdate(v -> v == null ? finalRecords : null);
          if (waitingObject == null) {
            break;
          }
          // if found waiting block, merge and loop
          if (waitingObject instanceof GroupByResultsBlock) {
            records = mergeBlocks(records, (GroupByResultsBlock) waitingObject);
          } else {
            records = mergeRecords(records, (SortedRecords) waitingObject);
          }
          Tracing.ThreadAccountantOps.sampleAndCheckInterruption();
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

  /**
   * <p>Collect the merged group by result and wraps it into a result block
   * <li>Set all exceptions encountered during execution into the merged result block</li>
   */
  @Override
  public BaseResultsBlock mergeResults()
      throws Exception {
    long timeoutMs = _queryContext.getEndTimeMs() - System.currentTimeMillis();
    boolean opCompleted = _operatorLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
    if (!opCompleted) {
      // If this happens, the broker side should already timed out, just log the error and return
      String userError = "Timed out while combining group-by order-by results after " + timeoutMs + "ms";
      String logMsg = userError + ", queryContext = " + _queryContext;
      LOGGER.error(logMsg);
      return new ExceptionResultsBlock(new QueryErrorMessage(QueryErrorCode.EXECUTION_TIMEOUT, userError, logMsg));
    }

    Throwable ex = _processingException.get();
    if (ex != null) {
      String userError = "Caught exception while processing group-by order-by query";
      String devError = userError + ": " + ex.getMessage();
      QueryErrorMessage errMsg;
      if (ex instanceof QueryException) {
        // If the exception is a QueryException, use the error code from the exception and trust the error message
        errMsg = new QueryErrorMessage(((QueryException) ex).getErrorCode(), devError, devError);
      } else {
        // If the exception is not a QueryException, use the generic error code and don't expose the exception message
        errMsg = new QueryErrorMessage(QueryErrorCode.QUERY_EXECUTION, userError, devError);
      }
      return new ExceptionResultsBlock(errMsg);
    }

    Object records = _waitingRecords.get();
    // should be SortedRecords since one-block only case is short-circuited
    assert (records instanceof SortedRecords);
    return finishSortedRecords((SortedRecords) records);
  }

  private GroupByResultsBlock finishSortedRecords(SortedRecords records) {
    SortedRecordTable table =
        new SortedRecordTable(records, _dataSchema, _queryContext, _executorService);

    // finish
    if (_queryContext.isServerReturnFinalResult()) {
      table.finish(true, true);
    } else if (_queryContext.isServerReturnFinalResultKeyUnpartitioned()) {
      table.finish(false, true);
    } else {
      table.finish(false);
    }
    GroupByResultsBlock mergedBlock = new GroupByResultsBlock(table, _queryContext);
    mergedBlock.setGroupsTrimmed(_groupsTrimmed);
    mergedBlock.setNumGroupsLimitReached(_numGroupsLimitReached);
    mergedBlock.setNumGroupsWarningLimitReached(_numGroupsWarningLimitReached);
    return mergedBlock;
  }

  private SortedRecords mergeRecords(SortedRecords block1, SortedRecords block2) {
    return _sortedRecordsMerger.mergeSortedRecordArray(block1, block2);
  }

  private SortedRecords mergeBlocks(SortedRecords block1, GroupByResultsBlock block2) {
    return _sortedRecordsMerger.mergeGroupByResultsBlock(block1, block2);
  }
}
