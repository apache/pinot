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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.SortedRecordTable;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.core.query.utils.OrderByComparatorFactory;
import org.apache.pinot.core.util.GroupByUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>Sequential Combine operator for sort-aggregation</p>
 *
 * <p>This operator merges sorted group-by results similar to other
 * {@link BaseSingleBlockCombineOperator}s,
 * as it uses a producer-consumer paradigm.</p>
 *
 * <p>Each worker thread produces sorted segment-level group-by results
 * while the main thread consumer via a {@code _blockingQueue} and merges them</p>
 *
 * <p>This allows merging in a streaming fashion without having to wait
 * for all segments to be ready. This sequential merging is usually
 * more efficient than the pair-size merging {@link SortedGroupByCombineOperator}
 * when the number of segments is smaller than the available number of cores</p>
 */
@SuppressWarnings("rawtypes")
public class SequentialSortedGroupByCombineOperator extends BaseSingleBlockCombineOperator<GroupByResultsBlock> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SequentialSortedGroupByCombineOperator.class);
  private static final String EXPLAIN_NAME = "COMBINE_GROUP_BY";

  // We use a CountDownLatch to track if all Futures are finished by the query timeout, and cancel the unfinished
  // _futures (try to interrupt the execution if it already started).
  private volatile boolean _groupsTrimmed;
  private volatile boolean _numGroupsLimitReached;
  private volatile boolean _numGroupsWarningLimitReached;

  private SortedRecordTable _table = null;
  private final Comparator<Record> _recordKeyComparator;

  public SequentialSortedGroupByCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService) {
    super(null, operators, overrideMaxExecutionThreads(queryContext, operators.size()), executorService);

    assert (queryContext.shouldSortAggregateUnderSafeTrim());
    _recordKeyComparator = OrderByComparatorFactory.getRecordKeyComparator(queryContext.getOrderByExpressions(),
        queryContext.getGroupByExpressions(), queryContext.isNullHandlingEnabled());
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
   * Executes query on one sorted segment in a worker thread and merges the results into the sorted record table.
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
        _blockingQueue.offer(resultsBlock);
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

  /**
   * {@inheritDoc}
   *
   * <p>Combines sorted intermediate aggregation result blocks from underlying operators and returns a merged one.
   * <ul>
   *   <li>
   *     Merges multiple sorted intermediate aggregation result blocks as a merged one.
   *   </li>
   *   <li>
   *     Set all exceptions encountered during execution into the merged result block
   *   </li>
   * </ul>
   */
  @Override
  public BaseResultsBlock mergeResults()
      throws Exception {

    int numBlocksMerged = 0;
    long endTimeMs = _queryContext.getEndTimeMs();
    while (numBlocksMerged < _numOperators) {
      // Timeout has reached, shouldn't continue to process. `_blockingQueue.poll` will continue to return blocks even
      // if negative timeout is provided; therefore an extra check is needed
      long waitTimeMs = endTimeMs - System.currentTimeMillis();
      if (waitTimeMs <= 0) {
        String userError = "Timed out while combining group-by order-by results after " + waitTimeMs + "ms";
        String logMsg = userError + ", queryContext = " + _queryContext;
        LOGGER.error(logMsg);
        return getTimeoutResultsBlock(numBlocksMerged);
      }
      BaseResultsBlock blockToMerge = _blockingQueue.poll(waitTimeMs, TimeUnit.MILLISECONDS);
      if (blockToMerge == null) {
        return getTimeoutResultsBlock(numBlocksMerged);
      }
      if (blockToMerge.getErrorMessages() != null) {
        // Caught exception while processing segment, skip merging the remaining results blocks and directly return the
        // exception
        return blockToMerge;
      }
      GroupByResultsBlock groupByResultBlockToMerge = (GroupByResultsBlock) blockToMerge;

      if (groupByResultBlockToMerge.isGroupsTrimmed()) {
        _groupsTrimmed = true;
      }
      // Set groups limit reached flag.
      if (groupByResultBlockToMerge.isNumGroupsLimitReached()) {
        _numGroupsLimitReached = true;
      }
      if (groupByResultBlockToMerge.isNumGroupsWarningLimitReached()) {
        _numGroupsWarningLimitReached = true;
      }

      if (_table == null) {
        _table = GroupByUtils.getAndPopulateSortedRecordTable(groupByResultBlockToMerge, _queryContext,
                _queryContext.getLimit(), _executorService, _numOperators, _recordKeyComparator);
      } else {
        _table.mergeSortedGroupByResultBlock(groupByResultBlockToMerge);
      }
      numBlocksMerged++;
    }

    if (_queryContext.isServerReturnFinalResult()) {
      _table.finish(true, true);
    } else if (_queryContext.isServerReturnFinalResultKeyUnpartitioned()) {
      _table.finish(false, true);
    } else {
      _table.finish(false);
    }
    GroupByResultsBlock mergedBlock = new GroupByResultsBlock(_table, _queryContext);
    mergedBlock.setGroupsTrimmed(_groupsTrimmed);
    mergedBlock.setNumGroupsLimitReached(_numGroupsLimitReached);
    mergedBlock.setNumGroupsWarningLimitReached(_numGroupsWarningLimitReached);
    return mergedBlock;
  }
}
