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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryErrorMessage;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Combine operator for group-by queries.
 */
@SuppressWarnings("rawtypes")
public class GroupByCombineOperator extends BaseSingleBlockCombineOperator<GroupByResultsBlock> {
  public static final String ALGORITHM = "CONCURRENT";

  private static final Logger LOGGER = LoggerFactory.getLogger(GroupByCombineOperator.class);
  private static final String EXPLAIN_NAME = "COMBINE_GROUP_BY";

  protected final int _numAggregationFunctions;
  protected final int _numGroupByExpressions;
  protected final int _numColumns;
  // We use a CountDownLatch to track if all Futures are finished by the query timeout, and cancel the unfinished
  // _futures (try to interrupt the execution if it already started).
  protected final CountDownLatch _operatorLatch;

  protected volatile IndexedTable _indexedTable;
  protected volatile boolean _groupsTrimmed;
  protected volatile boolean _numGroupsLimitReached;
  protected volatile boolean _numGroupsWarningLimitReached;

  public GroupByCombineOperator(List<Operator> operators, QueryContext queryContext, ExecutorService executorService) {
    super(null, operators, overrideMaxExecutionThreads(queryContext, operators.size()), executorService);

    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();
    assert aggregationFunctions != null;
    _numAggregationFunctions = aggregationFunctions.length;
    assert _queryContext.getGroupByExpressions() != null;
    _numGroupByExpressions = _queryContext.getGroupByExpressions().size();
    _numColumns = _numGroupByExpressions + _numAggregationFunctions;
    _operatorLatch = new CountDownLatch(_numTasks);
  }

  /**
   * For group-by queries, when maxExecutionThreads is not explicitly configured, override it to create as many tasks
   * as the default number of query worker threads (or the number of operators / segments if that's lower).
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
   * Executes query on one segment in a worker thread and merges the results into the indexed table.
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
        if (_indexedTable == null) {
          synchronized (this) {
            if (_indexedTable == null) {
              _indexedTable = createIndexedTable(resultsBlock, _numTasks);
            }
          }
        }
        mergeGroupByResultsBlock(_indexedTable, resultsBlock, EXPLAIN_NAME);
      } catch (RuntimeException e) {
        throw wrapOperatorException(operator, e);
      } finally {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).release();
        }
      }
    }
  }

  protected IndexedTable createIndexedTable(GroupByResultsBlock resultsBlock, int numThreads) {
    return GroupByUtils.createIndexedTableForCombineOperator(resultsBlock, _queryContext, numThreads, _executorService);
  }

  protected void updateCombineResultsStats(GroupByResultsBlock resultsBlock) {
    if (resultsBlock.isGroupsTrimmed()) {
      _groupsTrimmed = true;
    }
    if (resultsBlock.isNumGroupsLimitReached()) {
      _numGroupsLimitReached = true;
    }
    if (resultsBlock.isNumGroupsWarningLimitReached()) {
      _numGroupsWarningLimitReached = true;
    }
  }

  protected void mergeGroupByResultsBlock(IndexedTable indexedTable, GroupByResultsBlock resultsBlock,
      String explainName) {
    updateCombineResultsStats(resultsBlock);

    Collection<IntermediateRecord> intermediateRecords = resultsBlock.getIntermediateRecords();
    int mergedKeys = 0;
    if (intermediateRecords == null) {
      AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
      if (aggregationGroupByResult != null) {
        try {
          Iterator<GroupKeyGenerator.GroupKey> dicGroupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
          while (dicGroupKeyIterator.hasNext()) {
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(mergedKeys++, explainName);
            GroupKeyGenerator.GroupKey groupKey = dicGroupKeyIterator.next();
            Object[] keys = groupKey._keys;
            Object[] values = Arrays.copyOf(keys, _numColumns);
            int groupId = groupKey._groupId;
            for (int i = 0; i < _numAggregationFunctions; i++) {
              values[_numGroupByExpressions + i] = aggregationGroupByResult.getResultForGroupId(i, groupId);
            }
            indexedTable.upsert(new Key(keys), new Record(values));
          }
        } finally {
          aggregationGroupByResult.closeGroupKeyGenerator();
        }
      }
    } else {
      for (IntermediateRecord intermediateResult : intermediateRecords) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(mergedKeys++, explainName);
        indexedTable.upsert(intermediateResult._key, intermediateResult._record);
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
   * {@inheritDoc}
   *
   * <p>Combines intermediate group-by results produced by underlying operators and returns a merged results block.
   * <ul>
   *   <li>
   *     Merges multiple intermediate group-by result blocks into a single merged result.
   *   </li>
   *   <li>
   *     Sets all exceptions encountered during execution into the merged result block.
   *   </li>
   * </ul>
   */
  @Override
  public BaseResultsBlock mergeResults()
      throws Exception {
    long timeoutMs = _queryContext.getEndTimeMs() - System.currentTimeMillis();
    boolean opCompleted = _operatorLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
    if (!opCompleted) {
      return getTimeoutResultsBlock(timeoutMs);
    }

    Throwable ex = _processingException.get();
    if (ex != null) {
      return getExceptionResultsBlock(ex);
    }

    return getMergedResultsBlock(_indexedTable);
  }

  protected ExceptionResultsBlock getTimeoutResultsBlock(long timeoutMs) {
    long reportedTimeoutMs = Math.max(timeoutMs, 0L);
    String userError = "Timed out while combining group-by results after " + reportedTimeoutMs + "ms";
    String logMsg = userError + ", queryContext = " + _queryContext;
    LOGGER.error(logMsg);
    return new ExceptionResultsBlock(new QueryErrorMessage(QueryErrorCode.EXECUTION_TIMEOUT, userError, logMsg));
  }

  protected ExceptionResultsBlock getExceptionResultsBlock(Throwable ex) {
    String userError = "Caught exception while processing group-by query";
    String devError = userError + ": " + ex.getMessage();
    QueryErrorMessage errMsg;
    if (ex instanceof QueryException) {
      errMsg = new QueryErrorMessage(((QueryException) ex).getErrorCode(), devError, devError);
    } else {
      errMsg = new QueryErrorMessage(QueryErrorCode.QUERY_EXECUTION, userError, devError);
    }
    return new ExceptionResultsBlock(errMsg);
  }

  protected GroupByResultsBlock getMergedResultsBlock(IndexedTable indexedTable) {
    if (indexedTable.isTrimmed() && _queryContext.isUnsafeTrim()) {
      _groupsTrimmed = true;
    }

    if (_queryContext.isServerReturnFinalResult()) {
      indexedTable.finish(true, true);
    } else if (_queryContext.isServerReturnFinalResultKeyUnpartitioned()) {
      indexedTable.finish(false, true);
    } else {
      indexedTable.finish(false);
    }
    GroupByResultsBlock mergedBlock = new GroupByResultsBlock(indexedTable, _queryContext);
    mergedBlock.setGroupsTrimmed(_groupsTrimmed);
    mergedBlock.setNumGroupsLimitReached(_numGroupsLimitReached);
    mergedBlock.setNumGroupsWarningLimitReached(_numGroupsWarningLimitReached);
    mergedBlock.setNumResizes(indexedTable.getNumResizes());
    mergedBlock.setResizeTimeMs(indexedTable.getResizeTimeMs());
    return mergedBlock;
  }
}
