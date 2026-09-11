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


/// Combine operator for group-by queries.
@SuppressWarnings("rawtypes")
public class GroupByCombineOperator extends BaseSingleBlockCombineOperator<GroupByResultsBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GroupByCombineOperator.class);
  private static final String EXPLAIN_NAME = "COMBINE_GROUP_BY";

  private final int _numAggregationFunctions;
  /// Number of key columns used while MERGING segment results. For a base-aggregation grouping-set query this is
  /// the number of union group-by columns (segments emit base groups without the synthetic $groupingId column,
  /// which is added later while deriving); otherwise it is the full group-by key count (union columns plus the
  /// $groupingId column for expansion-path grouping sets). Key columns precede the aggregation columns.
  ///
  /// Resolved from the first result block's schema (which reflects whether the segment chose base aggregation or
  /// expansion, including the MV-column carve-out) rather than the query context alone, so it always matches the
  /// records actually emitted.
  private int _numKeyColumns;
  private int _numColumns;
  /// Whether segments emitted BASE groups (union grouping only) that this combine merges and then derives into
  /// the individual grouping sets in [#mergeResults()]. See [QueryContext#isGroupingSetsBaseAggregation].
  private boolean _groupingSetsBaseAggregation;
  // We use a CountDownLatch to track if all Futures are finished by the query timeout, and cancel the unfinished
  // _futures (try to interrupt the execution if it already started).
  private final CountDownLatch _operatorLatch;

  private volatile IndexedTable _indexedTable;
  private volatile boolean _groupsTrimmed;
  private volatile boolean _numGroupsLimitReached;
  private volatile boolean _numGroupsWarningLimitReached;

  public GroupByCombineOperator(List<Operator> operators, QueryContext queryContext, ExecutorService executorService) {
    super(null, operators, overrideMaxExecutionThreads(queryContext, operators.size()), executorService);

    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();
    assert aggregationFunctions != null;
    _numAggregationFunctions = aggregationFunctions.length;
    assert _queryContext.getGroupByExpressions() != null;
    // Default (non-grouping-set / expansion-path) key layout. For base aggregation this is corrected to the
    // base (union-only) key layout once the first result block reveals its schema (see #resolveMergeLayout).
    _numKeyColumns = _queryContext.getNumGroupByKeyColumns();
    _numColumns = _numKeyColumns + _numAggregationFunctions;
    _operatorLatch = new CountDownLatch(_numTasks);
  }

  /// For group-by queries, when maxExecutionThreads is not explicitly configured, override it to create as many tasks
  /// as the default number of query worker threads (or the number of operators / segments if that's lower).
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

  /// Executes query on one segment in a worker thread and merges the results into the indexed table.
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
              resolveMergeLayout(resultsBlock);
              _indexedTable = GroupByUtils.createIndexedTableForCombineOperator(resultsBlock, _queryContext, _numTasks,
                  _executorService);
            }
          }
        }

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

        // Merge aggregation group-by result.
        // Iterate over the group-by keys, for each key, update the group-by result in the indexedTable
        Collection<IntermediateRecord> intermediateRecords = resultsBlock.getIntermediateRecords();
        // Count the number of merged keys
        int mergedKeys = 0;
        // For now, only GroupBy OrderBy query has pre-constructed intermediate records
        if (intermediateRecords == null) {
          // Merge aggregation group-by result.
          AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
          if (aggregationGroupByResult != null) {
            // Iterate over the group-by keys, for each key, update the group-by result in the indexedTable
            try {
              Iterator<GroupKeyGenerator.GroupKey> dicGroupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
              while (dicGroupKeyIterator.hasNext()) {
                QueryThreadContext.checkTerminationAndSampleUsagePeriodically(mergedKeys++, EXPLAIN_NAME);
                GroupKeyGenerator.GroupKey groupKey = dicGroupKeyIterator.next();
                Object[] keys = groupKey._keys;
                Object[] values = Arrays.copyOf(keys, _numColumns);
                int groupId = groupKey._groupId;
                for (int i = 0; i < _numAggregationFunctions; i++) {
                  values[_numKeyColumns + i] = aggregationGroupByResult.getResultForGroupId(i, groupId);
                }
                _indexedTable.upsert(new Key(keys), new Record(values));
              }
            } finally {
              // Release the resources used by the group key generator
              aggregationGroupByResult.closeGroupKeyGenerator();
            }
          }
        } else {
          for (IntermediateRecord intermediateResult : intermediateRecords) {
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(mergedKeys++, EXPLAIN_NAME);
            //TODO: change upsert api so that it accepts intermediateRecord directly
            _indexedTable.upsert(intermediateResult._key, intermediateResult._record);
          }
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

  /// Resolves the merge-time record layout from the first result block's schema. A base-aggregation grouping-set
  /// segment emits base groups whose schema has no synthetic $groupingId column, so it has exactly
  /// `numUnionColumns + numAggregationFunctions` columns; the expansion path (and non-grouping-set queries)
  /// carries the full `$groupingId`-including layout. Deriving the mode from the schema (rather than the query
  /// context alone) also covers the MV-column carve-out, where a grouping-set query still uses expansion.
  /// Must be called under the same synchronization that guards the one-time `_indexedTable` creation.
  private void resolveMergeLayout(GroupByResultsBlock resultsBlock) {
    if (_queryContext.isGroupingSets()) {
      int numUnionColumns = _queryContext.getGroupByExpressions().size();
      int numBaseColumns = numUnionColumns + _numAggregationFunctions;
      if (resultsBlock.getDataSchema() != null && resultsBlock.getDataSchema().size() == numBaseColumns) {
        _groupingSetsBaseAggregation = true;
        _numKeyColumns = numUnionColumns;
        _numColumns = numBaseColumns;
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

  /// {@inheritDoc}
  ///
  /// Combines intermediate selection result blocks from underlying operators and returns a merged one.
  ///
  /// - Merges multiple intermediate selection result blocks as a merged one.
  /// - Set all exceptions encountered during execution into the merged result block
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

    if (_indexedTable.isTrimmed() && _queryContext.isUnsafeTrim()) {
      _groupsTrimmed = true;
    }

    IndexedTable indexedTable = _indexedTable;
    /// Base aggregation: `indexedTable` holds the merged BASE groups (union grouping). Derive the individual
    /// grouping sets from them once, in parallel across the combine executor, into the final grouping-set table.
    /// This is where the per-set fan-out happens -- after the row-collapsing base merge and multi-threaded, so
    /// it never repeats the per-row expansion the segment phase would otherwise pay.
    if (_groupingSetsBaseAggregation) {
      indexedTable = GroupByUtils.deriveGroupingSetsFromMergedBaseTable(indexedTable, _queryContext, _numTasks,
          _executorService);
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
