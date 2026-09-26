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
import java.util.Map;
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
  /// Full-layout record shape: the group-by key columns (union columns plus the synthetic $groupingId column for
  /// grouping-set queries) followed by the aggregation columns.
  private final int _numKeyColumns;
  private final int _numColumns;
  /// Whether this is a grouping-set query, in which segments may emit either FULL-layout records (per-row
  /// expansion, with the $groupingId column) or BASE-layout records (base aggregation: union columns only). The
  /// choice is per segment -- the MV-column carve-out and the base-group cardinality gate are evaluated against
  /// each segment's own metadata -- so a single query can produce a mix of both layouts. Each block is routed by
  /// its schema width into the matching table; [#mergeResults()] derives the grouping sets from the base table
  /// and merges the full-layout records in.
  private final boolean _groupingSets;
  /// BASE-layout record shape (grouping-set base aggregation): union key columns followed by aggregations.
  private final int _numBaseKeyColumns;
  private final int _numBaseColumns;
  // We use a CountDownLatch to track if all Futures are finished by the query timeout, and cancel the unfinished
  // _futures (try to interrupt the execution if it already started).
  private final CountDownLatch _operatorLatch;

  /// Merge table for FULL-layout blocks (also the only table for non-grouping-set queries).
  private volatile IndexedTable _indexedTable;
  /// Merge table for BASE-layout blocks (grouping-set base aggregation); derived into grouping sets on merge.
  private volatile IndexedTable _baseIndexedTable;
  private volatile boolean _groupsTrimmed;
  private volatile boolean _numGroupsLimitReached;
  private volatile boolean _numGroupsWarningLimitReached;

  public GroupByCombineOperator(List<Operator> operators, QueryContext queryContext, ExecutorService executorService) {
    super(null, operators, overrideMaxExecutionThreads(queryContext, operators.size()), executorService);

    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();
    assert aggregationFunctions != null;
    _numAggregationFunctions = aggregationFunctions.length;
    assert _queryContext.getGroupByExpressions() != null;
    _numKeyColumns = _queryContext.getNumGroupByKeyColumns();
    _numColumns = _numKeyColumns + _numAggregationFunctions;
    _groupingSets = _queryContext.isGroupingSets();
    _numBaseKeyColumns = _queryContext.getGroupByExpressions().size();
    _numBaseColumns = _numBaseKeyColumns + _numAggregationFunctions;
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
        /// Route the block by its record layout. A base-aggregation grouping-set segment emits BASE-layout
        /// blocks (no synthetic $groupingId column, exactly `numUnionColumns + numAggregationFunctions`
        /// columns); the expansion path (and non-grouping-set queries) emits full-layout blocks. The layout is
        /// per segment -- MV carve-out and cardinality gate are per-segment decisions -- so both tables can be
        /// live in the same query.
        boolean baseLayout = _groupingSets && resultsBlock.getDataSchema() != null
            && resultsBlock.getDataSchema().size() == _numBaseColumns;
        IndexedTable indexedTable = baseLayout ? ensureBaseIndexedTable(resultsBlock)
            : ensureIndexedTable(resultsBlock);
        int numKeyColumns = baseLayout ? _numBaseKeyColumns : _numKeyColumns;
        int numColumns = baseLayout ? _numBaseColumns : _numColumns;

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
                Object[] values = Arrays.copyOf(keys, numColumns);
                int groupId = groupKey._groupId;
                for (int i = 0; i < _numAggregationFunctions; i++) {
                  values[numKeyColumns + i] = aggregationGroupByResult.getResultForGroupId(i, groupId);
                }
                indexedTable.upsert(new Key(keys), new Record(values));
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
            indexedTable.upsert(intermediateResult._key, intermediateResult._record);
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

  private IndexedTable ensureIndexedTable(GroupByResultsBlock resultsBlock) {
    IndexedTable indexedTable = _indexedTable;
    if (indexedTable == null) {
      synchronized (this) {
        indexedTable = _indexedTable;
        if (indexedTable == null) {
          indexedTable = GroupByUtils.createIndexedTableForCombineOperator(resultsBlock, _queryContext, _numTasks,
              _executorService);
          _indexedTable = indexedTable;
        }
      }
    }
    return indexedTable;
  }

  private IndexedTable ensureBaseIndexedTable(GroupByResultsBlock resultsBlock) {
    IndexedTable baseIndexedTable = _baseIndexedTable;
    if (baseIndexedTable == null) {
      synchronized (this) {
        baseIndexedTable = _baseIndexedTable;
        if (baseIndexedTable == null) {
          baseIndexedTable = GroupByUtils.createIndexedTableForCombineOperator(resultsBlock, _queryContext, _numTasks,
              _executorService);
          _baseIndexedTable = baseIndexedTable;
        }
      }
    }
    return baseIndexedTable;
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

    if (_indexedTable != null && _indexedTable.isTrimmed() && _queryContext.isUnsafeTrim()) {
      _groupsTrimmed = true;
    }

    IndexedTable indexedTable = _indexedTable;
    /// Base aggregation: `_baseIndexedTable` holds the merged BASE groups (union grouping). Derive the
    /// individual grouping sets from them once, in parallel across the combine executor, into the final
    /// grouping-set table. This is where the per-set fan-out happens -- after the row-collapsing base merge and
    /// multi-threaded, so it never repeats the per-row expansion the segment phase would otherwise pay. When
    /// some segments used the expansion path (per-segment MV carve-out or cardinality gate), their full-layout
    /// records are merged into the derived table afterwards: both hold intermediate aggregates under the same
    /// grouping-set key space, so this is a plain aggregate merge.
    IndexedTable baseIndexedTable = _baseIndexedTable;
    if (baseIndexedTable != null) {
      // The base combine table is capped at numGroupsLimit; if it saturated, base keys may have been dropped
      // from every derived set, so surface the limit like the segment-level cap does.
      if (baseIndexedTable.size() >= _queryContext.getNumGroupsLimit()) {
        _numGroupsLimitReached = true;
      }
      IndexedTable derivedTable = GroupByUtils.deriveGroupingSetsFromMergedBaseTable(baseIndexedTable, _queryContext,
          _numTasks, _executorService);
      // The per-set server trim (groupingSetsMinServerTrimSize) drops groups; propagate the trimmed flag so the
      // broker response reports the approximation.
      if (derivedTable.isTrimmed()) {
        _groupsTrimmed = true;
      }
      if (indexedTable != null) {
        int mergedKeys = 0;
        for (Map.Entry<Key, Record> entry : indexedTable.getRecordEntries()) {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(mergedKeys++, EXPLAIN_NAME);
          derivedTable.upsert(entry.getKey(), entry.getValue());
        }
      }
      indexedTable = derivedTable;
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
