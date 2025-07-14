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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.RadixPartitionedHashMap;
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
import org.apache.pinot.spi.trace.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Combine operator for group-by queries.
 */
@SuppressWarnings("rawtypes")
public class GroupByCombineOperator extends BaseSingleBlockCombineOperator<GroupByResultsBlock> {
  public static final int MAX_TRIM_THRESHOLD = 1_000_000_000;

  private static final Logger LOGGER = LoggerFactory.getLogger(GroupByCombineOperator.class);
  private static final String EXPLAIN_NAME = "COMBINE_GROUP_BY";

  private final int _numAggregationFunctions;
  private final int _numGroupByExpressions;
  private final int _numColumns;
  // We use a CountDownLatch to track if all Futures are finished by the query timeout, and cancel the unfinished
  // _futures (try to interrupt the execution if it already started).
  private final CountDownLatch _operatorLatch;

  private DataSchema _dataSchema;
  private volatile IndexedTable _indexedTable;
  private final RadixPartitionedHashMap<Key, Record>[] _partitionedHashMaps;
  private volatile HashMap[] _mergedHashMaps;
  private volatile boolean _groupsTrimmed;
  private volatile boolean _numGroupsLimitReached;
  private volatile boolean _numGroupsWarningLimitReached;

  public GroupByCombineOperator(List<Operator> operators, QueryContext queryContext, ExecutorService executorService) {
    super(null, operators, overrideMaxExecutionThreads(queryContext, operators.size()), executorService);

    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();
    assert aggregationFunctions != null;
    _numAggregationFunctions = aggregationFunctions.length;
    assert _queryContext.getGroupByExpressions() != null;
    _numGroupByExpressions = _queryContext.getGroupByExpressions().size();
    _numColumns = _numGroupByExpressions + _numAggregationFunctions;
    _operatorLatch = new CountDownLatch(_numTasks);
    _partitionedHashMaps = new RadixPartitionedHashMap[operators.size()];
    _mergedHashMaps = new HashMap[_queryContext.getGroupByNumPartitions()];
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
   * partitionedGroupBy phase 1 execution,
   * create thread-local partitioned hashTable
   */
  protected void processPartitionedGroupBy() {
    int operatorId;
    while (_processingException.get() == null && (operatorId = _nextOperatorId.getAndIncrement()) < _numOperators) {
      Operator operator = _operators.get(operatorId);
      try {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).acquire();
        }
        GroupByResultsBlock resultsBlock = (GroupByResultsBlock) operator.nextBlock();
        if (_dataSchema == null) {
          _dataSchema = resultsBlock.getDataSchema();
        }
        // partition this into _partitionedIndexedTable[operatorId], without trimming
        RadixPartitionedHashMap<Key, Record> map =
            new RadixPartitionedHashMap<>(_queryContext.getGroupByPartitionNumRadixBits());

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
            Iterator<GroupKeyGenerator.GroupKey> dicGroupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
            while (dicGroupKeyIterator.hasNext()) {
              GroupKeyGenerator.GroupKey groupKey = dicGroupKeyIterator.next();
              Object[] keys = groupKey._keys;
              Object[] values = Arrays.copyOf(keys, _numColumns);
              int groupId = groupKey._groupId;
              for (int i = 0; i < _numAggregationFunctions; i++) {
                values[_numGroupByExpressions + i] = aggregationGroupByResult.getResultForGroupId(i, groupId);
              }
              map.put(new Key(keys), new Record(values));
              Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(mergedKeys);
              mergedKeys++;
            }
          }
        } else {
          for (IntermediateRecord intermediateResult : intermediateRecords) {
            //TODO: change upsert api so that it accepts intermediateRecord directly
            map.put(intermediateResult._key, intermediateResult._record);
            Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(mergedKeys);
            mergedKeys++;
          }
        }
        _partitionedHashMaps[operatorId] = map;
      } catch (RuntimeException e) {
        throw wrapOperatorException(operator, e);
      } finally {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).release();
        }
      }
    }
  }

  /**
   * partitionedGroupBy phase 2 execution,
   * each thread merge one partition of all hashTables
   * finally stitch them into one
   */
  private void mergePartitionedGroupBy()
      throws RuntimeException {
    List<Future> futures = new ArrayList<>();
    try {
      // now all partitionedHashMaps are ready, submit tasks that combines the partition
      for (int i = 0; i < _queryContext.getGroupByNumPartitions(); i++) {
        int partitionId = i;
        futures.add(_executorService.submit(() -> {
          HashMap<Key, Record> map = new HashMap<>();
          AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();
          int numKeyColumns = _queryContext.getGroupByExpressions().size();
          for (RadixPartitionedHashMap<Key, Record> partitionedHashMap : _partitionedHashMaps) {
            Map<Key, Record> partition = partitionedHashMap.getPartition(partitionId);
            for (Map.Entry<Key, Record> entry : partition.entrySet()) {
              GroupByUtils.addOrUpdateRecord(map, entry.getKey(), entry.getValue(), aggregationFunctions,
                  numKeyColumns);
            }
          }
          _mergedHashMaps[partitionId] = map;
        }));
      }
      // wait and stitch together mergedHashMaps
      for (Future fut : futures) {
        fut.get();
      }

      RadixPartitionedHashMap<Key, Record> mergedMap =
          new RadixPartitionedHashMap<>(Arrays.asList(_mergedHashMaps),
              _queryContext.getGroupByPartitionNumRadixBits());

      _indexedTable = GroupByUtils.createPartitionedIndexedTableForCombineOperator(_dataSchema, _queryContext,
          mergedMap, _executorService);
    } catch (Exception e) {
      // TODO: check error handling
      throw wrapOperatorException(this, new RuntimeException(e));
    } finally {
      for (Future future : futures) {
        if (future != null && !future.isDone()) {
          future.cancel(true);
        }
      }
    }
  }

  /**
   * Executes query on one segment in a worker thread and merges the results into the indexed table.
   */
  @Override
  protected void processSegments() {
    if (GroupByUtils.shouldPartitionGroupBy(_queryContext)) {
      // partition segment results
      processPartitionedGroupBy();
      return;
    }
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
            Iterator<GroupKeyGenerator.GroupKey> dicGroupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
            while (dicGroupKeyIterator.hasNext()) {
              GroupKeyGenerator.GroupKey groupKey = dicGroupKeyIterator.next();
              Object[] keys = groupKey._keys;
              Object[] values = Arrays.copyOf(keys, _numColumns);
              int groupId = groupKey._groupId;
              for (int i = 0; i < _numAggregationFunctions; i++) {
                values[_numGroupByExpressions + i] = aggregationGroupByResult.getResultForGroupId(i, groupId);
              }
              _indexedTable.upsert(new Key(keys), new Record(values));
              Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(mergedKeys);
              mergedKeys++;
            }
          }
        } else {
          for (IntermediateRecord intermediateResult : intermediateRecords) {
            //TODO: change upsert api so that it accepts intermediateRecord directly
            _indexedTable.upsert(intermediateResult._key, intermediateResult._record);
            Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(mergedKeys);
            mergedKeys++;
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
   * <p>Combines intermediate selection result blocks from underlying operators and returns a merged one.
   * <ul>
   *   <li>
   *     Merges multiple intermediate selection result blocks as a merged one.
   *   </li>
   *   <li>
   *     Set all exceptions encountered during execution into the merged result block
   *   </li>
   * </ul>
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

    if (GroupByUtils.shouldPartitionGroupBy(_queryContext)) {
      // multithreaded merge partitioned hashtable, this creates _indexedTable
      mergePartitionedGroupBy();
    }

    if (_indexedTable.isTrimmed() && _queryContext.isUnsafeTrim()) {
      _groupsTrimmed = true;
    }

    IndexedTable indexedTable = _indexedTable;
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
