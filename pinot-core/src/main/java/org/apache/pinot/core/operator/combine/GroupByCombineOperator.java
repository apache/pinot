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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.RadixPartitionedHashMap;
import org.apache.pinot.core.data.table.RadixPartitionedIntermediateRecords;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.TwoLevelHashMapIndexedTable;
import org.apache.pinot.core.data.table.TwoLevelLinearProbingRecordHashMap;
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

  private static final Logger LOGGER = LoggerFactory.getLogger(GroupByCombineOperator.class);
  private static final String EXPLAIN_NAME = "COMBINE_GROUP_BY";

  private final int _numAggregationFunctions;
  private final int _numGroupByExpressions;
  private final int _numColumns;
  private final CountDownLatch _operatorLatch;
  private final int _numPartitions;

  private volatile DataSchema _dataSchema;
  private volatile IndexedTable _indexedTable;
  private final CompletableFuture<RadixPartitionedIntermediateRecords>[] _partitionedRecordsFutures;

  private final AtomicInteger _nextPartitionId = new AtomicInteger(0);
  private final TwoLevelHashMapIndexedTable[] _mergedIndexedTables;
  private volatile boolean _groupsTrimmed;
  private volatile boolean _numGroupsLimitReached;
  private volatile boolean _numGroupsWarningLimitReached;

  private final LinkedBlockingQueue<List<IntermediateRecord>>[] _queues;

  public GroupByCombineOperator(List<Operator> operators, QueryContext queryContext, ExecutorService executorService) {
    super(null, operators, overrideMaxExecutionThreads(queryContext, operators.size()), executorService);

    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();
    assert aggregationFunctions != null;
    _numAggregationFunctions = aggregationFunctions.length;
    assert _queryContext.getGroupByExpressions() != null;
    _numGroupByExpressions = _queryContext.getGroupByExpressions().size();
    _numColumns = _numGroupByExpressions + _numAggregationFunctions;
    _operatorLatch = new CountDownLatch(_numTasks);
    _partitionedRecordsFutures = new CompletableFuture[operators.size()];

    for (int i = 0; i < operators.size(); i++) {
      _partitionedRecordsFutures[i] = new CompletableFuture<>();
    }

    _numPartitions = _numTasks;
    _queryContext.setGroupByPartitionNumRadixBits(31 - Integer.numberOfLeadingZeros(_numPartitions));
//    System.out.println("numPartitions: " + _numPartitions
//    + ", numRadixBits: " + _queryContext.getGroupByPartitionNumRadixBits());

//    _numPartitions = _queryContext.getGroupByNumPartitions();
    _mergedIndexedTables = new TwoLevelHashMapIndexedTable[_numPartitions];
    _queues = new LinkedBlockingQueue[_numPartitions];
    for (int i = 0; i < _queues.length; i++) {
      _queues[i] = new LinkedBlockingQueue<>();
    }
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
   * producer-consumer version of partition-merge
   * <p>
   * Two-phase Partitioned GroupBy Execution.
   * </p><p>
   * This handles any cases that has an OrderBy clause.
   * </p><p>
   * phase 1:
   * each thread repeatedly takes a single segment and partition it into thread-local
   * {@link RadixPartitionedIntermediateRecords}
   * </p><p>
   * phase 2:
   * each thread merge one partition of all {@link RadixPartitionedIntermediateRecords} into a single
   * {@link TwoLevelHashMapIndexedTable} that internally uses a {@link TwoLevelLinearProbingRecordHashMap}
   * for minimal movement and copy of the payload {@link IntermediateRecord}s
   * </p><p>
   * when all threads finishes, {@link GroupByCombineOperator#mergePartitionedGroupByResults()} is called to logically
   * stitch the {@link TwoLevelHashMapIndexedTable} together into a single {@link RadixPartitionedHashMap} and
   * then wrap it into a {@link org.apache.pinot.core.data.table.PartitionedIndexedTable} as final indexedTable
   * </p><p>
   * </p>
   */
  protected void processPartitionedGroupByStreaming() {
    int partitionId = _nextPartitionId.getAndIncrement();
    int progress = 0;
    int operatorId = -1;
    List<IntermediateRecord> localPendingPartition = null;
    TwoLevelHashMapIndexedTable table = null;
    LinkedBlockingQueue<List<IntermediateRecord>> queue = _queues[partitionId];

    while (true) {
      // merge
      if (localPendingPartition != null) {
        for (IntermediateRecord record : localPendingPartition) {
          table.upsert(record);
        }
        localPendingPartition = null;
        if (++progress == _numOperators) {
          _mergedIndexedTables[partitionId] = table;
          return;
        }
      }

      try {
        while (!queue.isEmpty() || operatorId >= _numOperators) {
          long timeoutMs = _queryContext.getEndTimeMs() - System.currentTimeMillis();
          List<IntermediateRecord> partition = queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
          assert (partition != null);
          if (table == null) {
            table = GroupByUtils.createIndexedTableForPartitionMerge(_dataSchema, _queryContext, _executorService);
          }
          for (IntermediateRecord record : partition) {
            table.upsert(record);
          }
          if (++progress == _numOperators) {
            _mergedIndexedTables[partitionId] = table;
            return;
          }
        }
      } catch (Exception e) {
        throw wrapOperatorException(this, new RuntimeException(e));
      }

      // partition
      operatorId = _nextOperatorId.getAndIncrement();
      if (operatorId >= _numOperators) {
        continue;
      }
      Operator operator = _operators.get(operatorId);
      try {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).acquire();
        }

        GroupByResultsBlock resultsBlock = (GroupByResultsBlock) operator.nextBlock();
        if (_dataSchema == null) {
          _dataSchema = resultsBlock.getDataSchema();
        }
        if (table == null) {
          table = GroupByUtils.createIndexedTableForPartitionMerge(_dataSchema, _queryContext, _executorService);
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
        RadixPartitionedIntermediateRecords partitionedRecords =
            getRadixPartitionedIntermediateRecords(resultsBlock, operatorId);

        for (int i = 0; i < _numPartitions; i++) {
          if (i == partitionId) {
            localPendingPartition = partitionedRecords.getPartition(i);
            continue;
          }
          _queues[i].offer(partitionedRecords.getPartition(i));
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

  /// get IntermediateRecords from resultBlock and partition in-place
  private RadixPartitionedIntermediateRecords getRadixPartitionedIntermediateRecords(GroupByResultsBlock resultsBlock,
      int operatorId) {
    List<IntermediateRecord> intermediateRecords = resultsBlock.getIntermediateRecords();
    // Count the number of merged keys
    int mergedKeys = 0;
    // For now, only GroupBy OrderBy query has pre-constructed intermediate records
    if (intermediateRecords == null) {
      // Merge aggregation group-by result.
      AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
      if (aggregationGroupByResult != null) {
        IntermediateRecord[] convertedRecords = new IntermediateRecord[aggregationGroupByResult.getNumGroups()];
        // Iterate over the group-by keys, for each key, update the group-by result in the indexedTable
        Iterator<GroupKeyGenerator.GroupKey> dicGroupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
        int idx = 0;
        while (dicGroupKeyIterator.hasNext()) {
          GroupKeyGenerator.GroupKey groupKey = dicGroupKeyIterator.next();
          Object[] keys = groupKey._keys;
          Object[] values = Arrays.copyOf(keys, _numColumns);
          int groupId = groupKey._groupId;
          for (int i = 0; i < _numAggregationFunctions; i++) {
            values[_numGroupByExpressions + i] = aggregationGroupByResult.getResultForGroupId(i, groupId);
          }
          convertedRecords[idx++] = new IntermediateRecord(new Key(keys), new Record(values), null);
          Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(mergedKeys);
          mergedKeys++;
        }
        intermediateRecords = Arrays.asList(convertedRecords);
      } else {
        // empty segment result
        intermediateRecords = new ArrayList<>();
      }
    }

    // in-place partition segment results
    RadixPartitionedIntermediateRecords partitionedRecords =
        new RadixPartitionedIntermediateRecords(_queryContext.getGroupByPartitionNumRadixBits(), operatorId,
            intermediateRecords);
    return partitionedRecords;
  }

  /**
   * Executes query on one segment in a worker thread and merges the results into the indexed table.
   */
  @Override
  protected void processSegments() {
    if (GroupByUtils.shouldPartitionGroupBy(_queryContext)) {
      // partition segment results
      processPartitionedGroupByStreaming();
//      processPartitionedGroupBy();
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
   * Logically stitch all merged {@link TwoLevelHashMapIndexedTable}'s _lookupMap
   * into a single {@link RadixPartitionedHashMap}, then wrap it into a
   * {@link org.apache.pinot.core.data.table.PartitionedIndexedTable}.
   * sort and trim the indexedTable and return result block
   */
  private GroupByResultsBlock mergePartitionedGroupByResults() {
    TwoLevelLinearProbingRecordHashMap[] lookupMaps =
        new TwoLevelLinearProbingRecordHashMap[_queryContext.getGroupByNumPartitions()];
    int idx = 0;
    int resizeNum = 0;
    long resizeTimeMs = 0;
    for (TwoLevelHashMapIndexedTable table : _mergedIndexedTables) {
      if (table.isTrimmed()) {
        _groupsTrimmed = true;
      }
      lookupMaps[idx++] = table.getLookupMap();
      resizeNum += table.getNumResizes();
      resizeTimeMs = Math.max(resizeTimeMs, table.getResizeTimeMs());
    }
    // keep all partition records in the map, later finish will trim them if needed
    RadixPartitionedHashMap map = new RadixPartitionedHashMap(lookupMaps,
        _queryContext.getGroupByPartitionNumRadixBits());
    _indexedTable = GroupByUtils.createPartitionedIndexedTableForCombineOperator(_dataSchema, _queryContext, map,
        _executorService);

    // finish table and set stats
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
    mergedBlock.setNumResizes(resizeNum);
    mergedBlock.setResizeTimeMs(resizeTimeMs);
    return mergedBlock;
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
      return mergePartitionedGroupByResults();
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
