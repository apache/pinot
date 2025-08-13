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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
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
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.accounting.ThreadResourceSnapshot;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryErrorMessage;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.trace.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Partitioned parallel combine operator for group-by queries.
 * See {@link PartitionedGroupByCombineOperator#processSegments()} for algorithm details
 */
@SuppressWarnings("rawtypes")
public class PartitionedGroupByCombineOperator extends BaseSingleBlockCombineOperator<GroupByResultsBlock> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionedGroupByCombineOperator.class);
  private static final String EXPLAIN_NAME = "COMBINE_GROUP_BY";

  private final int _numAggregationFunctions;
  private final int _numGroupByExpressions;
  private final int _numColumns;
  private final CountDownLatch _operatorLatch;
  private final int _numPartitions;

  private volatile DataSchema _dataSchema;
  private volatile IndexedTable _indexedTable;

  private final AtomicInteger _nextPartitionId = new AtomicInteger(0);
  private final TwoLevelHashMapIndexedTable[] _mergedIndexedTables;
  private volatile boolean _groupsTrimmed;
  private volatile boolean _numGroupsLimitReached;
  private volatile boolean _numGroupsWarningLimitReached;
  protected final Future[] _futures;

  private final LinkedBlockingQueue<List<IntermediateRecord>>[] _queues;

  @SuppressWarnings({"unchecked"})
  public PartitionedGroupByCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService) {
    super(null, operators, overrideMaxExecutionThreads(queryContext, operators.size()), executorService);

    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();
    assert aggregationFunctions != null;
    _numAggregationFunctions = aggregationFunctions.length;
    assert _queryContext.getGroupByExpressions() != null;
    _numGroupByExpressions = _queryContext.getGroupByExpressions().size();
    _numColumns = _numGroupByExpressions + _numAggregationFunctions;
    _numPartitions = queryContext.getGroupByNumPartitions();
    _operatorLatch = new CountDownLatch(_numPartitions);
    _futures = new Future[_numPartitions];

    _mergedIndexedTables = new TwoLevelHashMapIndexedTable[_numPartitions];
    _queues = new LinkedBlockingQueue[_numPartitions];
    for (int i = 0; i < _queues.length; i++) {
      _queues[i] = new LinkedBlockingQueue<>();
    }
  }

  /**
   * Copied from {@link BaseCombineOperator}, but fires {@code _numPartitions} tasks instead of {@code _numTasks}.
   * Start the combine operator process. This will spin up multiple threads to process data segments in parallel.
   */
  @Override
  protected void startProcess() {
    Tracing.activeRecording().setNumTasks(_numPartitions);
    ThreadExecutionContext parentContext = Tracing.getThreadAccountant().getThreadExecutionContext();
    for (int i = 0; i < _numPartitions; i++) {
      int taskId = i;
      _futures[i] = _executorService.submit(new TraceRunnable() {
        @Override
        public void runJob() {
          ThreadResourceSnapshot resourceSnapshot = new ThreadResourceSnapshot();
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

          _totalWorkerThreadCpuTimeNs.getAndAdd(resourceSnapshot.getCpuTimeNs());
          _totalWorkerThreadMemAllocatedBytes.getAndAdd(resourceSnapshot.getAllocatedBytes());
        }
      });
    }
  }

  /**
   * Stop the combine operator process. This will stop all sub-tasks that were spun up to process data segments.
   */
  @Override
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

  /**
   * For group-by queries, when maxExecutionThreads is not explicitly configured, override it to create as many tasks
   * as the default number of query worker threads (or the number of operators / segments if that's lower).
   * TODO: This should be useless since numPartitions is used instead, remove.
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
   * Producer-consumer version of partition-merge
   * <p>
   * Two-phase Partitioned GroupBy Execution.
   * </p><p>
   * This handles any cases that has an OrderBy clause.
   * </p><p>
   * Phase 1:
   * each thread repeatedly takes a single segment and partition it into thread-local
   * {@link RadixPartitionedIntermediateRecords}
   * </p><p>
   * Phase 2:
   * each thread merge one partition of all {@link RadixPartitionedIntermediateRecords} into a single
   * {@link TwoLevelHashMapIndexedTable} that internally uses a {@link TwoLevelLinearProbingRecordHashMap}
   * for minimal movement and copy of the payload {@link IntermediateRecord}s
   * </p><p>
   * When all threads finishes, {@link PartitionedGroupByCombineOperator#mergePartitionedGroupByResults()} is called
   * to logically
   * stitch the {@link TwoLevelHashMapIndexedTable} together into a single {@link RadixPartitionedHashMap} and
   * then wrap it into a {@link org.apache.pinot.core.data.table.PartitionedIndexedTable} as final indexedTable
   * </p><p>
   * </p>
   */
  @Override
  protected void processSegments() {
    int partitionId = _nextPartitionId.getAndIncrement();
    int progress = 0;
    int operatorId = -1;
    List<IntermediateRecord> localPendingPartition = null;
    TwoLevelHashMapIndexedTable table = null;
    LinkedBlockingQueue<List<IntermediateRecord>> queue = _queues[partitionId];

    while (_processingException.get() == null) {
      // merge
      if (localPendingPartition != null) {
        int mergedKeys = 0;
        for (IntermediateRecord record : localPendingPartition) {
          table.upsert(record);
          Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(mergedKeys++);
        }
        localPendingPartition = null;
        if (++progress == _numOperators) {
          _mergedIndexedTables[partitionId] = table;
          return;
        }
      }

      try {
        while (!queue.isEmpty() || operatorId >= _numOperators) {
          if (_processingException.get() != null) {
            return;
          }
          long timeoutMs = _queryContext.getEndTimeMs() - System.currentTimeMillis();
          List<IntermediateRecord> partition = queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
          assert (partition != null);
          if (table == null) {
            table = GroupByUtils.createIndexedTableForPartitionMerge(_dataSchema, _queryContext, _executorService);
          }
          int mergedKeys = 0;
          for (IntermediateRecord record : partition) {
            table.upsert(record);
            Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(mergedKeys++);
          }
          if (++progress == _numOperators) {
            _mergedIndexedTables[partitionId] = table;
            return;
          }
        }
      } catch (RuntimeException | InterruptedException e) {
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
        if (_queryContext.getLimit() == 0) {
          return;
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

  /// Get IntermediateRecords from resultBlock and partition in-place
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
        try {
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
            Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(mergedKeys++);
          }
          intermediateRecords = Arrays.asList(convertedRecords);
        } finally {
          aggregationGroupByResult.closeGroupKeyGenerator();
        }
      } else {
        // empty segment result
        intermediateRecords = new ArrayList<>();
      }
    }

    // in-place partition segment results
    return new RadixPartitionedIntermediateRecords(_queryContext.getGroupByPartitionNumRadixBits(), operatorId,
        intermediateRecords);
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

    if (_queryContext.getLimit() == 0) {
      return new GroupByResultsBlock(_dataSchema, Collections.emptyList(), _queryContext);
    }

    return mergePartitionedGroupByResults();
  }
}
