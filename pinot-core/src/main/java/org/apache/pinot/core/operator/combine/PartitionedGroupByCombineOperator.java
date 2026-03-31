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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ResultsBlockUtils;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.trace.TraceCallable;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Combine operator for group-by queries.
 * TODO: Use CombineOperatorUtils.getNumThreadsForQuery() to get the parallelism of the query instead of using
 *       all threads
 */
@SuppressWarnings("rawtypes")
public class PartitionedGroupByCombineOperator extends GroupByCombineOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionedGroupByCombineOperator.class);
  private static final String EXPLAIN_NAME = "PARTITIONED_COMBINE_GROUP_BY";

  protected final IndexedTable[] _indexedTables;
  private final Object[] _partitionLocks;
  private final int _partitionMask;

  public PartitionedGroupByCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService) {
    super(operators, queryContext, executorService);
    int numGroupByPartitions = Math.max(1, _queryContext.getNumGroupByPartitions());
    _indexedTables = new IndexedTable[numGroupByPartitions];
    _partitionLocks = new Object[numGroupByPartitions];
    Arrays.setAll(_partitionLocks, ignored -> new Object());
    _partitionMask = Integer.bitCount(numGroupByPartitions) == 1 ? numGroupByPartitions - 1 : -1;
    LOGGER.debug("Using {} for group-by combine, with {} partitions and {} numTasks", EXPLAIN_NAME,
        numGroupByPartitions,
        _numTasks);
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
    IndexedTable[] localIndexedTables = null;
    while (_processingException.get() == null && (operatorId = _nextOperatorId.getAndIncrement()) < _numOperators) {
      Operator operator = _operators.get(operatorId);
      try {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).acquire();
        }
        GroupByResultsBlock resultsBlock = (GroupByResultsBlock) operator.nextBlock();
        if (localIndexedTables == null) {
          localIndexedTables = new IndexedTable[_indexedTables.length];
        }
        mergeResultsBlockIntoLocalPartitions(localIndexedTables, resultsBlock);
      } catch (RuntimeException e) {
        throw wrapOperatorException(operator, e);
      } finally {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).release();
        }
      }
    }
    publishLocalPartitions(localIndexedTables);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Combines intermediate group-by result blocks from underlying operators and returns a merged one.
   * <ul>
   *   <li>Merges partition tables into a single table via parallel tree-reduction.</li>
   *   <li>Sets all exceptions encountered during execution into the merged result block.</li>
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

    Throwable processingException = _processingException.get();
    if (processingException != null) {
      return getExceptionResultsBlock(processingException);
    }

    List<IndexedTable> partitionTables = new ArrayList<>(_indexedTables.length);
    for (IndexedTable partitionTable : _indexedTables) {
      if (partitionTable != null) {
        partitionTables.add(partitionTable);
      }
    }
    IndexedTable indexedTable;
    try {
      indexedTable = mergePartitionTables(partitionTables);
    } catch (TimeoutException e) {
      long remainingTimeMs = _queryContext.getEndTimeMs() - System.currentTimeMillis();
      return getTimeoutResultsBlock(Math.max(0L, remainingTimeMs));
    }
    if (indexedTable == null) {
      return ResultsBlockUtils.buildEmptyQueryResults(_queryContext);
    }
    return getMergedResultsBlock(indexedTable);
  }

  private void mergeResultsBlockIntoLocalPartitions(IndexedTable[] localIndexedTables,
      GroupByResultsBlock resultsBlock) {
    updateCombineResultsStats(resultsBlock);

    Collection<IntermediateRecord> intermediateRecords = resultsBlock.getIntermediateRecords();
    int mergedKeys = 0;
    if (intermediateRecords == null) {
      AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
      if (aggregationGroupByResult != null) {
        try {
          Iterator<GroupKeyGenerator.GroupKey> dicGroupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
          while (dicGroupKeyIterator.hasNext()) {
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(mergedKeys++, EXPLAIN_NAME);
            GroupKeyGenerator.GroupKey groupKey = dicGroupKeyIterator.next();
            Object[] keys = groupKey._keys;
            Object[] values = Arrays.copyOf(keys, _numColumns);
            int groupId = groupKey._groupId;
            for (int i = 0; i < _numAggregationFunctions; i++) {
              values[_numGroupByExpressions + i] = aggregationGroupByResult.getResultForGroupId(i, groupId);
            }
            Key key = new Key(keys);
            getOrCreateLocalPartitionTable(localIndexedTables, getPartitionId(key), resultsBlock).upsert(key,
                new Record(values));
          }
        } finally {
          aggregationGroupByResult.closeGroupKeyGenerator();
        }
      }
    } else {
      for (IntermediateRecord intermediateResult : intermediateRecords) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(mergedKeys++, EXPLAIN_NAME);
        getOrCreateLocalPartitionTable(localIndexedTables, getPartitionId(intermediateResult._key), resultsBlock)
            .upsert(intermediateResult._key, intermediateResult._record);
      }
    }
  }

  private IndexedTable getOrCreateLocalPartitionTable(IndexedTable[] localIndexedTables, int partitionId,
      GroupByResultsBlock resultsBlock) {
    IndexedTable indexedTable = localIndexedTables[partitionId];
    if (indexedTable == null) {
      indexedTable = createIndexedTable(resultsBlock, 1);
      localIndexedTables[partitionId] = indexedTable;
    }
    return indexedTable;
  }

  private void publishLocalPartitions(IndexedTable[] localIndexedTables) {
    if (localIndexedTables == null) {
      return;
    }
    for (int partitionId = 0; partitionId < localIndexedTables.length; partitionId++) {
      IndexedTable localIndexedTable = localIndexedTables[partitionId];
      if (localIndexedTable == null) {
        continue;
      }
      synchronized (_partitionLocks[partitionId]) {
        IndexedTable indexedTable = _indexedTables[partitionId];
        if (indexedTable == null) {
          _indexedTables[partitionId] = localIndexedTable;
        } else if (localIndexedTable.size() > indexedTable.size()) {
          localIndexedTable.merge(indexedTable);
          localIndexedTable.absorbResizeStats(indexedTable);
          _indexedTables[partitionId] = localIndexedTable;
        } else {
          indexedTable.merge(localIndexedTable);
          indexedTable.absorbResizeStats(localIndexedTable);
        }
      }
    }
  }

  private IndexedTable mergePartitionTables(List<IndexedTable> partitionTables)
      throws Exception {
    int numPartitionTables = partitionTables.size();
    if (numPartitionTables == 0) {
      return null;
    }
    if (numPartitionTables == 1) {
      return partitionTables.get(0);
    }

    List<IndexedTable> tablesToMerge = partitionTables;
    if (tablesToMerge.size() <= 2 || _numTasks <= 1) {
      return mergePartitionTablesSequentially(tablesToMerge);
    }

    while (tablesToMerge.size() > 1) {
      if (_queryContext.getEndTimeMs() - System.currentTimeMillis() <= 0) {
        throw new TimeoutException("Timed out while reducing partitioned group-by results");
      }

      int numPairs = tablesToMerge.size() / 2;
      List<Future<IndexedTable>> futures = new ArrayList<>(numPairs);
      List<IndexedTable> nextRoundTables = new ArrayList<>((tablesToMerge.size() + 1) / 2);
      for (int i = 0; i < numPairs; i++) {
        IndexedTable leftTable = tablesToMerge.get(i * 2);
        IndexedTable rightTable = tablesToMerge.get(i * 2 + 1);
        futures.add(_executorService.submit(new TraceCallable<IndexedTable>() {
          @Override
          public IndexedTable callJob() {
            return mergePartitionTables(leftTable, rightTable);
          }
        }));
      }
      if ((tablesToMerge.size() & 1) == 1) {
        nextRoundTables.add(tablesToMerge.get(tablesToMerge.size() - 1));
      }
      try {
        for (Future<IndexedTable> future : futures) {
          long timeoutMs = _queryContext.getEndTimeMs() - System.currentTimeMillis();
          if (timeoutMs <= 0) {
            throw new TimeoutException("Timed out while reducing partitioned group-by results");
          }
          nextRoundTables.add(future.get(timeoutMs, TimeUnit.MILLISECONDS));
        }
      } catch (InterruptedException e) {
        cancelFutures(futures);
        Thread.currentThread().interrupt();
        throw e;
      } catch (TimeoutException e) {
        cancelFutures(futures);
        throw e;
      } catch (ExecutionException e) {
        cancelFutures(futures);
        throw new RuntimeException("Error while reducing partitioned group-by results", e.getCause());
      }
      tablesToMerge = nextRoundTables;
    }
    return tablesToMerge.get(0);
  }

  private IndexedTable mergePartitionTablesSequentially(List<IndexedTable> partitionTables) {
    IndexedTable indexedTable = partitionTables.get(0);
    for (int i = 1; i < partitionTables.size(); i++) {
      indexedTable = mergePartitionTables(indexedTable, partitionTables.get(i));
    }
    return indexedTable;
  }

  private static IndexedTable mergePartitionTables(IndexedTable leftTable, IndexedTable rightTable) {
    if (leftTable.size() >= rightTable.size()) {
      leftTable.mergePartitionTable(rightTable);
      return leftTable;
    }
    rightTable.mergePartitionTable(leftTable);
    return rightTable;
  }

  private static void cancelFutures(List<Future<IndexedTable>> futures) {
    for (Future<IndexedTable> future : futures) {
      future.cancel(true);
    }
  }

  private int getPartitionId(Key key) {
    int hashCode = key.hashCode();
    return _partitionMask >= 0 ? (hashCode & _partitionMask) : Math.floorMod(hashCode, _indexedTables.length);
  }
}
