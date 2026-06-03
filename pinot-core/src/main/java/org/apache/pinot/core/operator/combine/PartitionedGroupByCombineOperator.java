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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryErrorMessage;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Partitioned non-blocking combine operator for group-by queries.
 *
 * <p>Records from segment results are hash-routed to {@code P} independent partitions. Each worker thread builds
 * {@code P} thread-local {@link org.apache.pinot.core.data.table.SimpleIndexedTable}s (one per partition) and
 * processes its assigned segments without any cross-thread contention during the accumulation phase. After all
 * segments are processed, a per-partition tournament-exchange protocol (using {@code P} independent lock objects)
 * merges the per-thread partition tables into {@code P} single tables. Finally, the {@code P} partition tables are
 * merged into one result in {@link #mergeResults()}.
 *
 * <p>Trim strategy: each thread maintains a raw record counter (including duplicate-key upserts) as a cheap proxy for
 * unique group count. When it reaches {@code _globalGroupTrimThreshold} (the same threshold a non-partitioned combine
 * would use), the actual unique group count (sum of all partition table sizes) is verified first. If the actual count
 * is below the threshold (duplicate-key upserts inflated the counter), the counter is reset without trimming —
 * matching the resize semantics of {@link NonblockingGroupByCombineOperator}. If the actual count also exceeds the
 * threshold, all partition-local tables are trimmed simultaneously via {@link IndexedTable#trim()}. This avoids the
 * over-trimming that would occur if each partition trimmed independently at {@code T/P}: a hot partition that receives
 * most keys would trim at {@code T/P} entries even though the global total is far below {@code T}.
 *
 * <p>Compared with {@link NonblockingGroupByCombineOperator}:
 * <ul>
 *   <li>Tournament merges for different partitions can proceed in parallel (different lock objects).</li>
 *   <li>Final merge of {@code P} tables in {@code mergeResults()} adds a small sequential overhead.</li>
 * </ul>
 */
@SuppressWarnings("rawtypes")
public class PartitionedGroupByCombineOperator extends GroupByCombineOperator {
  public static final String ALGORITHM = "PARTITIONED-NON-BLOCKING";
  // Shadows the parent's EXPLAIN_NAME so that toExplainString() and checkTermination sampling correctly
  // attribute CPU time to this operator rather than to the generic COMBINE_GROUP_BY parent.
  private static final String EXPLAIN_NAME = "COMBINE_GROUP_BY_PARTITIONED";

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionedGroupByCombineOperator.class);

  private final int _numPartitions;
  // Per-partition shared result slots used for the tournament exchange; _partitionLocks[p] guards slot p.
  private final IndexedTable[] _partitionedSharedTables;
  private final Object[] _partitionLocks;
  // The threshold at which a worker thread trims one of its partition-local tables. Equal to the effective trim
  // threshold a non-partitioned combine would use (Integer.MAX_VALUE when trim is disabled). Each thread
  // independently tracks its own total across all P local tables and triggers a trim when the total reaches this
  // value; only the largest partition is trimmed per event (cache-friendly, same total work as trimming all P).
  private final int _globalGroupTrimThreshold;
  // Initial-capacity hint for each partition table: max(T / P, 0). Pre-sizing to at least T/P entries prevents
  // HashMap growth (and treeification) during normal per-thread accumulation and tournament merges.
  private final int _partitionCapacityHint;
  // Stores the first results block seen by any worker thread; used to construct an empty table when all partitions
  // are empty (all segments produced no groups). Only one write can win via compareAndSet — all blocks for a given
  // query share the same DataSchema so any non-null value is acceptable. The value is read only in mergeResults(),
  // which runs after awaitOperatorLatch() provides the happens-before barrier that guarantees visibility.
  private final AtomicReference<GroupByResultsBlock> _firstResultsBlock = new AtomicReference<>();

  public PartitionedGroupByCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService) {
    this(operators, queryContext, executorService, Math.max(2, Math.min(operators.size(), 8)));
  }

  public PartitionedGroupByCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService, int numPartitions) {
    super(operators, queryContext, executorService);
    _numPartitions = numPartitions;
    _partitionedSharedTables = new IndexedTable[numPartitions];
    _partitionLocks = new Object[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      _partitionLocks[i] = new Object();
    }
    // Use the same trim threshold a non-partitioned combine would use. Each thread tracks its own total entries
    // across all P local partition tables and triggers a trim of the largest partition when the total reaches
    // this value. This avoids over-trimming hot partitions (which would happen if each partition trimmed at T/P).
    _globalGroupTrimThreshold = GroupByUtils.getEffectiveCombineTrimThreshold(queryContext);
    // Pre-size each partition table to hold at least T/P entries without any HashMap growth, which prevents
    // treeification of HashMap bins during per-thread accumulation and tournament-deposit merges.
    _partitionCapacityHint =
        _globalGroupTrimThreshold == Integer.MAX_VALUE ? 0 : _globalGroupTrimThreshold / _numPartitions;
    LOGGER.debug("Using {} for group-by combine with {} tasks and {} partitions", ALGORITHM, _numTasks, _numPartitions);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  /**
   * Processes all assigned segments, routing each group-key record to one of {@code P} partition-local tables by
   * {@code (key.hashCode() & Integer.MAX_VALUE) % P}. After all segments are processed the thread performs a
   * per-partition tournament deposit, merging its local table for each partition into the shared per-partition slot.
   */
  @Override
  protected void processSegments() {
    IndexedTable[] localTables = new IndexedTable[_numPartitions];
    // Raw record counter (includes duplicate-key upserts). Used as a cheap proxy for unique group count:
    // when it reaches _globalGroupTrimThreshold, maybeTrimLocalPartitions() verifies the actual unique count
    // before deciding whether to trim. This matches the trim semantics of NonblockingGroupByCombineOperator.
    int threadLocalTotal = 0;

    int operatorId;
    while (_processingException.get() == null && (operatorId = _nextOperatorId.getAndIncrement()) < _numOperators) {
      Operator operator = _operators.get(operatorId);
      try {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).acquire();
        }
        GroupByResultsBlock resultsBlock = (GroupByResultsBlock) operator.nextBlock();
        _firstResultsBlock.compareAndSet(null, resultsBlock);
        updateCombineResultsStats(resultsBlock);
        threadLocalTotal = mergeIntoPartitions(localTables, resultsBlock, threadLocalTotal);
      } catch (RuntimeException e) {
        throw wrapOperatorException(operator, e);
      } finally {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).release();
        }
      }
    }

    // Per-partition tournament: each local table competes for its partition slot
    for (int p = 0; p < _numPartitions; p++) {
      if (localTables[p] != null) {
        depositPartition(localTables, p);
      }
    }
  }

  /**
   * Routes each record in {@code resultsBlock} to the appropriate partition table in {@code localTables}, creating
   * the partition table lazily on first write. The initial-capacity hint is scaled by {@code 1 / _numPartitions}
   * since each partition table holds approximately that fraction of the total groups.
   *
   * <p>The {@code threadLocalTotal} counter tracks the total records seen across all partition-local tables since
   * the last trim (or since the thread started). When the counter reaches {@code _globalGroupTrimThreshold}, the
   * actual unique group count (sum of all partition table sizes) is compared against the threshold. This
   * two-phase check ensures trim fires on <em>unique group count</em>, not raw record count — matching the
   * behavior of {@link NonblockingGroupByCombineOperator}. Records that update existing groups (duplicates)
   * increment the counter cheaply but do not trigger a trim unless the actual unique count is also high.
   *
   * @return the updated {@code threadLocalTotal} after processing this results block
   */
  private int mergeIntoPartitions(IndexedTable[] localTables, GroupByResultsBlock resultsBlock, int threadLocalTotal) {
    // Scale the numGroups hint: each partition holds ~1/P of the total groups.
    // Floor at _partitionCapacityHint (= T/P) so the underlying HashMap is pre-sized to hold all expected
    // groups without growth, preventing HashMap bin treeification during accumulation and tournament merges.
    // Guard getNumGroups(): it asserts that at least one of aggregationGroupByResult / intermediateRecords is
    // non-null; use _partitionCapacityHint as the sole hint for blocks that carry neither (e.g. empty blocks).
    Collection<IntermediateRecord> intermediateRecords = resultsBlock.getIntermediateRecords();
    AggregationGroupByResult aggregationGroupByResult =
        intermediateRecords == null ? resultsBlock.getAggregationGroupByResult() : null;
    int numGroupsHint = (intermediateRecords != null || aggregationGroupByResult != null)
        ? Math.max(_partitionCapacityHint, resultsBlock.getNumGroups() / _numPartitions) : _partitionCapacityHint;
    int mergedKeys = 0;
    if (intermediateRecords == null) {
      if (aggregationGroupByResult != null) {
        try {
          Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
          while (groupKeyIterator.hasNext()) {
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(mergedKeys++, EXPLAIN_NAME);
            GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
            Object[] keys = groupKey._keys;
            Object[] values = Arrays.copyOf(keys, _numColumns);
            int groupId = groupKey._groupId;
            for (int i = 0; i < _numAggregationFunctions; i++) {
              values[_numGroupByExpressions + i] = aggregationGroupByResult.getResultForGroupId(i, groupId);
            }
            Key key = new Key(keys);
            int p = partitionFor(key);
            if (localTables[p] == null) {
              localTables[p] =
                  GroupByUtils.createPartitionTableForCombineOperator(resultsBlock, _queryContext, numGroupsHint,
                      _executorService);
            }
            localTables[p].upsert(key, new Record(values));
            if (_globalGroupTrimThreshold != Integer.MAX_VALUE && ++threadLocalTotal >= _globalGroupTrimThreshold) {
              threadLocalTotal = maybeTrimLocalPartitions(localTables);
            }
          }
        } finally {
          aggregationGroupByResult.closeGroupKeyGenerator();
        }
      }
    } else {
      for (IntermediateRecord intermediateResult : intermediateRecords) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(mergedKeys++, EXPLAIN_NAME);
        int p = partitionFor(intermediateResult._key);
        if (localTables[p] == null) {
          localTables[p] =
              GroupByUtils.createPartitionTableForCombineOperator(resultsBlock, _queryContext, numGroupsHint,
                  _executorService);
        }
        localTables[p].upsert(intermediateResult._key, intermediateResult._record);
        if (_globalGroupTrimThreshold != Integer.MAX_VALUE && ++threadLocalTotal >= _globalGroupTrimThreshold) {
          threadLocalTotal = maybeTrimLocalPartitions(localTables);
        }
      }
    }
    return threadLocalTotal;
  }

  /**
   * Two-phase trim: first computes the actual unique group count (sum of all partition table sizes). If the
   * actual total is below {@code _globalGroupTrimThreshold} (the record counter over-counted due to
   * duplicate-key upserts), trims are skipped and the counter is reset to the actual unique count. This matches
   * the behavior of {@link NonblockingGroupByCombineOperator}, whose {@link IndexedTable} fires resize only when
   * {@code _lookupMap.size()} — unique groups — reaches the threshold.
   *
   * <p>When trim is needed, partitions are trimmed (largest first) in a loop until the total drops below
   * {@code _globalGroupTrimThreshold}. Trimming all necessary partitions in one invocation avoids a "trim storm"
   * where returning a total that is still at or above the threshold would cause the caller to immediately
   * re-invoke trim after the very next upsert — especially costly when {@code P} is large and trimming only one
   * partition per event would fire P rapid-succession trims.
   *
   * @return the updated {@code threadLocalTotal}: actual unique count after trim (or without trim if skipped),
   *         guaranteed to be below {@code _globalGroupTrimThreshold}
   */
  private int maybeTrimLocalPartitions(IndexedTable[] localTables) {
    // Compute actual unique group count and locate the largest partition in one pass (O(P), each size() is O(1)).
    int actualTotal = 0;
    int maxSize = 0;
    int maxIdx = -1;
    for (int i = 0; i < localTables.length; i++) {
      IndexedTable t = localTables[i];
      if (t != null) {
        int sz = t.size();
        actualTotal += sz;
        if (sz > maxSize) {
          maxSize = sz;
          maxIdx = i;
        }
      }
    }
    if (actualTotal < _globalGroupTrimThreshold) {
      // Record count exceeded threshold but unique group count did not (duplicate-key upserts inflated the
      // counter). Reset to actual unique count without trimming, matching NON-BLOCKING behavior.
      return actualTotal;
    }
    // Trim partitions (largest first) until the total drops below the threshold. Trimming all needed partitions
    // in one call prevents a "trim storm" where a single-partition trim still leaves the total at or above the
    // threshold, causing the caller to re-fire trim after every subsequent upsert.
    while (actualTotal >= _globalGroupTrimThreshold && maxIdx >= 0) {
      int preTrimSize = maxSize;
      localTables[maxIdx].trim();
      actualTotal = actualTotal - preTrimSize + localTables[maxIdx].size();
      // Find the new largest for the next iteration (if still needed).
      if (actualTotal >= _globalGroupTrimThreshold) {
        maxSize = 0;
        maxIdx = -1;
        for (int i = 0; i < localTables.length; i++) {
          IndexedTable t = localTables[i];
          if (t != null) {
            int sz = t.size();
            if (sz > maxSize) {
              maxSize = sz;
              maxIdx = i;
            }
          }
        }
      }
    }
    return actualTotal;
  }

  /**
   * Returns the partition index for the given key using Knuth's multiplicative hash.
   *
   * <p>A plain {@code h % P} or {@code (h ^ h>>>16) % P} would cause bucket clustering inside the partition
   * tables: all partition-p keys would share the same low {@code log2(P)} bits in Java's HashMap bucket selector
   * {@code (h ^ h>>>16) & (capacity-1)}, causing every partition table to use only {@code capacity/P} effective
   * buckets and triggering {@link java.util.HashMap} bin treeification.
   *
   * <p>Multiplying by the 32-bit Fibonacci/golden-ratio constant {@code 0x9E3779B9} (Knuth's multiplicative hash)
   * thoroughly mixes all 32 input bits into the result. The product's bit pattern is orthogonal to the
   * {@code h ^ h>>>16} expression that Java's HashMap uses for bucket selection: keys that map to the same
   * multiplicative-hash partition are uniformly scattered across all HashMap buckets, eliminating clustering and
   * treeification regardless of key distribution or table capacity.
   *
   * <p>Uses {@code & Integer.MAX_VALUE} (clear sign bit) rather than {@code Math.abs()} because
   * {@code Math.abs(Integer.MIN_VALUE)} returns {@code Integer.MIN_VALUE} (negative), while
   * {@code Integer.MIN_VALUE & Integer.MAX_VALUE} = 0 (safe non-negative).
   */
  private int partitionFor(Key key) {
    // Knuth's multiplicative hash: multiply by the 32-bit Fibonacci constant to scatter bits.
    int h = key.hashCode() * 0x9E3779B9;
    return (h & Integer.MAX_VALUE) % _numPartitions;
  }

  /**
   * Performs a tournament deposit for partition {@code p}: tries to claim the empty shared slot or steal and merge
   * with the current occupant. The winning (larger) table becomes the new local candidate and loops until it
   * successfully claims the slot. Called at most once per partition index per thread.
   */
  private void depositPartition(IndexedTable[] localTables, int p) {
    // Use a local variable so we can re-assign after merges
    IndexedTable table = localTables[p];
    boolean deposited = false;
    while (!deposited) {
      IndexedTable toMerge;
      synchronized (_partitionLocks[p]) {
        if (_partitionedSharedTables[p] == null) {
          _partitionedSharedTables[p] = table;
          deposited = true;
          break;
        }
        toMerge = _partitionedSharedTables[p];
        _partitionedSharedTables[p] = null;
      }
      // Merge outside the lock: merge smaller INTO larger
      if (table.size() > toMerge.size()) {
        table.merge(toMerge);
        table.absorbTrimStats(toMerge);
      } else {
        toMerge.merge(table);
        toMerge.absorbTrimStats(table);
        table = toMerge;
      }
    }
  }

  /**
   * Waits for all worker tasks to finish, then merges the {@code P} per-partition winner tables into a single result.
   */
  @Override
  public BaseResultsBlock mergeResults()
      throws Exception {
    long timeoutMs = _queryContext.getEndTimeMs() - System.currentTimeMillis();
    boolean opCompleted = awaitOperatorLatch(timeoutMs);
    if (!opCompleted) {
      return getTimeoutResultsBlock(timeoutMs);
    }

    Throwable ex = _processingException.get();
    if (ex != null) {
      return getExceptionResultsBlock(ex);
    }

    // Merge P partition winner tables into one final table.
    // absorbTrimStats is intentionally omitted here: each partition winner already accumulated stats from
    // the per-partition tournament exchange (analogous to NonblockingGroupByCombineOperator). Adding the
    // cross-partition stats on top would inflate _numResizes by a factor of P, misleading isTrimmed().
    IndexedTable finalTable = null;
    for (int p = 0; p < _numPartitions; p++) {
      IndexedTable partTable = _partitionedSharedTables[p];
      if (partTable == null) {
        continue;
      }
      if (finalTable == null) {
        finalTable = partTable;
      } else {
        finalTable.merge(partTable);
      }
    }

    if (finalTable == null) {
      // All segments produced zero groups (e.g. WHERE clause matched no rows). Return an empty table.
      GroupByResultsBlock firstResultsBlock = _firstResultsBlock.get();
      if (firstResultsBlock != null) {
        finalTable = createIndexedTable(firstResultsBlock, 1);
      } else {
        // No segments were processed at all (zero operators, or all workers exited before setting _firstResultsBlock).
        String msg = "No segments were processed by " + ALGORITHM;
        LOGGER.warn(msg);
        return new ExceptionResultsBlock(new QueryErrorMessage(QueryErrorCode.QUERY_EXECUTION, msg, msg));
      }
    }

    return getMergedResultsBlock(finalTable);
  }
}
