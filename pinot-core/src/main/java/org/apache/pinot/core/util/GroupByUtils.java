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
package org.apache.pinot.core.util;

import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.context.GroupingSets;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.data.table.ConcurrentIndexedTable;
import org.apache.pinot.core.data.table.DeterministicConcurrentIndexedTable;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.SimpleIndexedTable;
import org.apache.pinot.core.data.table.SortedRecords;
import org.apache.pinot.core.data.table.SortedRecordsMerger;
import org.apache.pinot.core.data.table.TableResizer;
import org.apache.pinot.core.data.table.UnboundedConcurrentIndexedTable;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction.SerializedIntermediateResult;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.reduce.DataTableReducerContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.query.QueryThreadContext;


public final class GroupByUtils {
  private GroupByUtils() {
  }

  public static final int DEFAULT_MIN_NUM_GROUPS = 5000;
  public static final int MAX_TRIM_THRESHOLD = 1_000_000_000;

  /// Builds the segment-level [GroupByResultsBlock] for a GROUP BY GROUPING SETS / ROLLUP / CUBE query,
  /// shared by `GroupByOperator` and `FilteredGroupByOperator`. When the group count exceeds the
  /// per-set budget (`perSetTrimSize * numGroupingSets`), a per-set bucketed trim (keyed on the
  /// `$groupingId` discriminator at `discriminatorColumnIndex`) keeps each grouping set's own top
  /// candidates so a global top-K cannot starve low-magnitude sets such as the grand total; otherwise the full
  /// segment result is returned. The broker still applies the final ORDER BY + LIMIT across all sets.
  ///
  /// @param discriminatorColumnIndex index of the synthetic $groupingId column, i.e. the number of union
  ///                                 group-by columns
  public static GroupByResultsBlock buildGroupingSetsResultsBlock(QueryContext queryContext, DataSchema dataSchema,
      GroupKeyGenerator groupKeyGenerator, GroupByResultHolder[] groupByResultHolders, int numGroups,
      int discriminatorColumnIndex, boolean numGroupsLimitReached, boolean numGroupsWarningLimitReached) {
    GroupByResultsBlock resultsBlock;
    int perSetTrimSize = queryContext.getGroupingSetSegmentTrimSize();
    int numGroupingSets = queryContext.getGroupingSets().size();
    if (perSetTrimSize > 0 && numGroups > (long) perSetTrimSize * numGroupingSets) {
      TableResizer tableResizer = new TableResizer(dataSchema, queryContext);
      List<IntermediateRecord> intermediateRecords =
          tableResizer.trimInSegmentResultsByGroupingSet(groupKeyGenerator, groupByResultHolders, perSetTrimSize,
              discriminatorColumnIndex);
      groupKeyGenerator.close();
      ServerMetrics.get().addMeteredGlobalValue(ServerMeter.AGGREGATE_TIMES_GROUPS_TRIMMED, 1);
      resultsBlock = new GroupByResultsBlock(dataSchema, intermediateRecords, queryContext);
      resultsBlock.setGroupsTrimmed(true);
    } else {
      AggregationGroupByResult aggregationGroupByResult =
          new AggregationGroupByResult(groupKeyGenerator, queryContext.getAggregationFunctions(), groupByResultHolders);
      resultsBlock = new GroupByResultsBlock(dataSchema, aggregationGroupByResult, queryContext);
    }
    resultsBlock.setNumGroupsLimitReached(numGroupsLimitReached);
    resultsBlock.setNumGroupsWarningLimitReached(numGroupsWarningLimitReached);
    return resultsBlock;
  }

  /// Derives the individual grouping sets from a merged BASE-grouping [IndexedTable] (union columns aggregated
  /// once, like a plain GROUP BY), for a GROUP BY GROUPING SETS / ROLLUP / CUBE query using base aggregation.
  /// Each base group is projected into every grouping set: its rolled-up (non-participating) columns are set to
  /// `null`, the `$groupingId` discriminator is inserted after the union columns, and the base group's
  /// aggregation intermediates are merged into the derived group. This moves the per-set fan-out from O(rows) to
  /// O(base groups) and runs it here -- after the row-collapsing base merge -- across the combine's threads,
  /// rather than expanding every scanned row.
  ///
  /// The grouping sets are partitioned round-robin across up to `numTasks` worker tasks; each task derives its
  /// owned sets over all base entries into a task-local plain HashMap. Task key spaces are disjoint (every
  /// derived key carries its set ordinal), so the heavy merge work runs contention-free -- no concurrent table,
  /// no bin locks, no shared size counters -- and the task maps are then unioned single-threaded into the result
  /// table with plain stores (never merges). Profiling showed the previous shared-concurrent-table derive spent
  /// most of its time in ConcurrentHashMap machinery rather than in the projection itself.
  ///
  /// Clone discipline: a base group's intermediate flows into every grouping set (and is read concurrently by
  /// other tasks), while [AggregationFunction#merge] mutates/returns its arguments. Each derived group's stored
  /// accumulator is therefore a clone (see [#cloneIntermediate]), and OBJECT intermediates are re-cloned per
  /// merge so a merge can never mutate the shared base accumulator another task is still reading. Scalar
  /// intermediates are immutable and clone-free.
  public static IndexedTable deriveGroupingSetsFromMergedBaseTable(IndexedTable baseTable, QueryContext queryContext,
      int numTasks, ExecutorService executorService) {
    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
    assert aggregationFunctions != null;
    int numAggregationFunctions = aggregationFunctions.length;
    List<int[]> groupingSets = queryContext.getGroupingSets();
    int numSets = groupingSets.size();
    int numUnionColumns = queryContext.getGroupByExpressions().size();
    // Per grouping set: membership mask over the union columns (true = participates, false = rolled up to NULL).
    boolean[][] setContains = new boolean[numSets][numUnionColumns];
    for (int s = 0; s < numSets; s++) {
      for (int columnIndex : groupingSets.get(s)) {
        setContains[s][columnIndex] = true;
      }
    }

    // Grouping-set output schema: the base schema with the synthetic $groupingId INT column inserted right after
    // the union group-by columns (mirroring GroupByOperator's grouping-set schema layout).
    DataSchema groupingSetsSchema = insertGroupingIdColumn(baseTable.getDataSchema(), numUnionColumns);
    // The derive must not drop groups: it is a bounded transformation of the already-bounded base groups (whose
    // count is capped at numGroupsLimit per segment), so the derived count is at most numGroupsLimit * numSets --
    // a finite amount. Capping the derived table at numGroupsLimit would drop derived groups NON-DETERMINISTICALLY
    // under the parallel upsert (whichever threads fill the quota first win), which can starve an entire
    // low-magnitude grouping set such as the grand total. Use an unbounded result size here and defer the real
    // ORDER BY + LIMIT to the broker (per-set trim below still bounds it when explicitly configured).
    int derivedUpperBound = (int) Math.min((long) baseTable.size() * numSets, Integer.MAX_VALUE);
    int initialCapacity = getIndexedTableInitialCapacity(derivedUpperBound, derivedUpperBound,
        queryContext.getMinInitialIndexedTableCapacity());

    List<Map.Entry<Key, Record>> baseEntries = new ArrayList<>(baseTable.getRecordEntries());
    // A full-union set (one that contains every union column, i.e. the identity grouping) maps base groups to
    // derived groups 1:1: keys are unique so no merge ever runs on its records, and its stored intermediates can
    // safely be the base objects themselves (after the pre-serialize pass below, no other task touches them).
    boolean[] isFullUnionSet = new boolean[numSets];
    for (int s = 0; s < numSets; s++) {
      boolean fullUnion = true;
      for (int col = 0; col < numUnionColumns; col++) {
        fullUnion &= setContains[s][col];
      }
      isFullUnionSet[s] = fullUnion;
    }

    /// Serialize each OBJECT base intermediate exactly ONCE, on exactly one thread (parallel by base-entry
    /// range, so no object is shared between pre-pass tasks). This is a correctness requirement, not just a
    /// perf win: serialization can MUTATE the accumulator (TDigest#compress rewrites centroid arrays; theta /
    /// CPC / tuple sketch accumulators flush their pending lists in getResult), so concurrent serialization of
    /// the same base object by multiple derive tasks corrupts it. After this pass, derive tasks read only the
    /// immutable bytes and deserialize their own private copies.
    SerializedIntermediateResult[][] serializedIntermediates =
        preSerializeObjectIntermediates(baseEntries, aggregationFunctions, numUnionColumns, numTasks,
            queryContext, executorService);

    int numTaskSlots = Math.max(1, Math.min(numTasks, numSets));
    List<Map<Key, Record>[]> taskResults = new ArrayList<>(numTaskSlots);
    if (numTaskSlots == 1) {
      taskResults.add(deriveSets(baseEntries, serializedIntermediates, setContains, isFullUnionSet, 0, 1,
          numUnionColumns, numAggregationFunctions, aggregationFunctions));
    } else {
      List<Future<Map<Key, Record>[]>> futures = new ArrayList<>(numTaskSlots);
      for (int t = 0; t < numTaskSlots; t++) {
        int taskIndex = t;
        futures.add(executorService.submit(() -> deriveSets(baseEntries, serializedIntermediates, setContains,
            isFullUnionSet, taskIndex, numTaskSlots, numUnionColumns, numAggregationFunctions,
            aggregationFunctions)));
      }
      taskResults.addAll(awaitAll(futures, queryContext));
    }
    // Per-set maps in ordinal order (each produced by exactly one task).
    Map<Key, Record>[] perSetMaps = taskResults.get(0);
    for (int t = 1; t < taskResults.size(); t++) {
      Map<Key, Record>[] taskSetMaps = taskResults.get(t);
      for (int s = 0; s < numSets; s++) {
        if (taskSetMaps[s] != null) {
          perSetMaps[s] = taskSetMaps[s];
        }
      }
    }

    /// Union the disjoint per-set maps into the result table, bounding the derived output at `numGroupsLimit`
    /// like the expansion path's combine table. Sets are admitted COARSEST FIRST (fewest participating columns,
    /// then ordinal), so low-magnitude sets such as the grand total and the subtotals always survive the cap and
    /// only the finest (largest) sets get trimmed; a trim is surfaced via the table's trimmed flag. Without a
    /// trim, the largest per-set map is adopted as the table's backing map so its keys are never re-hashed. The
    /// deterministic accurate-group-by mode needs a sorted (skip-list) backing map, so it uses upserts instead.
    int derivedCap = Math.max(queryContext.getNumGroupsLimit(), numSets);
    long totalDerived = 0;
    for (int s = 0; s < numSets; s++) {
      totalDerived += perSetMaps[s] != null ? perSetMaps[s].size() : 0;
    }
    Integer[] setOrder = new Integer[numSets];
    for (int s = 0; s < numSets; s++) {
      setOrder[s] = s;
    }
    Arrays.sort(setOrder, (a, b) -> {
      int lengthCompare = Integer.compare(groupingSets.get(a).length, groupingSets.get(b).length);
      return lengthCompare != 0 ? lengthCompare : Integer.compare(a, b);
    });
    IndexedTable derivedTable;
    if (useDeterministicIndexedTable(queryContext)) {
      derivedTable = getTrimDisabledIndexedTable(groupingSetsSchema, false, queryContext, Integer.MAX_VALUE,
          initialCapacity, 1, executorService);
      int admitted = 0;
      for (int s : setOrder) {
        Map<Key, Record> setMap = perSetMaps[s];
        if (setMap == null) {
          continue;
        }
        for (Map.Entry<Key, Record> entry : setMap.entrySet()) {
          if (admitted >= derivedCap) {
            break;
          }
          derivedTable.upsert(entry.getKey(), entry.getValue());
          admitted++;
        }
      }
      if (totalDerived > derivedCap) {
        derivedTable.markTrimmed();
      }
    } else if (totalDerived <= derivedCap) {
      Map<Key, Record> mergedMap = null;
      for (int s = 0; s < numSets; s++) {
        Map<Key, Record> setMap = perSetMaps[s];
        if (setMap != null && (mergedMap == null || setMap.size() > mergedMap.size())) {
          mergedMap = setMap;
        }
      }
      if (mergedMap == null) {
        mergedMap = new HashMap<>();
      }
      for (int s = 0; s < numSets; s++) {
        Map<Key, Record> setMap = perSetMaps[s];
        if (setMap != null && setMap != mergedMap) {
          mergedMap.putAll(setMap);
        }
      }
      derivedTable = new SimpleIndexedTable(groupingSetsSchema, false, queryContext, Integer.MAX_VALUE,
          Integer.MAX_VALUE, Integer.MAX_VALUE, mergedMap, executorService);
    } else {
      Map<Key, Record> mergedMap = new HashMap<>(HashUtil.getHashMapCapacity(derivedCap));
      int admitted = 0;
      for (int s : setOrder) {
        Map<Key, Record> setMap = perSetMaps[s];
        if (setMap == null) {
          continue;
        }
        for (Map.Entry<Key, Record> entry : setMap.entrySet()) {
          if (admitted >= derivedCap) {
            break;
          }
          mergedMap.put(entry.getKey(), entry.getValue());
          admitted++;
        }
      }
      derivedTable = new SimpleIndexedTable(groupingSetsSchema, false, queryContext, Integer.MAX_VALUE,
          Integer.MAX_VALUE, Integer.MAX_VALUE, mergedMap, executorService);
      derivedTable.markTrimmed();
    }

    /// Optional server-side per-set trim: when configured (and there is an ORDER BY), keep at most K groups
    /// within each grouping set (bucketed by $groupingId), bounding this server's derived output for
    /// high-cardinality unions. Bucketing per set means a global top-K cannot starve a low-magnitude set. This
    /// is an approximate top-K (deferred exact ORDER BY + LIMIT still runs at the broker); unset -> keep all.
    int serverTrimSize = queryContext.getGroupingSetServerTrimSize();
    if (serverTrimSize > 0 && derivedTable.size() > (long) serverTrimSize * numSets) {
      derivedTable.finish(false);
      TableResizer tableResizer = new TableResizer(groupingSetsSchema, queryContext);
      List<IntermediateRecord> kept = tableResizer.trimTableByGroupingSet(derivedTable, serverTrimSize,
          numUnionColumns);
      ServerMetrics.get().addMeteredGlobalValue(ServerMeter.AGGREGATE_TIMES_GROUPS_TRIMMED, 1);
      IndexedTable trimmedTable =
          buildIndexedTableFromRecords(groupingSetsSchema, queryContext, kept, executorService);
      // Surface the approximation: groups were dropped on the server, so the merged block (and ultimately the
      // broker response) must be flagged as trimmed.
      trimmedTable.markTrimmed();
      return trimmedTable;
    }
    return derivedTable;
  }

  /// Builds a trim-disabled grouping-set [IndexedTable] pre-populated with the given (already unique) records.
  /// Used to materialize the per-set-trim survivors back into the table the combine returns.
  private static IndexedTable buildIndexedTableFromRecords(DataSchema dataSchema, QueryContext queryContext,
      List<IntermediateRecord> records, ExecutorService executorService) {
    int numRecords = records.size();
    int initialCapacity =
        getIndexedTableInitialCapacity(numRecords, numRecords, queryContext.getMinInitialIndexedTableCapacity());
    // Populated single-threaded, so use a plain-HashMap-backed table (numThreads = 1).
    IndexedTable table = getTrimDisabledIndexedTable(dataSchema, false, queryContext, Integer.MAX_VALUE,
        initialCapacity, 1, executorService);
    for (IntermediateRecord record : records) {
      table.upsert(record._key, record._record);
    }
    return table;
  }

  /// Serializes each OBJECT aggregation intermediate of each base entry exactly once, parallel by base-entry
  /// range (no object is shared between pre-pass tasks). Returns `null` when the query has no OBJECT
  /// intermediates (scalar intermediates are immutable and need no cloning). Row `i` of the result corresponds
  /// to `baseEntries.get(i)`; column `j` is `null` for non-OBJECT aggregation `j`.
  @Nullable
  private static SerializedIntermediateResult[][] preSerializeObjectIntermediates(
      List<Map.Entry<Key, Record>> baseEntries, AggregationFunction[] aggregationFunctions, int numUnionColumns,
      int numTasks, QueryContext queryContext, ExecutorService executorService) {
    int numAggregationFunctions = aggregationFunctions.length;
    boolean hasObjectIntermediate = false;
    for (AggregationFunction aggregationFunction : aggregationFunctions) {
      hasObjectIntermediate |= aggregationFunction.getIntermediateResultColumnType() == ColumnDataType.OBJECT;
    }
    if (!hasObjectIntermediate || baseEntries.isEmpty()) {
      return null;
    }
    int numEntries = baseEntries.size();
    SerializedIntermediateResult[][] serialized = new SerializedIntermediateResult[numEntries][];
    int numChunks = Math.max(1, Math.min(numTasks, numEntries));
    if (numChunks == 1) {
      serializeChunk(baseEntries, serialized, 0, numEntries, numUnionColumns, aggregationFunctions);
    } else {
      int chunkSize = (numEntries + numChunks - 1) / numChunks;
      List<Future<Void>> futures = new ArrayList<>(numChunks);
      for (int c = 0; c < numChunks; c++) {
        int from = c * chunkSize;
        int to = Math.min(from + chunkSize, numEntries);
        if (from >= to) {
          break;
        }
        futures.add(executorService.submit(() -> {
          serializeChunk(baseEntries, serialized, from, to, numUnionColumns, aggregationFunctions);
          return null;
        }));
      }
      awaitAll(futures, queryContext);
    }
    return serialized;
  }

  private static void serializeChunk(List<Map.Entry<Key, Record>> baseEntries,
      SerializedIntermediateResult[][] serialized, int from, int to, int numUnionColumns,
      AggregationFunction[] aggregationFunctions) {
    int numAggregationFunctions = aggregationFunctions.length;
    for (int e = from; e < to; e++) {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(e - from, "GroupByUtils#serializeChunk");
      Object[] baseValues = baseEntries.get(e).getValue().getValues();
      SerializedIntermediateResult[] row = new SerializedIntermediateResult[numAggregationFunctions];
      for (int i = 0; i < numAggregationFunctions; i++) {
        Object intermediate = baseValues[numUnionColumns + i];
        if (intermediate != null
            && aggregationFunctions[i].getIntermediateResultColumnType() == ColumnDataType.OBJECT) {
          row[i] = aggregationFunctions[i].serializeIntermediateResult(intermediate);
        }
      }
      serialized[e] = row;
    }
  }

  /// One derive task: projects every base group into the grouping sets owned by `taskIndex` (set `s` is owned
  /// when `s % numTaskSlots == taskIndex`) and aggregates each owned set into its own task-local map (returned
  /// in an array indexed by set ordinal; non-owned slots are `null`). Each task runs single-threaded over its
  /// own maps, so plain HashMaps suffice; key spaces are disjoint because every derived key ends with its set
  /// ordinal.
  ///
  /// Cloning: OBJECT intermediates are never read from the shared base objects here -- they are deserialized
  /// from the pre-serialized bytes (see [#preSerializeObjectIntermediates]), each call producing a private
  /// copy, so no derive task can observe or cause mutation of a base accumulator. Scalar intermediates are
  /// immutable and pass through directly. Full-union sets store the base objects themselves: their records
  /// never merge (base keys are unique) and after the pre-pass no other task touches those objects.
  @SuppressWarnings("unchecked")
  private static Map<Key, Record>[] deriveSets(List<Map.Entry<Key, Record>> baseEntries,
      @Nullable SerializedIntermediateResult[][] serializedIntermediates, boolean[][] setContains,
      boolean[] isFullUnionSet, int taskIndex, int numTaskSlots, int numUnionColumns, int numAggregationFunctions,
      AggregationFunction[] aggregationFunctions) {
    int numSets = setContains.length;
    Map<Key, Record>[] setMaps = new Map[numSets];
    for (int s = taskIndex; s < numSets; s += numTaskSlots) {
      // A set produces at most one derived group per base entry; presize the full-union (identity) set for
      // exactly that (it never rehashes), and let coarser sets start small and grow.
      setMaps[s] = isFullUnionSet[s] ? new HashMap<>(HashUtil.getHashMapCapacity(baseEntries.size()))
          : new HashMap<>();
    }
    // Flyweight probe: reuse one key buffer for lookups (HashMap.get does not retain its argument) and copy it
    // into a fresh array only when inserting a new derived group. Most projections into coarse sets hit an
    // existing group, so this avoids a key-array + Key allocation per projection on the merge path.
    Object[] probeValues = new Object[numUnionColumns + 1];
    Key probeKey = new Key(probeValues);
    int numProjections = 0;
    for (int e = 0; e < baseEntries.size(); e++) {
      Map.Entry<Key, Record> baseEntry = baseEntries.get(e);
      Object[] baseKeys = baseEntry.getKey().getValues();
      Object[] baseValues = baseEntry.getValue().getValues();
      SerializedIntermediateResult[] serializedRow =
          serializedIntermediates != null ? serializedIntermediates[e] : null;
      for (int s = taskIndex; s < numSets; s += numTaskSlots) {
        // Keep the derive responsive to query timeout / cancellation and visible to resource accounting, like
        // the segment-merge loop in GroupByCombineOperator.
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(numProjections++, "GroupByUtils#deriveSets");
        Map<Key, Record> setMap = setMaps[s];
        boolean[] contains = setContains[s];
        for (int col = 0; col < numUnionColumns; col++) {
          probeValues[col] = contains[col] ? baseKeys[col] : null;
        }
        probeValues[numUnionColumns] = s;
        // Full-union sets never see a duplicate key, so skip the probe and insert blindly.
        boolean fullUnion = isFullUnionSet[s];
        Record existing = fullUnion ? null : setMap.get(probeKey);
        if (existing == null) {
          Object[] keyValues = probeValues.clone();
          Object[] values = new Object[numUnionColumns + 1 + numAggregationFunctions];
          System.arraycopy(keyValues, 0, values, 0, numUnionColumns + 1);
          for (int i = 0; i < numAggregationFunctions; i++) {
            values[numUnionColumns + 1 + i] = fullUnion ? baseValues[numUnionColumns + i]
                : cloneIntermediate(aggregationFunctions[i], baseValues, serializedRow, numUnionColumns, i);
          }
          setMap.put(new Key(keyValues), new Record(values));
        } else {
          Object[] values = existing.getValues();
          for (int i = 0; i < numAggregationFunctions; i++) {
            int valueIndex = numUnionColumns + 1 + i;
            // The first argument is this set's owned accumulator (mutating it is safe). The second argument is
            // a private deserialized copy, because AggregationFunction#merge may mutate it or RETURN it as the
            // new accumulator (e.g. HyperLogLog merge with mismatched sizes).
            values[valueIndex] = AggregationFunctionUtils.merge(aggregationFunctions[i], values[valueIndex],
                cloneIntermediate(aggregationFunctions[i], baseValues, serializedRow, numUnionColumns, i));
          }
        }
      }
    }
    return setMaps;
  }

  /// Merges base records that OVERFLOWED the combine base table (their base key arrived after the table hit
  /// `numGroupsLimit` and was dropped) into the ALREADY EXISTING derived groups, mirroring the expansion path's
  /// behavior under the group limit: the grand total and the coarse subtotals -- whose groups exist -- stay
  /// exact, and only the overflowing fine-set groups are lost. Runs single-threaded on the merge thread; each
  /// OBJECT intermediate is serialized once here (single owner) and a private copy is deserialized per set.
  public static void mergeOverflowBaseRecords(IndexedTable derivedTable, List<Record> overflowRecords,
      QueryContext queryContext) {
    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
    assert aggregationFunctions != null;
    int numAggregationFunctions = aggregationFunctions.length;
    List<int[]> groupingSets = queryContext.getGroupingSets();
    int numSets = groupingSets.size();
    int numUnionColumns = queryContext.getGroupByExpressions().size();
    boolean[][] setContains = new boolean[numSets][numUnionColumns];
    for (int s = 0; s < numSets; s++) {
      for (int columnIndex : groupingSets.get(s)) {
        setContains[s][columnIndex] = true;
      }
    }
    int numMerged = 0;
    for (Record overflowRecord : overflowRecords) {
      Object[] baseValues = overflowRecord.getValues();
      SerializedIntermediateResult[] serializedRow = new SerializedIntermediateResult[numAggregationFunctions];
      for (int i = 0; i < numAggregationFunctions; i++) {
        Object intermediate = baseValues[numUnionColumns + i];
        if (intermediate != null
            && aggregationFunctions[i].getIntermediateResultColumnType() == ColumnDataType.OBJECT) {
          serializedRow[i] = aggregationFunctions[i].serializeIntermediateResult(intermediate);
        }
      }
      for (int s = 0; s < numSets; s++) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(numMerged++,
            "GroupByUtils#mergeOverflowBaseRecords");
        boolean[] contains = setContains[s];
        Object[] keyValues = new Object[numUnionColumns + 1];
        for (int col = 0; col < numUnionColumns; col++) {
          keyValues[col] = contains[col] ? baseValues[col] : null;
        }
        keyValues[numUnionColumns] = s;
        Object[] values = new Object[numUnionColumns + 1 + numAggregationFunctions];
        System.arraycopy(keyValues, 0, values, 0, numUnionColumns + 1);
        for (int i = 0; i < numAggregationFunctions; i++) {
          values[numUnionColumns + 1 + i] =
              cloneIntermediate(aggregationFunctions[i], baseValues, serializedRow, numUnionColumns, i);
        }
        derivedTable.upsertExisting(new Key(keyValues), new Record(values));
      }
    }
  }

  /// Waits for all futures, bounded by the query deadline when one is set, cancelling the whole batch on
  /// interrupt, timeout or failure.
  private static <T> List<T> awaitAll(List<Future<T>> futures, QueryContext queryContext) {
    List<T> results = new ArrayList<>(futures.size());
    try {
      long endTimeMs = queryContext.getEndTimeMs();
      for (Future<T> future : futures) {
        if (endTimeMs > 0) {
          results.add(future.get(Math.max(endTimeMs - System.currentTimeMillis(), 0), TimeUnit.MILLISECONDS));
        } else {
          results.add(future.get());
        }
      }
      return results;
    } catch (InterruptedException e) {
      cancelAll(futures);
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while deriving grouping sets", e);
    } catch (TimeoutException e) {
      cancelAll(futures);
      throw new RuntimeException("Timed out while deriving grouping sets", e);
    } catch (ExecutionException e) {
      cancelAll(futures);
      throw new RuntimeException("Caught exception while deriving grouping sets", e.getCause());
    }
  }

  private static <T> void cancelAll(List<Future<T>> futures) {
    for (Future<T> future : futures) {
      future.cancel(true);
    }
  }

  /// Returns `schema` with a synthetic `$groupingId` INT column inserted at `index` (after the union group-by
  /// columns), producing the grouping-set output schema from the base-grouping schema.
  private static DataSchema insertGroupingIdColumn(DataSchema baseSchema, int index) {
    String[] baseNames = baseSchema.getColumnNames();
    ColumnDataType[] baseTypes = baseSchema.getColumnDataTypes();
    int numColumns = baseNames.length + 1;
    String[] names = new String[numColumns];
    ColumnDataType[] types = new ColumnDataType[numColumns];
    System.arraycopy(baseNames, 0, names, 0, index);
    System.arraycopy(baseTypes, 0, types, 0, index);
    names[index] = GroupingSets.GROUPING_ID_COLUMN;
    types[index] = ColumnDataType.INT;
    System.arraycopy(baseNames, index, names, index + 1, baseNames.length - index);
    System.arraycopy(baseTypes, index, types, index + 1, baseTypes.length - index);
    return new DataSchema(names, types);
  }

  /// Returns a private copy of aggregation `i`'s intermediate for one base record, safe to hand to
  /// [AggregationFunction#merge]: scalar (non-OBJECT) intermediates are immutable boxed values returned as-is
  /// (`serializedRow[i]` is null for them); `null` (nothing aggregated) is the merge identity; OBJECT
  /// accumulators are deserialized from the pre-serialized bytes, never touching the shared base object.
  private static Object cloneIntermediate(AggregationFunction aggregationFunction, Object[] baseValues,
      @Nullable SerializedIntermediateResult[] serializedRow, int numUnionColumns, int i) {
    SerializedIntermediateResult serialized = serializedRow != null ? serializedRow[i] : null;
    if (serialized == null) {
      return baseValues[numUnionColumns + i];
    }
    return aggregationFunction.deserializeIntermediateResult(
        new CustomObject(serialized.getType(), ByteBuffer.wrap(serialized.getBytes())));
  }

  /// Returns the capacity of the table required by the given query. NOTE: It returns `max(limit * 5, 5000)` to
  /// ensure the result accuracy.
  public static int getTableCapacity(int limit) {
    return getTableCapacity(limit, DEFAULT_MIN_NUM_GROUPS);
  }

  /// Returns the capacity of the table required by the given query. NOTE: It returns
  /// `max(limit * 5, minNumGroups)` where minNumGroups is configurable to tune the table size and result
  /// accuracy.
  public static int getTableCapacity(int limit, int minNumGroups) {
    long capacityByLimit = limit * 5L;
    return capacityByLimit > Integer.MAX_VALUE ? Integer.MAX_VALUE : Math.max((int) capacityByLimit, minNumGroups);
  }

  /// Returns the actual trim threshold used for the indexed table. Trim threshold should be at least (2 \* trimSize) to
  /// avoid excessive trimming. When trim threshold is non-positive or higher than 10^9, trim is considered disabled,
  /// where `Integer.MAX_VALUE` is returned.
  @VisibleForTesting
  static int getIndexedTableTrimThreshold(int trimSize, int trimThreshold) {
    if (trimThreshold <= 0 || trimThreshold > MAX_TRIM_THRESHOLD || trimSize > MAX_TRIM_THRESHOLD / 2) {
      return Integer.MAX_VALUE;
    }
    return Math.max(trimThreshold, 2 * trimSize);
  }

  /// Returns the initial capacity of the indexed table required by the given query.
  @VisibleForTesting
  public static int getIndexedTableInitialCapacity(int maxRowsToKeep, int minNumGroups, int minCapacity) {
    // The upper bound of the initial capacity is the capacity required to hold all the required rows. The indexed table
    // should never grow over this capacity.
    int upperBound = HashUtil.getHashMapCapacity(maxRowsToKeep);
    if (minCapacity > upperBound) {
      return upperBound;
    }
    // The lower bound of the initial capacity is the capacity required by the min number of groups to be added to the
    // table.
    int lowerBound = HashUtil.getHashMapCapacity(minNumGroups);
    if (lowerBound > upperBound) {
      return upperBound;
    }
    return Math.max(minCapacity, lowerBound);
  }

  /// Creates an indexed table for the combine operator given a sample results block.
  public static IndexedTable createIndexedTableForCombineOperator(GroupByResultsBlock resultsBlock,
      QueryContext queryContext, int numThreads, ExecutorService executorService) {
    DataSchema dataSchema = resultsBlock.getDataSchema();
    int numGroups = resultsBlock.getNumGroups();
    int limit = queryContext.getLimit();
    boolean hasOrderBy = queryContext.getOrderByExpressions() != null;
    boolean hasHaving = queryContext.getHavingFilter() != null;
    int minTrimSize =
        queryContext.getMinServerGroupTrimSize(); // it's minBrokerGroupTrimSize in broker
    int minInitialIndexedTableCapacity = queryContext.getMinInitialIndexedTableCapacity();

    /// Grouping-set queries must not trim per server: a global ORDER BY top-K here would drop a row that ranks
    /// higher globally once partial aggregates are merged at the broker, and could starve entire grouping sets
    /// (silently wrong results). Keep all groups (bounded by numGroupsLimit) and defer ORDER BY + LIMIT to the
    /// broker. Per-set bucketed trim still happens at the segment level.
    if (queryContext.isGroupingSets()) {
      int resultSize = queryContext.getNumGroupsLimit();
      int initialCapacity = getIndexedTableInitialCapacity(resultSize, numGroups, minInitialIndexedTableCapacity);
      return getTrimDisabledIndexedTable(dataSchema, false, queryContext, resultSize, initialCapacity, numThreads,
          executorService);
    }

    // Disable trim when min trim size is non-positive
    int trimSize = minTrimSize > 0 ? getTableCapacity(limit, minTrimSize) : Integer.MAX_VALUE;

    // When there is no ORDER BY, trim is not required because the indexed table stops accepting new groups once the
    // result size is reached
    if (!hasOrderBy) {
      int resultSize;
      if (hasHaving) {
        // Keep more groups when there is HAVING clause
        resultSize = trimSize;
      } else {
        // TODO: Keeping only 'LIMIT' groups can cause inaccurate result because the groups are randomly selected
        //       without ordering. Consider ordering on group-by columns if no ordering is specified.
        resultSize = limit;
      }
      int initialCapacity = getIndexedTableInitialCapacity(resultSize, numGroups, minInitialIndexedTableCapacity);
      return getTrimDisabledIndexedTable(dataSchema, false, queryContext, resultSize, initialCapacity, numThreads,
          executorService);
    }

    int resultSize;
    if (queryContext.isServerReturnFinalResult() && !hasHaving) {
      // When server is asked to return final result and there is no HAVING clause, return only LIMIT groups
      resultSize = limit;
    } else {
      resultSize = trimSize;
    }
    int trimThreshold = getIndexedTableTrimThreshold(trimSize, queryContext.getGroupTrimThreshold());
    int initialCapacity = getIndexedTableInitialCapacity(trimThreshold, numGroups, minInitialIndexedTableCapacity);
    if (trimThreshold == Integer.MAX_VALUE) {
      return getTrimDisabledIndexedTable(dataSchema, false, queryContext, resultSize, initialCapacity, numThreads,
          executorService);
    } else {
      return getTrimEnabledIndexedTable(dataSchema, false, queryContext, resultSize, trimSize, trimThreshold,
          initialCapacity, numThreads, executorService);
    }
  }

  /// Creates an indexed table for the data table reducer given a sample data table.
  public static IndexedTable createIndexedTableForDataTableReducer(DataTable dataTable, QueryContext queryContext,
      DataTableReducerContext reducerContext, int numThreads, ExecutorService executorService) {
    DataSchema dataSchema = dataTable.getDataSchema();
    int numGroups = dataTable.getNumberOfRows();
    int limit = queryContext.getLimit();
    boolean hasOrderBy = queryContext.getOrderByExpressions() != null;
    boolean hasHaving = queryContext.getHavingFilter() != null;
    boolean hasFinalInput =
        queryContext.isServerReturnFinalResult() || queryContext.isServerReturnFinalResultKeyUnpartitioned();
    int minTrimSize = reducerContext.getMinGroupTrimSize();
    int minInitialIndexedTableCapacity = reducerContext.getMinInitialIndexedTableCapacity();

    // Disable trim when min trim size is non-positive
    int trimSize = minTrimSize > 0 ? getTableCapacity(limit, minTrimSize) : Integer.MAX_VALUE;

    // Keep more groups when there is HAVING clause
    // TODO: Resolve the HAVING clause within the IndexedTable before returning the result
    int resultSize = hasHaving ? trimSize : limit;

    /// Grouping-set queries must not incrementally trim while merging server responses (a row could be dropped
    /// before all its partial aggregates are merged) nor apply a per-server top-K. Force the trim-disabled path
    /// so all groups are merged first; finish() then keeps the correct global top-K (resultSize) and the broker
    /// applies the final ORDER BY + LIMIT over the fully-merged table.
    if (queryContext.isGroupingSets()) {
      int initialCapacity = getIndexedTableInitialCapacity(resultSize, numGroups, minInitialIndexedTableCapacity);
      return getTrimDisabledIndexedTable(dataSchema, hasFinalInput, queryContext, resultSize, initialCapacity,
          numThreads, executorService);
    }

    // When there is no ORDER BY, trim is not required because the indexed table stops accepting new groups once the
    // result size is reached
    if (!hasOrderBy) {
      int initialCapacity = getIndexedTableInitialCapacity(resultSize, numGroups, minInitialIndexedTableCapacity);
      return getTrimDisabledIndexedTable(dataSchema, hasFinalInput, queryContext, resultSize, initialCapacity,
          numThreads, executorService);
    }

    int trimThreshold = getIndexedTableTrimThreshold(trimSize, reducerContext.getGroupByTrimThreshold());
    int initialCapacity = getIndexedTableInitialCapacity(trimThreshold, numGroups, minInitialIndexedTableCapacity);
    if (trimThreshold == Integer.MAX_VALUE) {
      return getTrimDisabledIndexedTable(dataSchema, hasFinalInput, queryContext, resultSize, initialCapacity,
          numThreads, executorService);
    } else {
      return getTrimEnabledIndexedTable(dataSchema, hasFinalInput, queryContext, resultSize, trimSize, trimThreshold,
          initialCapacity, numThreads, executorService);
    }
  }

  /// Whether trim-disabled tables must be [DeterministicConcurrentIndexedTable] (sorted backing map for
  /// deterministic iteration order under the accurate-group-by-without-order-by mode).
  private static boolean useDeterministicIndexedTable(QueryContext queryContext) {
    return queryContext.isAccurateGroupByWithoutOrderBy() && queryContext.getOrderByExpressions() == null
        && queryContext.getHavingFilter() == null;
  }

  private static IndexedTable getTrimDisabledIndexedTable(DataSchema dataSchema, boolean hasFinalInput,
      QueryContext queryContext, int resultSize, int initialCapacity, int numThreads, ExecutorService executorService) {
    if (useDeterministicIndexedTable(queryContext)) {
      return new DeterministicConcurrentIndexedTable(dataSchema, hasFinalInput, queryContext, resultSize,
          Integer.MAX_VALUE, Integer.MAX_VALUE, initialCapacity, executorService);
    }
    if (numThreads == 1) {
      return new SimpleIndexedTable(dataSchema, hasFinalInput, queryContext, resultSize, Integer.MAX_VALUE,
          Integer.MAX_VALUE, initialCapacity, executorService);
    } else {
      return new UnboundedConcurrentIndexedTable(dataSchema, hasFinalInput, queryContext, resultSize, initialCapacity,
          executorService);
    }
  }

  private static IndexedTable getTrimEnabledIndexedTable(DataSchema dataSchema, boolean hasFinalInput,
      QueryContext queryContext, int resultSize, int trimSize, int trimThreshold, int initialCapacity, int numThreads,
      ExecutorService executorService) {
    assert trimThreshold != Integer.MAX_VALUE;
    if (numThreads == 1) {
      return new SimpleIndexedTable(dataSchema, hasFinalInput, queryContext, resultSize, trimSize, trimThreshold,
          initialCapacity, executorService);
    } else {
      return new ConcurrentIndexedTable(dataSchema, hasFinalInput, queryContext, resultSize, trimSize, trimThreshold,
          initialCapacity, executorService);
    }
  }

  public static SortedRecords getAndPopulateSortedRecords(GroupByResultsBlock block) {
    List<IntermediateRecord> intermediateRecords = block.getIntermediateRecords();
    Record[] sortedRecords = new Record[intermediateRecords.size()];
    int idx = 0;
    for (IntermediateRecord intermediateRecord : intermediateRecords) {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(idx, "GroupByUtils#getAndPopulateSortedRecords");
      sortedRecords[idx++] = intermediateRecord._record;
    }
    return new SortedRecords(sortedRecords, idx);
  }

  public static SortedRecordsMerger getSortedReduceMerger(QueryContext queryContext,
      int resultSize, Comparator<Record> comparator) {
    return new SortedRecordsMerger(queryContext, resultSize, comparator);
  }
}
