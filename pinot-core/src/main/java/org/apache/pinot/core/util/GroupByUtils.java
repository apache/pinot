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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
    // safely be the base objects themselves (all other readers of a base intermediate treat it as read-only).
    boolean[] isFullUnionSet = new boolean[numSets];
    for (int s = 0; s < numSets; s++) {
      boolean fullUnion = true;
      for (int col = 0; col < numUnionColumns; col++) {
        fullUnion &= setContains[s][col];
      }
      isFullUnionSet[s] = fullUnion;
    }
    int numTaskSlots = Math.max(1, Math.min(numTasks, numSets));
    List<Map<Key, Record>> taskMaps;
    if (numTaskSlots == 1) {
      taskMaps = List.of(deriveSets(baseEntries, setContains, isFullUnionSet, 0, 1, numUnionColumns,
          numAggregationFunctions, aggregationFunctions));
    } else {
      List<Future<Map<Key, Record>>> futures = new ArrayList<>(numTaskSlots);
      for (int t = 0; t < numTaskSlots; t++) {
        int taskIndex = t;
        futures.add(executorService.submit(() -> deriveSets(baseEntries, setContains, isFullUnionSet, taskIndex,
            numTaskSlots, numUnionColumns, numAggregationFunctions, aggregationFunctions)));
      }
      taskMaps = new ArrayList<>(numTaskSlots);
      try {
        // Bound each wait by the query deadline (when one is set) so a stuck task cannot hold the combine past
        // the timeout.
        long endTimeMs = queryContext.getEndTimeMs();
        for (Future<Map<Key, Record>> future : futures) {
          if (endTimeMs > 0) {
            taskMaps.add(future.get(Math.max(endTimeMs - System.currentTimeMillis(), 0), TimeUnit.MILLISECONDS));
          } else {
            taskMaps.add(future.get());
          }
        }
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

    // Union the disjoint task maps into the result table. Since task key spaces are disjoint, the union never
    // merges records. In the common case, adopt the largest task map as the table's backing map and putAll the
    // rest into it, so the largest map's keys are never re-hashed. The deterministic accurate-group-by mode
    // needs a sorted (skip-list) backing map for deterministic iteration order, so it keeps the upsert-based
    // union into the table produced by getTrimDisabledIndexedTable (single-threaded, numThreads = 1).
    IndexedTable derivedTable;
    if (useDeterministicIndexedTable(queryContext)) {
      derivedTable = getTrimDisabledIndexedTable(groupingSetsSchema, false, queryContext, Integer.MAX_VALUE,
          initialCapacity, 1, executorService);
      for (Map<Key, Record> taskMap : taskMaps) {
        for (Map.Entry<Key, Record> entry : taskMap.entrySet()) {
          derivedTable.upsert(entry.getKey(), entry.getValue());
        }
      }
    } else {
      Map<Key, Record> mergedMap = taskMaps.get(0);
      for (Map<Key, Record> taskMap : taskMaps) {
        if (taskMap.size() > mergedMap.size()) {
          mergedMap = taskMap;
        }
      }
      for (Map<Key, Record> taskMap : taskMaps) {
        if (taskMap != mergedMap) {
          mergedMap.putAll(taskMap);
        }
      }
      derivedTable = new SimpleIndexedTable(groupingSetsSchema, false, queryContext, Integer.MAX_VALUE,
          Integer.MAX_VALUE, Integer.MAX_VALUE, mergedMap, executorService);
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

  /// One derive task: projects every base group into the grouping sets owned by `taskIndex` (set `s` is owned
  /// when `s % numTaskSlots == taskIndex`) and aggregates the derived groups into a task-local map. Each task
  /// runs single-threaded over its own map, so a plain HashMap suffices; task key spaces are disjoint because
  /// every derived key ends with its set ordinal.
  ///
  /// The first record stored for a derived key holds cloned intermediates (it becomes the group's accumulator,
  /// mutated by later merges); on merge, OBJECT intermediates from the shared base group are cloned again so the
  /// merge can never mutate a base accumulator that other tasks are still reading. Scalar intermediates are
  /// immutable and pass through uncloned.
  private static Map<Key, Record> deriveSets(List<Map.Entry<Key, Record>> baseEntries, boolean[][] setContains,
      boolean[] isFullUnionSet, int taskIndex, int numTaskSlots, int numUnionColumns, int numAggregationFunctions,
      AggregationFunction[] aggregationFunctions) {
    int numSets = setContains.length;
    // A full-union owned set produces exactly one derived group per base entry, so presize for it (it never
    // rehashes); tasks owning only coarser sets start small and grow, since their distinct-group counts are
    // usually far below the base size and a full-size bucket array per task would be wasted memory.
    boolean ownsFullUnionSet = false;
    for (int s = taskIndex; s < numSets; s += numTaskSlots) {
      ownsFullUnionSet |= isFullUnionSet[s];
    }
    Map<Key, Record> taskMap =
        ownsFullUnionSet ? new HashMap<>(HashUtil.getHashMapCapacity(baseEntries.size())) : new HashMap<>();
    // Flyweight probe: reuse one key buffer for lookups (HashMap.get does not retain its argument) and copy it
    // into a fresh array only when inserting a new derived group. Most projections into coarse sets hit an
    // existing group, so this avoids a key-array + Key allocation per projection on the merge path.
    Object[] probeValues = new Object[numUnionColumns + 1];
    Key probeKey = new Key(probeValues);
    int numProjections = 0;
    for (Map.Entry<Key, Record> baseEntry : baseEntries) {
      Object[] baseKeys = baseEntry.getKey().getValues();
      Object[] baseValues = baseEntry.getValue().getValues();
      for (int s = taskIndex; s < numSets; s += numTaskSlots) {
        // Keep the derive responsive to query timeout / cancellation and visible to resource accounting, like
        // the segment-merge loop in GroupByCombineOperator.
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(numProjections++, "GroupByUtils#deriveSets");
        boolean[] contains = setContains[s];
        for (int col = 0; col < numUnionColumns; col++) {
          probeValues[col] = contains[col] ? baseKeys[col] : null;
        }
        probeValues[numUnionColumns] = s;
        // Full-union sets never see a duplicate key, so skip the probe and insert blindly. Their records can
        // also share the base intermediates without cloning: no merge will ever mutate them (this set never
        // merges, and every merge into OTHER sets clones its second argument before use).
        boolean fullUnion = isFullUnionSet[s];
        Record existing = fullUnion ? null : taskMap.get(probeKey);
        if (existing == null) {
          Object[] keyValues = probeValues.clone();
          Object[] values = new Object[numUnionColumns + 1 + numAggregationFunctions];
          System.arraycopy(keyValues, 0, values, 0, numUnionColumns + 1);
          for (int i = 0; i < numAggregationFunctions; i++) {
            Object intermediate = baseValues[numUnionColumns + i];
            values[numUnionColumns + 1 + i] =
                fullUnion ? intermediate : cloneIntermediate(aggregationFunctions[i], intermediate);
          }
          taskMap.put(new Key(keyValues), new Record(values));
        } else {
          Object[] values = existing.getValues();
          for (int i = 0; i < numAggregationFunctions; i++) {
            int valueIndex = numUnionColumns + 1 + i;
            // The first argument is this set's owned accumulator (mutating it is safe). The second argument is
            // cloned because AggregationFunction#merge may RETURN its second argument as the new accumulator
            // (e.g. HyperLogLog merge with mismatched sizes), which would then be mutated by later merges while
            // other tasks are still reading the shared base intermediate.
            values[valueIndex] = AggregationFunctionUtils.merge(aggregationFunctions[i], values[valueIndex],
                cloneIntermediate(aggregationFunctions[i], baseValues[numUnionColumns + i]));
          }
        }
      }
    }
    return taskMap;
  }

  private static void cancelAll(List<Future<Map<Key, Record>>> futures) {
    for (Future<Map<Key, Record>> future : futures) {
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

  /// Returns a defensive copy of an aggregation intermediate result so that a subsequent
  /// [AggregationFunction#merge] into a derived grouping-set group cannot mutate a base group's shared
  /// accumulator. Scalar (non-OBJECT) intermediates are immutable boxed values and are returned as-is; `null`
  /// (nothing aggregated) is the merge identity and needs no copy; OBJECT accumulators are cloned via the
  /// function's own serialize/deserialize round-trip.
  private static Object cloneIntermediate(AggregationFunction aggregationFunction, Object intermediate) {
    if (intermediate == null || aggregationFunction.getIntermediateResultColumnType() != ColumnDataType.OBJECT) {
      return intermediate;
    }
    SerializedIntermediateResult serialized = aggregationFunction.serializeIntermediateResult(intermediate);
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
  static int getIndexedTableInitialCapacity(int maxRowsToKeep, int minNumGroups, int minCapacity) {
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
