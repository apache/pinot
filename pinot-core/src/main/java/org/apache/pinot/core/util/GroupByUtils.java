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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
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

  /// Builds the segment-level [GroupByResultsBlock] for a grouping-set query from BASE groups, i.e. groups
  /// aggregated once over the union of all grouping-set columns (a plain GROUP BY), rather than by expanding
  /// every input row into one group per grouping set. Each base group is then projected into each grouping set:
  /// its rolled-up (non-participating) columns are set to `null`, the `$groupingId` discriminator is appended,
  /// and the base group's aggregation intermediates are merged into the derived group. This moves the per-set
  /// fan-out from O(rows) to O(base groups), reusing the fast plain-GROUP-BY scan path.
  ///
  /// Because a base group's intermediate result flows into every grouping set, and [AggregationFunction#merge]
  /// mutates its first argument, each base intermediate is cloned per set before it becomes a merge target (see
  /// [#cloneIntermediate]). This keeps the derivation exact for object-backed accumulators (AVG, DISTINCTCOUNT,
  /// percentiles, ...) as well as scalar ones.
  ///
  /// @param discriminatorColumnIndex index of the synthetic $groupingId column, i.e. the number of union
  ///                                 group-by columns
  public static GroupByResultsBlock buildGroupingSetsResultsBlockFromBaseGroups(QueryContext queryContext,
      DataSchema dataSchema, AggregationGroupByResult baseResult, int discriminatorColumnIndex,
      boolean numGroupsLimitReached, boolean numGroupsWarningLimitReached) {
    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
    assert aggregationFunctions != null;
    int numAggregationFunctions = aggregationFunctions.length;
    List<int[]> groupingSets = queryContext.getGroupingSets();
    int numSets = groupingSets.size();
    int numUnionColumns = discriminatorColumnIndex;
    // Per grouping set: membership mask over the union columns (true = participates, false = rolled up to NULL).
    boolean[][] setContains = new boolean[numSets][numUnionColumns];
    for (int s = 0; s < numSets; s++) {
      for (int columnIndex : groupingSets.get(s)) {
        setContains[s][columnIndex] = true;
      }
    }

    // Derived group table keyed on (projected union values..., $groupingId). Values layout mirrors the record
    // schema: key columns first, then the aggregation intermediates. Bound the number of DERIVED groups by the
    // query's numGroupsLimit, mirroring the generator's per-segment cap: the fan-out over grouping sets can
    // multiply base groups by up to numSets, so without this bound the derive map (built before any trim) could
    // exhaust heap. Once the limit is reached, no new derived group is created, but existing ones keep merging
    // (so results stay a stable superset, exactly like the generator dropping brand-new keys past the limit).
    int numGroupsLimit = queryContext.getNumGroupsLimit();
    boolean derivedLimitReached = false;
    Map<Key, Record> derived = new HashMap<>();
    try {
      Iterator<GroupKeyGenerator.GroupKey> baseGroups = baseResult.getGroupKeyIterator();
      while (baseGroups.hasNext()) {
        GroupKeyGenerator.GroupKey baseGroup = baseGroups.next();
        Object[] baseKeys = baseGroup._keys;
        int baseGroupId = baseGroup._groupId;
        Object[] baseIntermediates = new Object[numAggregationFunctions];
        for (int i = 0; i < numAggregationFunctions; i++) {
          baseIntermediates[i] = baseResult.getResultForGroupId(i, baseGroupId);
        }
        for (int s = 0; s < numSets; s++) {
          boolean[] contains = setContains[s];
          Object[] keyValues = new Object[numUnionColumns + 1];
          for (int col = 0; col < numUnionColumns; col++) {
            keyValues[col] = contains[col] ? baseKeys[col] : null;
          }
          keyValues[numUnionColumns] = s;
          Key key = new Key(keyValues);
          Record existing = derived.get(key);
          if (existing == null) {
            if (derived.size() >= numGroupsLimit) {
              // At the group limit: skip brand-new derived keys (existing groups above still accumulate).
              derivedLimitReached = true;
              continue;
            }
            Object[] values = new Object[numUnionColumns + 1 + numAggregationFunctions];
            System.arraycopy(keyValues, 0, values, 0, numUnionColumns + 1);
            for (int i = 0; i < numAggregationFunctions; i++) {
              // Clone so a later merge into this derived group cannot mutate the base group's shared intermediate.
              values[numUnionColumns + 1 + i] = cloneIntermediate(aggregationFunctions[i], baseIntermediates[i]);
            }
            derived.put(key, new Record(values));
          } else {
            Object[] values = existing.getValues();
            for (int i = 0; i < numAggregationFunctions; i++) {
              int index = numUnionColumns + 1 + i;
              // Clone the base intermediate here too: merge() may RETURN its second argument unchanged (when the
              // derived accumulator is null or an empty object), which would otherwise alias the base group's
              // live shared accumulator into this derived record and corrupt it when a later base group mutates
              // it in place. See cloneIntermediate.
              values[index] = AggregationFunctionUtils.merge(aggregationFunctions[i], values[index],
                  cloneIntermediate(aggregationFunctions[i], baseIntermediates[i]));
            }
          }
        }
      }
    } finally {
      baseResult.closeGroupKeyGenerator();
    }
    // The derived fan-out can hit the group limit even when the base grouping did not.
    numGroupsLimitReached |= derivedLimitReached;
    numGroupsWarningLimitReached |= derived.size() >= queryContext.getNumGroupsWarningLimit();

    List<IntermediateRecord> intermediateRecords = new ArrayList<>(derived.size());
    for (Map.Entry<Key, Record> entry : derived.entrySet()) {
      intermediateRecords.add(IntermediateRecord.withoutOrderByValues(entry.getKey(), entry.getValue()));
    }

    /// Apply the same per-set bucketed segment trim as the legacy expansion path so the base-aggregation path
    /// keeps the same memory/network guardrail: when the per-set budget is enabled and the derived group count
    /// exceeds it, keep each grouping set's own top candidates (a global top-K cannot starve low-magnitude sets
    /// such as the grand total). The broker still applies the final ORDER BY + LIMIT across all sets.
    int perSetTrimSize = queryContext.getGroupingSetSegmentTrimSize();
    boolean groupsTrimmed = false;
    if (perSetTrimSize > 0 && intermediateRecords.size() > (long) perSetTrimSize * numSets) {
      TableResizer tableResizer = new TableResizer(dataSchema, queryContext);
      intermediateRecords = tableResizer.trimInSegmentRecordsByGroupingSet(intermediateRecords, perSetTrimSize,
          discriminatorColumnIndex);
      groupsTrimmed = true;
      ServerMetrics.get().addMeteredGlobalValue(ServerMeter.AGGREGATE_TIMES_GROUPS_TRIMMED, 1);
    }

    GroupByResultsBlock resultsBlock = new GroupByResultsBlock(dataSchema, intermediateRecords, queryContext);
    resultsBlock.setGroupsTrimmed(groupsTrimmed);
    resultsBlock.setNumGroupsLimitReached(numGroupsLimitReached);
    resultsBlock.setNumGroupsWarningLimitReached(numGroupsWarningLimitReached);
    return resultsBlock;
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

  private static IndexedTable getTrimDisabledIndexedTable(DataSchema dataSchema, boolean hasFinalInput,
      QueryContext queryContext, int resultSize, int initialCapacity, int numThreads, ExecutorService executorService) {
    if (queryContext.isAccurateGroupByWithoutOrderBy() && queryContext.getOrderByExpressions() == null
        && queryContext.getHavingFilter() == null) {
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
