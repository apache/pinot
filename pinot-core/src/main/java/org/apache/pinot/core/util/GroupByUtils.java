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
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.data.table.ConcurrentIndexedTable;
import org.apache.pinot.core.data.table.DeterministicConcurrentIndexedTable;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.SimpleIndexedTable;
import org.apache.pinot.core.data.table.SortedRecords;
import org.apache.pinot.core.data.table.SortedRecordsMerger;
import org.apache.pinot.core.data.table.UnboundedConcurrentIndexedTable;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.reduce.DataTableReducerContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.query.QueryThreadContext;


public final class GroupByUtils {
  private GroupByUtils() {
  }

  public static final int DEFAULT_MIN_NUM_GROUPS = 5000;
  public static final int MAX_TRIM_THRESHOLD = 1_000_000_000;

  /**
   * Returns the capacity of the table required by the given query. NOTE: It returns {@code max(limit * 5, 5000)} to
   * ensure the result accuracy.
   */
  public static int getTableCapacity(int limit) {
    return getTableCapacity(limit, DEFAULT_MIN_NUM_GROUPS);
  }

  /**
   * Returns the capacity of the table required by the given query. NOTE: It returns
   * {@code max(limit * 5, minNumGroups)} where minNumGroups is configurable to tune the table size and result
   * accuracy.
   */
  public static int getTableCapacity(int limit, int minNumGroups) {
    long capacityByLimit = limit * 5L;
    return capacityByLimit > Integer.MAX_VALUE ? Integer.MAX_VALUE : Math.max((int) capacityByLimit, minNumGroups);
  }

  /**
   * Returns the actual trim threshold used for the indexed table. Trim threshold should be at least (2 * trimSize) to
   * avoid excessive trimming. When trim threshold is non-positive or higher than 10^9, trim is considered disabled,
   * where {@code Integer.MAX_VALUE} is returned.
   */
  @VisibleForTesting
  static int getIndexedTableTrimThreshold(int trimSize, int trimThreshold) {
    if (trimThreshold <= 0 || trimThreshold > MAX_TRIM_THRESHOLD || trimSize > MAX_TRIM_THRESHOLD / 2) {
      return Integer.MAX_VALUE;
    }
    return Math.max(trimThreshold, 2 * trimSize);
  }

  /**
   * Returns the initial capacity of the indexed table required by the given query.
   */
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

  /**
   * Creates an indexed table for the combine operator given a sample results block.
   */
  public static IndexedTable createIndexedTableForCombineOperator(GroupByResultsBlock resultsBlock,
      QueryContext queryContext, int numThreads, ExecutorService executorService) {
    return createIndexedTableForCombineOperator(resultsBlock, queryContext, numThreads, executorService,
        resultsBlock.getNumGroups());
  }

  /**
   * Creates an indexed table for the combine operator given a sample results block, using {@code numGroupsHint} as the
   * initial-capacity hint instead of {@code resultsBlock.getNumGroups()}. Use this overload when the table will hold
   * only a subset of the segment groups (e.g., one partition out of P in a partitioned combine).
   */
  public static IndexedTable createIndexedTableForCombineOperator(GroupByResultsBlock resultsBlock,
      QueryContext queryContext, int numThreads, ExecutorService executorService, int numGroupsHint) {
    return createIndexedTableForCombineOperator(resultsBlock, queryContext, numThreads, executorService, numGroupsHint,
        queryContext.getGroupTrimThreshold());
  }

  /**
   * Returns the effective trim threshold for a combine operator, accounting for disabled-trim sentinels.
   * Returns {@link Integer#MAX_VALUE} when:
   * <ul>
   *   <li>there is no ORDER BY (trim is only meaningful with top-K ordering), or</li>
   *   <li>{@code groupTrimThreshold} is non-positive or above {@link #MAX_TRIM_THRESHOLD}, or</li>
   *   <li>the computed trim size would cause integer overflow.</li>
   * </ul>
   * This value can be used as {@code _globalGroupTrimThreshold} in a partitioned combine to decide when to trigger a
   * coordinated trim across all partition-local tables.
   */
  public static int getEffectiveCombineTrimThreshold(QueryContext queryContext) {
    if (queryContext.getOrderByExpressions() == null) {
      return Integer.MAX_VALUE;
    }
    int minTrimSize = queryContext.getMinServerGroupTrimSize();
    int trimSize = minTrimSize > 0 ? getTableCapacity(queryContext.getLimit(), minTrimSize) : Integer.MAX_VALUE;
    return getIndexedTableTrimThreshold(trimSize, queryContext.getGroupTrimThreshold());
  }

  /**
   * Creates a partition-local indexed table for the partitioned group-by combine operator.
   *
   * <p>The returned table has auto-trim <em>disabled</em> ({@code trimThreshold = MAX_VALUE}) so the combine operator
   * can coordinate trim across all partitions at the global threshold rather than per-partition. When an explicit
   * {@link IndexedTable#trim()} call is made, the table trims to its {@code trimSize} — computed the same way as a
   * regular combine table — rather than to {@link Integer#MAX_VALUE}.
   *
   * @param resultsBlock  sample results block used to derive the data schema
   * @param queryContext  query context
   * @param numGroupsHint expected number of groups for this partition (used for initial-capacity hint)
   * @param executorService executor for multi-threaded final reduce
   */
  public static IndexedTable createPartitionTableForCombineOperator(GroupByResultsBlock resultsBlock,
      QueryContext queryContext, int numGroupsHint, ExecutorService executorService) {
    DataSchema dataSchema = resultsBlock.getDataSchema();
    int limit = queryContext.getLimit();
    boolean hasOrderBy = queryContext.getOrderByExpressions() != null;
    boolean hasHaving = queryContext.getHavingFilter() != null;
    int minTrimSize = queryContext.getMinServerGroupTrimSize();
    int trimSize = minTrimSize > 0 ? getTableCapacity(limit, minTrimSize) : Integer.MAX_VALUE;
    int minInitialCapacity = queryContext.getMinInitialIndexedTableCapacity();

    int resultSize;
    int tableTrimSize;
    if (!hasOrderBy) {
      resultSize = hasHaving ? trimSize : limit;
      tableTrimSize = Integer.MAX_VALUE;  // trim() is a no-op without ORDER BY
    } else {
      resultSize = (queryContext.isServerReturnFinalResult() && !hasHaving) ? limit : trimSize;
      tableTrimSize = trimSize;  // used when trim() is explicitly invoked
    }
    // Initial capacity based on expected groups per partition, uncapped by tableTrimSize
    int initialCapacity = getIndexedTableInitialCapacity(Integer.MAX_VALUE, numGroupsHint, minInitialCapacity);
    // trimThreshold=MAX_VALUE disables auto-trim; trim() calls still work via tableTrimSize
    return new SimpleIndexedTable(dataSchema, false, queryContext, resultSize, tableTrimSize, Integer.MAX_VALUE,
        initialCapacity, executorService);
  }

  /**
   * Creates an indexed table for the combine operator given a sample results block, using {@code numGroupsHint} as the
   * initial-capacity hint and {@code groupTrimThresholdOverride} in place of
   * {@code queryContext.getGroupTrimThreshold()}. Use this overload when the caller needs to supply an explicit trim
   * threshold that differs from the one in {@code queryContext} — for example, passing
   * {@link Integer#MAX_VALUE} to disable auto-trim on a partition-local table that will be trimmed externally.
   */
  public static IndexedTable createIndexedTableForCombineOperator(GroupByResultsBlock resultsBlock,
      QueryContext queryContext, int numThreads, ExecutorService executorService, int numGroupsHint,
      int groupTrimThresholdOverride) {
    DataSchema dataSchema = resultsBlock.getDataSchema();
    int numGroups = numGroupsHint;
    int limit = queryContext.getLimit();
    boolean hasOrderBy = queryContext.getOrderByExpressions() != null;
    boolean hasHaving = queryContext.getHavingFilter() != null;
    int minTrimSize =
        queryContext.getMinServerGroupTrimSize(); // it's minBrokerGroupTrimSize in broker
    int minInitialIndexedTableCapacity = queryContext.getMinInitialIndexedTableCapacity();

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
    int trimThreshold = getIndexedTableTrimThreshold(trimSize, groupTrimThresholdOverride);
    int initialCapacity = getIndexedTableInitialCapacity(trimThreshold, numGroups, minInitialIndexedTableCapacity);
    if (trimThreshold == Integer.MAX_VALUE) {
      return getTrimDisabledIndexedTable(dataSchema, false, queryContext, resultSize, initialCapacity, numThreads,
          executorService);
    } else {
      return getTrimEnabledIndexedTable(dataSchema, false, queryContext, resultSize, trimSize, trimThreshold,
          initialCapacity, numThreads, executorService);
    }
  }

  /**
   * Creates an indexed table for the data table reducer given a sample data table.
   */
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
