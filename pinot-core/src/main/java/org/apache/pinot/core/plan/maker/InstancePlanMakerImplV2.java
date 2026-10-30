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
package org.apache.pinot.core.plan.maker;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.plan.AcquireReleaseColumnsSegmentPlanNode;
import org.apache.pinot.core.plan.AggregationPlanNode;
import org.apache.pinot.core.plan.CombinePlanNode;
import org.apache.pinot.core.plan.DistinctPlanNode;
import org.apache.pinot.core.plan.GlobalPlanImplV0;
import org.apache.pinot.core.plan.GroupByPlanNode;
import org.apache.pinot.core.plan.InstanceResponsePlanNode;
import org.apache.pinot.core.plan.Plan;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.core.plan.SelectionPlanNode;
import org.apache.pinot.core.plan.StreamingInstanceResponsePlanNode;
import org.apache.pinot.core.plan.StreamingSelectionPlanNode;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.executor.ResultsBlockStreamer;
import org.apache.pinot.core.query.prefetch.FetchPlanner;
import org.apache.pinot.core.query.prefetch.FetchPlannerRegistry;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.CommonConstants.Server.AndRestrictionPushdownMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// The `InstancePlanMakerImplV2` class is the default implementation of [PlanMaker].
public class InstancePlanMakerImplV2 implements PlanMaker {
  public static final int DEFAULT_NUM_THREADS_EXTRACT_FINAL_RESULT = 1;
  public static final int DEFAULT_CHUNK_SIZE_EXTRACT_FINAL_RESULT = 10_000;

  private static final Logger LOGGER = LoggerFactory.getLogger(InstancePlanMakerImplV2.class);
  // Cap planning work independently of the query's configurable group limit.
  private static final int MAX_GROUPING_SETS_DICTIONARY_SCAN_ENTRIES = 1 << 20;

  private final FetchPlanner _fetchPlanner = FetchPlannerRegistry.getPlanner();
  private int _maxExecutionThreads = Server.DEFAULT_QUERY_EXECUTOR_MAX_EXECUTION_THREADS;
  private int _defaultExecutionThreads = Server.DEFAULT_QUERY_EXECUTOR_DEFAULT_EXECUTION_THREADS;
  private int _maxInitialResultHolderCapacity = Server.DEFAULT_QUERY_EXECUTOR_MAX_INITIAL_RESULT_HOLDER_CAPACITY;
  private int _minInitialIndexedTableCapacity = Server.DEFAULT_QUERY_EXECUTOR_MIN_INITIAL_INDEXED_TABLE_CAPACITY;
  // Limit on number of groups stored for each segment, beyond which no new group will be created
  private int _numGroupsLimit = Server.DEFAULT_QUERY_EXECUTOR_NUM_GROUPS_LIMIT;
  // Warning limit on number of groups stored for each segment
  private int _numGroupsWarningLimit = Server.DEFAULT_QUERY_EXECUTOR_NUM_GROUPS_WARN_LIMIT;
  // Used for SQL GROUP BY (server combine)
  private int _minSegmentGroupTrimSize = Server.DEFAULT_QUERY_EXECUTOR_MIN_SEGMENT_GROUP_TRIM_SIZE;
  private int _minServerGroupTrimSize = Server.DEFAULT_QUERY_EXECUTOR_MIN_SERVER_GROUP_TRIM_SIZE;
  private int _groupByTrimThreshold = Server.DEFAULT_QUERY_EXECUTOR_GROUPBY_TRIM_THRESHOLD;
  private AndRestrictionPushdownMode _andRestrictionPushdownMode =
      Server.DEFAULT_QUERY_EXECUTOR_AND_RESTRICTION_PUSHDOWN_MODE;

  @Override
  public void init(PinotConfiguration queryExecutorConfig) {
    _maxExecutionThreads = queryExecutorConfig.getProperty(Server.MAX_EXECUTION_THREADS,
        Server.DEFAULT_QUERY_EXECUTOR_MAX_EXECUTION_THREADS);
    _defaultExecutionThreads = queryExecutorConfig.getProperty(Server.DEFAULT_EXECUTION_THREADS,
        Server.DEFAULT_QUERY_EXECUTOR_DEFAULT_EXECUTION_THREADS);
    validateExecutionThreadConfig();
    _maxInitialResultHolderCapacity = queryExecutorConfig.getProperty(Server.MAX_INITIAL_RESULT_HOLDER_CAPACITY,
        Server.DEFAULT_QUERY_EXECUTOR_MAX_INITIAL_RESULT_HOLDER_CAPACITY);
    _minInitialIndexedTableCapacity = queryExecutorConfig.getProperty(Server.MIN_INITIAL_INDEXED_TABLE_CAPACITY,
        Server.DEFAULT_QUERY_EXECUTOR_MIN_INITIAL_INDEXED_TABLE_CAPACITY);
    _numGroupsLimit =
        queryExecutorConfig.getProperty(Server.NUM_GROUPS_LIMIT, Server.DEFAULT_QUERY_EXECUTOR_NUM_GROUPS_LIMIT);
    Preconditions.checkState(_maxInitialResultHolderCapacity <= _numGroupsLimit,
        "Invalid configuration: maxInitialResultHolderCapacity: %d must be smaller or equal to numGroupsLimit: %d",
        _maxInitialResultHolderCapacity, _numGroupsLimit);
    Preconditions.checkState(_minInitialIndexedTableCapacity <= _numGroupsLimit,
        "Invalid configuration: minInitialIndexedTableCapacity: %d must be smaller or equal to numGroupsLimit: %d",
        _minInitialIndexedTableCapacity, _numGroupsLimit);
    _numGroupsWarningLimit = queryExecutorConfig.getProperty(Server.NUM_GROUPS_WARN_LIMIT,
        Server.DEFAULT_QUERY_EXECUTOR_NUM_GROUPS_WARN_LIMIT);
    _minSegmentGroupTrimSize = queryExecutorConfig.getProperty(Server.MIN_SEGMENT_GROUP_TRIM_SIZE,
        Server.DEFAULT_QUERY_EXECUTOR_MIN_SEGMENT_GROUP_TRIM_SIZE);
    _minServerGroupTrimSize = queryExecutorConfig.getProperty(Server.MIN_SERVER_GROUP_TRIM_SIZE,
        Server.DEFAULT_QUERY_EXECUTOR_MIN_SERVER_GROUP_TRIM_SIZE);
    _groupByTrimThreshold = queryExecutorConfig.getProperty(Server.GROUPBY_TRIM_THRESHOLD,
        Server.DEFAULT_QUERY_EXECUTOR_GROUPBY_TRIM_THRESHOLD);
    _andRestrictionPushdownMode = QueryOptionsUtils.parseAndRestrictionPushdownMode(
        Server.AND_RESTRICTION_PUSHDOWN_MODE, queryExecutorConfig.getProperty(Server.AND_RESTRICTION_PUSHDOWN_MODE,
            Server.DEFAULT_QUERY_EXECUTOR_AND_RESTRICTION_PUSHDOWN_MODE.name()));
    Preconditions.checkState(_groupByTrimThreshold > 0,
        "Invalid configurable: groupByTrimThreshold: %d must be positive", _groupByTrimThreshold);
    LOGGER.info("Initialized plan maker with maxExecutionThreads: {}, defaultExecutionThreads: {}, "
            + "maxInitialResultHolderCapacity: {}, numGroupsLimit: {}, minSegmentGroupTrimSize: {}, "
            + "minServerGroupTrimSize: {}, groupByTrimThreshold: {}",
        _maxExecutionThreads, _defaultExecutionThreads, _maxInitialResultHolderCapacity, _numGroupsLimit,
        _minSegmentGroupTrimSize, _minServerGroupTrimSize, _groupByTrimThreshold);
  }

  @VisibleForTesting
  public void setMaxExecutionThreads(int maxExecutionThreads) {
    _maxExecutionThreads = maxExecutionThreads;
    validateExecutionThreadConfig();
  }

  @VisibleForTesting
  public void setDefaultExecutionThreads(int defaultExecutionThreads) {
    _defaultExecutionThreads = defaultExecutionThreads;
    validateExecutionThreadConfig();
  }

  private void validateExecutionThreadConfig() {
    if (_defaultExecutionThreads > 0 && _maxExecutionThreads > 0) {
      Preconditions.checkState(_defaultExecutionThreads <= _maxExecutionThreads,
          "Invalid configuration: defaultExecutionThreads: %d must be <= maxExecutionThreads: %d",
          _defaultExecutionThreads, _maxExecutionThreads);
    }
  }

  @VisibleForTesting
  public void setMaxInitialResultHolderCapacity(int maxInitialResultHolderCapacity) {
    _maxInitialResultHolderCapacity = maxInitialResultHolderCapacity;
  }

  @VisibleForTesting
  public void setMinInitialIndexedTableCapacity(int minInitialIndexedTableCapacity) {
    _minInitialIndexedTableCapacity = minInitialIndexedTableCapacity;
  }

  @VisibleForTesting
  public void setNumGroupsLimit(int numGroupsLimit) {
    _numGroupsLimit = numGroupsLimit;
  }

  @VisibleForTesting
  public void setMinSegmentGroupTrimSize(int minSegmentGroupTrimSize) {
    _minSegmentGroupTrimSize = minSegmentGroupTrimSize;
  }

  @VisibleForTesting
  public void setMinServerGroupTrimSize(int minServerGroupTrimSize) {
    _minServerGroupTrimSize = minServerGroupTrimSize;
  }

  @VisibleForTesting
  public void setGroupByTrimThreshold(int groupByTrimThreshold) {
    _groupByTrimThreshold = groupByTrimThreshold;
  }

  @Override
  public Plan makeInstancePlan(List<SegmentContext> segmentContexts, QueryContext queryContext,
      ExecutorService executorService) {
    rewriteQueryContextWithHints(queryContext, segmentContexts);
    applyQueryOptions(queryContext);
    boundGroupingSetsBaseAggregation(segmentContexts, queryContext);

    int numSegments = segmentContexts.size();
    List<PlanNode> planNodes = new ArrayList<>(numSegments);
    List<FetchContext> fetchContexts;
    if (queryContext.isEnablePrefetch()) {
      fetchContexts = new ArrayList<>(numSegments);
      for (SegmentContext segmentContext : segmentContexts) {
        FetchContext fetchContext =
            _fetchPlanner.planFetchForProcessing(segmentContext.getIndexSegment(), queryContext);
        fetchContexts.add(fetchContext);
        planNodes.add(
            new AcquireReleaseColumnsSegmentPlanNode(makeSegmentPlanNode(segmentContext, queryContext), segmentContext,
                fetchContext));
      }
    } else {
      fetchContexts = List.of();
      for (SegmentContext segmentContext : segmentContexts) {
        planNodes.add(makeSegmentPlanNode(segmentContext, queryContext));
      }
    }

    CombinePlanNode combinePlanNode = createCombinePlanNode(planNodes, queryContext, executorService, null);
    return new GlobalPlanImplV0(
        new InstanceResponsePlanNode(combinePlanNode, segmentContexts, fetchContexts, queryContext));
  }

  /// Resolves [AndRestrictionPushdownMode] for a query.
  ///
  /// The push-down materializes the filter result instead of streaming it, so under AUTO it is only applied to a
  /// query that is going to read every matching document anyway. That is exactly the aggregation and group-by
  /// queries:
  ///
  /// - a selection query without ORDER BY stops as soon as it has LIMIT rows;
  /// - a DISTINCT query without ORDER BY stops the same way, in `DistinctOperator#getNextBlock`;
  /// - whether a selection query *with* ORDER BY can stop early is decided per segment, because
  ///   [org.apache.pinot.core.plan.SelectionPlanNode] picks `SelectionPartiallyOrderedBy*` from the segment's sorted
  ///   column, so it cannot be ruled out here and is excluded conservatively;
  /// - LIMIT 0 reads nothing at all.
  ///
  /// Use ALWAYS to push down on a workload known not to terminate early.
  private static boolean isAndRestrictionPushdownEnabled(AndRestrictionPushdownMode mode, QueryContext queryContext) {
    switch (mode) {
      case ALWAYS:
        return true;
      case NEVER:
        return false;
      case AUTO:
        // NOTE: DISTINCT is modelled as an aggregation function, so it has to be excluded explicitly
        return queryContext.getLimit() > 0 && QueryContextUtils.isAggregationQuery(queryContext)
            && !QueryContextUtils.isDistinctQuery(queryContext);
      default:
        throw new IllegalStateException("Unsupported AND restriction push-down mode: " + mode);
    }
  }

  @VisibleForTesting
  void applyQueryOptions(QueryContext queryContext) {
    Map<String, String> queryOptions = queryContext.getQueryOptions();

    // Set skipUpsert
    queryContext.setSkipUpsert(QueryOptionsUtils.isSkipUpsert(queryOptions));

    // Set skipStarTree
    queryContext.setSkipStarTree(QueryOptionsUtils.isSkipStarTree(queryOptions));

    // Set accurateGroupByWithoutOrderBy
    queryContext.setAccurateGroupByWithoutOrderBy(
        QueryOptionsUtils.isAccurateGroupByWithoutOrderBy(queryOptions));

    // Set skipScanFilterReorder
    queryContext.setSkipScanFilterReorder(QueryOptionsUtils.isSkipScanFilterReorder(queryOptions));

    // Set andRestrictionPushdown, letting the query option override the server default
    AndRestrictionPushdownMode andRestrictionPushdownMode =
        QueryOptionsUtils.getAndRestrictionPushdownMode(queryOptions);
    queryContext.setAndRestrictionPushdownEnabled(isAndRestrictionPushdownEnabled(
        andRestrictionPushdownMode != null ? andRestrictionPushdownMode : _andRestrictionPushdownMode, queryContext));

    queryContext.setSkipIndexes(QueryOptionsUtils.getSkipIndexes(queryOptions));

    // Set maxExecutionThreads
    // Resolution order:
    //   1. Per-query override (SET maxExecutionThreads=N) — capped by server max
    //   2. Server-level default (default.execution.threads) — decoupled from max, but still capped by it
    //   3. Server-level max (max.execution.threads) — legacy fallback
    int maxExecutionThreads;
    Integer maxExecutionThreadsFromQuery = QueryOptionsUtils.getMaxExecutionThreads(queryOptions);
    if (maxExecutionThreadsFromQuery != null) {
      if (_maxExecutionThreads > 0) {
        maxExecutionThreads = Math.min(_maxExecutionThreads, maxExecutionThreadsFromQuery);
      } else {
        maxExecutionThreads = maxExecutionThreadsFromQuery;
      }
    } else if (_defaultExecutionThreads > 0) {
      maxExecutionThreads = _defaultExecutionThreads;
    } else {
      maxExecutionThreads = _maxExecutionThreads;
    }
    queryContext.setMaxExecutionThreads(maxExecutionThreads);

    // Set group-by query options
    if (QueryContextUtils.isAggregationQuery(queryContext) && queryContext.getGroupByExpressions() != null) {
      // Set maxInitialResultHolderCapacity
      Integer maxInitialResultHolderCapacity = QueryOptionsUtils.getMaxInitialResultHolderCapacity(queryOptions);
      if (maxInitialResultHolderCapacity != null) {
        queryContext.setMaxInitialResultHolderCapacity(maxInitialResultHolderCapacity);
      } else {
        queryContext.setMaxInitialResultHolderCapacity(_maxInitialResultHolderCapacity);
      }
      // Set initialResultTableCapacity
      Integer minInitialIndexedTableCapacity = QueryOptionsUtils.getMinInitialIndexedTableCapacity(queryOptions);
      if (minInitialIndexedTableCapacity != null) {
        queryContext.setMinInitialIndexedTableCapacity(minInitialIndexedTableCapacity);
      } else {
        queryContext.setMinInitialIndexedTableCapacity(_minInitialIndexedTableCapacity);
      }
      // Set numGroupsLimit
      Integer numGroupsLimit = QueryOptionsUtils.getNumGroupsLimit(queryOptions);
      if (numGroupsLimit != null) {
        queryContext.setNumGroupsLimit(numGroupsLimit);
      } else {
        queryContext.setNumGroupsLimit(_numGroupsLimit);
      }
      // Set numGroupsWarningThreshold
      queryContext.setNumGroupsWarningLimit(_numGroupsWarningLimit);
      // Set minSegmentGroupTrimSize
      Integer minSegmentGroupTrimSizeFromQuery = QueryOptionsUtils.getMinSegmentGroupTrimSize(queryOptions);
      if (minSegmentGroupTrimSizeFromQuery != null) {
        queryContext.setMinSegmentGroupTrimSize(minSegmentGroupTrimSizeFromQuery);
      } else {
        queryContext.setMinSegmentGroupTrimSize(_minSegmentGroupTrimSize);
      }
      // Set minServerGroupTrimSize
      Integer minServerGroupTrimSizeFromQuery = QueryOptionsUtils.getMinServerGroupTrimSize(queryOptions);
      int minServerGroupTrimSize =
          minServerGroupTrimSizeFromQuery != null ? minServerGroupTrimSizeFromQuery : _minServerGroupTrimSize;
      queryContext.setMinServerGroupTrimSize(minServerGroupTrimSize);
      // Set groupTrimThreshold
      Integer groupTrimThreshold = QueryOptionsUtils.getGroupTrimThreshold(queryOptions);
      if (groupTrimThreshold != null) {
        queryContext.setGroupTrimThreshold(groupTrimThreshold);
      } else {
        queryContext.setGroupTrimThreshold(_groupByTrimThreshold);
      }
      // Set optimizeMaxInitialResultHolderCapacity
      boolean optimizeMaxInitialResultHolderCapacity =
          QueryOptionsUtils.optimizeMaxInitialResultHolderCapacityEnabled(queryOptions);
      queryContext.setOptimizeMaxInitialResultHolderCapacity(optimizeMaxInitialResultHolderCapacity);
      // Set numThreadsExtractFinalResult
      Integer numThreadsExtractFinalResult = QueryOptionsUtils.getNumThreadsExtractFinalResult(queryOptions);
      if (numThreadsExtractFinalResult != null) {
        queryContext.setNumThreadsExtractFinalResult(numThreadsExtractFinalResult);
      } else {
        queryContext.setNumThreadsExtractFinalResult(DEFAULT_NUM_THREADS_EXTRACT_FINAL_RESULT);
      }
      // Set chunkSizeExtractFinalResult
      Integer chunkSizeExtractFinalResult = QueryOptionsUtils.getChunkSizeExtractFinalResult(queryOptions);
      if (chunkSizeExtractFinalResult != null) {
        queryContext.setChunkSizeExtractFinalResult(chunkSizeExtractFinalResult);
      } else {
        queryContext.setChunkSizeExtractFinalResult(DEFAULT_CHUNK_SIZE_EXTRACT_FINAL_RESULT);
      }
      // Set streamingGroupByFlushThreshold
      Integer streamingGroupByFlushThreshold =
          QueryOptionsUtils.getStreamingGroupByFlushThreshold(queryOptions);
      if (streamingGroupByFlushThreshold != null) {
        queryContext.setStreamingGroupByFlushThreshold(streamingGroupByFlushThreshold);
      }
    }

    // Set distinct query options. NOTE: This is intentionally outside the group-by block above, because DISTINCT is a
    // separate query class from aggregation (see QueryContextUtils.isDistinctQuery).
    if (QueryContextUtils.isDistinctQuery(queryContext)) {
      // Set streamingDistinctFlushThreshold
      Integer streamingDistinctFlushThreshold = QueryOptionsUtils.getStreamingDistinctFlushThreshold(queryOptions);
      if (streamingDistinctFlushThreshold != null) {
        queryContext.setStreamingDistinctFlushThreshold(streamingDistinctFlushThreshold);
      }
    }
  }

  @Override
  public PlanNode makeSegmentPlanNode(SegmentContext segmentContext, QueryContext queryContext) {
    if (QueryContextUtils.isAggregationQuery(queryContext)) {
      List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
      if (groupByExpressions != null) {
        // Group-by query
        return new GroupByPlanNode(segmentContext, queryContext);
      } else {
        // Aggregation query
        return new AggregationPlanNode(segmentContext, queryContext);
      }
    } else if (QueryContextUtils.isSelectionQuery(queryContext)) {
      return new SelectionPlanNode(segmentContext, queryContext);
    } else {
      assert QueryContextUtils.isDistinctQuery(queryContext);
      return new DistinctPlanNode(segmentContext, queryContext);
    }
  }

  @Override
  public Plan makeStreamingInstancePlan(List<SegmentContext> segmentContexts, QueryContext queryContext,
      ExecutorService executorService, ResultsBlockStreamer streamer) {
    rewriteQueryContextWithHints(queryContext, segmentContexts);
    applyQueryOptions(queryContext);
    boundGroupingSetsBaseAggregation(segmentContexts, queryContext);

    int numSegments = segmentContexts.size();
    List<PlanNode> planNodes = new ArrayList<>(numSegments);
    List<FetchContext> fetchContexts;
    if (queryContext.isEnablePrefetch()) {
      fetchContexts = new ArrayList<>(numSegments);
      for (SegmentContext segmentContext : segmentContexts) {
        FetchContext fetchContext =
            _fetchPlanner.planFetchForProcessing(segmentContext.getIndexSegment(), queryContext);
        fetchContexts.add(fetchContext);
        planNodes.add(
            new AcquireReleaseColumnsSegmentPlanNode(makeStreamingSegmentPlanNode(segmentContext, queryContext),
                segmentContext, fetchContext));
      }
    } else {
      fetchContexts = List.of();
      for (SegmentContext segmentContext : segmentContexts) {
        planNodes.add(makeStreamingSegmentPlanNode(segmentContext, queryContext));
      }
    }

    CombinePlanNode combinePlanNode = createCombinePlanNode(planNodes, queryContext, executorService, streamer);
    return new GlobalPlanImplV0(
        new StreamingInstanceResponsePlanNode(combinePlanNode, segmentContexts, fetchContexts, queryContext, streamer));
  }

  @Override
  public PlanNode makeStreamingSegmentPlanNode(SegmentContext segmentContext, QueryContext queryContext) {
    if (QueryContextUtils.isSelectionOnlyQuery(queryContext) && queryContext.getLimit() != 0) {
      // Use streaming operator only for non-empty selection-only query
      return new StreamingSelectionPlanNode(segmentContext, queryContext);
    } else {
      return makeSegmentPlanNode(segmentContext, queryContext);
    }
  }

  /// Returns the combine plan node placed above the segment plan nodes, for both the streaming and the
  /// non-streaming instance plan. `streamer` is null for a non-streaming query.
  ///
  /// Which combine operator that node builds is decided inside [CombinePlanNode] itself, per query type, so an
  /// implementation substituting one query type does not have to reproduce the dispatch for the others.
  protected CombinePlanNode createCombinePlanNode(List<PlanNode> planNodes, QueryContext queryContext,
      ExecutorService executorService, @Nullable ResultsBlockStreamer streamer) {
    return new CombinePlanNode(planNodes, queryContext, executorService, streamer);
  }

  /// Only emit BASE records when every local BASE key and every derived grouping-set key can fit the combine
  /// table. Summing each segment's dictionary product is conservative even when keys overlap across segments.
  /// Unknown, raw, transformed and multi-value columns use the per-row expansion path, and so does any mutable
  /// segment: a consuming segment keeps indexing between planning and execution, so a bound computed from its
  /// current dictionaries can be stale by the time the combine runs. This also makes a larger
  /// `groupingSetsBaseAggregationMaxGroups` option unable to bypass the actual `numGroupsLimit` memory guard.
  @VisibleForTesting
  static void boundGroupingSetsBaseAggregation(List<SegmentContext> segmentContexts, QueryContext queryContext) {
    if (queryContext.isGroupingSetsBaseAggregation()) {
      queryContext.setGroupingSetsBaseAggregationAllowed(
          fitsGroupingSetsBaseAggregationLimit(segmentContexts, queryContext));
    }
  }

  @VisibleForTesting
  static boolean fitsGroupingSetsBaseAggregationLimit(List<SegmentContext> segmentContexts,
      QueryContext queryContext) {
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    List<int[]> groupingSets = queryContext.getGroupingSets();
    int limit = queryContext.getNumGroupsLimit();
    if (groupByExpressions == null || groupingSets == null || limit <= 0) {
      return false;
    }
    int numColumns = groupByExpressions.size();
    for (int[] groupingSet : groupingSets) {
      for (int columnIndex : groupingSet) {
        if (columnIndex < 0 || columnIndex >= numColumns) {
          return false;
        }
      }
    }

    // Collect every nonempty segment's dictionaries once; each eligibility rule must hold for all of them.
    List<Dictionary[]> segmentDictionaries = new ArrayList<>();
    List<long[]> segmentCardinalities = new ArrayList<>();
    List<IndexSegment> segments = new ArrayList<>();
    boolean[] columnMayBeNull = new boolean[numColumns];
    for (SegmentContext segmentContext : segmentContexts) {
      IndexSegment segment = segmentContext.getIndexSegment();
      if (segment instanceof MutableSegment) {
        // Mutable dictionaries grow until FilterPlanNode.run() snapshots the doc count, so neither this bound
        // nor the zero-doc skip below holds at execution time.
        return false;
      }
      if (segment.getSegmentMetadata().getTotalDocs() == 0) {
        continue;
      }
      Dictionary[] dictionaries = new Dictionary[numColumns];
      long[] cardinalities = new long[numColumns];
      for (int i = 0; i < numColumns; i++) {
        ExpressionContext expression = groupByExpressions.get(i);
        if (expression.getType() != ExpressionContext.Type.IDENTIFIER) {
          return false;
        }
        DataSource dataSource = segment.getDataSourceNullable(expression.getIdentifier());
        if (dataSource == null || !dataSource.getDataSourceMetadata().isSingleValue()) {
          return false;
        }
        Dictionary dictionary = dataSource.getDictionary();
        ForwardIndexReader<?> forwardIndex = dataSource.getForwardIndex();
        if (dictionary == null || forwardIndex == null || !forwardIndex.isDictionaryEncoded()) {
          return false;
        }
        long cardinality = dictionary.length();
        if (queryContext.isNullHandlingEnabled() && dataSource.getNullValueVector() != null) {
          cardinality++;
          columnMayBeNull[i] = true;
        }
        if (cardinality == 0) {
          return false;
        }
        dictionaries[i] = dictionary;
        cardinalities[i] = cardinality;
      }
      segmentDictionaries.add(dictionaries);
      segmentCardinalities.add(cardinalities);
      segments.add(segment);
    }

    // Cheap proof first: sum each segment's dictionary-cardinality products without reading any values. This
    // multiple-counts keys shared across segments, so when it fails, fall through to the tighter (but costlier)
    // proof over the per-column UNION of dictionary values.
    if (fitsBySegmentCardinalityProducts(segmentCardinalities, groupingSets, limit)) {
      return true;
    }
    long[] maxCardinalities = new long[numColumns];
    long totalEntries = 0;
    for (int segmentIndex = 0; segmentIndex < segments.size(); segmentIndex++) {
      for (int i = 0; i < numColumns; i++) {
        maxCardinalities[i] = Math.max(maxCardinalities[i], segmentCardinalities.get(segmentIndex)[i]);
        totalEntries += segmentDictionaries.get(segmentIndex)[i].length();
      }
    }
    // Each union contains at least the largest segment dictionary. Reject impossible proofs and excessive scan
    // work before acquiring buffers or reading values.
    return totalEntries <= MAX_GROUPING_SETS_DICTIONARY_SCAN_ENTRIES
        && fitsBySegmentCardinalityProducts(List.of(maxCardinalities), groupingSets, limit)
        && fitsByColumnUnionCardinalities(segments, segmentDictionaries, columnMayBeNull, groupByExpressions,
            groupingSets, maxCardinalities, limit, queryContext.getEndTimeMs());
  }

  /// Sums each segment's per-column dictionary-cardinality product, for the BASE grouping and for every derived
  /// set, against separate `numGroupsLimit` budgets (the base table and the derived table are each capped at the
  /// limit). Conservative when segments share key values, since the same key is counted once per segment.
  private static boolean fitsBySegmentCardinalityProducts(List<long[]> segmentCardinalities,
      List<int[]> groupingSets, int limit) {
    long remainingBase = limit;
    long remainingDerived = limit;
    for (long[] cardinalities : segmentCardinalities) {
      long baseGroups = 1;
      for (long cardinality : cardinalities) {
        if (baseGroups > remainingBase / cardinality) {
          return false;
        }
        baseGroups *= cardinality;
      }
      if (baseGroups > remainingBase) {
        return false;
      }
      remainingBase -= baseGroups;

      for (int[] groupingSet : groupingSets) {
        long derivedGroups = 1;
        for (int columnIndex : groupingSet) {
          if (derivedGroups > remainingDerived / cardinalities[columnIndex]) {
            return false;
          }
          derivedGroups *= cardinalities[columnIndex];
        }
        // An empty grouping set (the grand total) skips the loop above, so re-check the final product.
        if (derivedGroups > remainingDerived) {
          return false;
        }
        remainingDerived -= derivedGroups;
      }
    }
    return true;
  }

  /// Bounds the groups by the per-column UNION of dictionary values across segments: every BASE key is a
  /// combination of per-column union values, so the product of union cardinalities bounds the base groups (and
  /// each set's product bounds that set) WITHOUT counting keys shared across segments once per segment. Reading
  /// dictionary values is bounded, cancellable plan-time work. Acquire only the dictionary being scanned, and
  /// release it even when the proof fails or the query is terminated. Unscanned columns retain their maximum
  /// segment cardinality as a lower bound, allowing early rejection after each column.
  private static boolean fitsByColumnUnionCardinalities(List<IndexSegment> segments,
      List<Dictionary[]> segmentDictionaries, boolean[] columnMayBeNull, List<ExpressionContext> groupByExpressions,
      List<int[]> groupingSets, long[] unionCardinalities, int limit, long deadlineMs) {
    for (int i = 0; i < unionCardinalities.length; i++) {
      Set<Object> union = new HashSet<>();
      for (int segmentIndex = 0; segmentIndex < segments.size(); segmentIndex++) {
        QueryThreadContext.checkTerminationAndSampleUsage("grouping-set dictionary union", deadlineMs);
        IndexSegment segment = segments.get(segmentIndex);
        Dictionary dictionary = segmentDictionaries.get(segmentIndex)[i];
        FetchContext fetchContext = new FetchContext(UUID.randomUUID(), segment.getSegmentName(),
            Map.of(groupByExpressions.get(i).getIdentifier(), List.of(StandardIndexes.dictionary())));
        try {
          segment.prefetch(fetchContext);
          segment.acquire(fetchContext);
          int length = dictionary.length();
          for (int dictId = 0; dictId < length; dictId++) {
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(dictId, "grouping-set dictionary union",
                deadlineMs);
            // getInternal: BYTES values come back as ByteArray, which supports value-based hashing.
            union.add(dictionary.getInternal(dictId));
            if (union.size() + (columnMayBeNull[i] ? 1L : 0L) > limit) {
              return false;
            }
          }
        } finally {
          segment.release(fetchContext);
        }
      }
      unionCardinalities[i] = union.size() + (columnMayBeNull[i] ? 1 : 0);
      if (!fitsBySegmentCardinalityProducts(List.of(unionCardinalities), groupingSets, limit)) {
        return false;
      }
    }
    return true;
  }

  /// Rewrites the shared context before fetch planning, using only columns available on every selected segment.
  /// A partially reloaded timestamp index must leave the original expression executable on all segments.
  /// Rewriting per segment would mutate the expressions shared by already constructed segment plans.
  @VisibleForTesting
  static void rewriteQueryContextWithHints(QueryContext queryContext, List<SegmentContext> segmentContexts) {
    Map<ExpressionContext, ExpressionContext> hints = queryContext.getExpressionOverrideHints();
    if (MapUtils.isEmpty(hints) || segmentContexts.isEmpty()) {
      return;
    }
    Map<ExpressionContext, ExpressionContext> applicableHints = getApplicableHints(hints, segmentContexts);
    if (!applicableHints.isEmpty()) {
      rewriteQueryContextWithHints(queryContext, applicableHints);
    }
  }

  /// Keeps availability checks local to this segment set without changing the broker-provided hints.
  private static Map<ExpressionContext, ExpressionContext> getApplicableHints(
      Map<ExpressionContext, ExpressionContext> hints, List<SegmentContext> segmentContexts) {
    if (hints.isEmpty()) {
      return Map.of();
    }
    if (hints.size() == 1) {
      Map.Entry<ExpressionContext, ExpressionContext> hint = hints.entrySet().iterator().next();
      String column = getOverrideColumn(hint.getValue());
      for (SegmentContext segmentContext : segmentContexts) {
        if (!segmentContext.getIndexSegment().getColumnNames().contains(column)) {
          return Map.of();
        }
      }
      // The single-entry map uses equality directly, avoiding recursive hashing of unrelated expression trees.
      return Map.of(hint.getKey(), hint.getValue());
    }
    Set<String> availableColumns = new HashSet<>();
    for (ExpressionContext target : hints.values()) {
      availableColumns.add(getOverrideColumn(target));
    }
    int numTargets = availableColumns.size();
    for (SegmentContext segmentContext : segmentContexts) {
      Set<String> columns = segmentContext.getIndexSegment().getColumnNames();
      availableColumns.removeIf(column -> !columns.contains(column));
      if (availableColumns.isEmpty()) {
        return Map.of();
      }
    }
    if (availableColumns.size() == numTargets) {
      return hints;
    }
    Map<ExpressionContext, ExpressionContext> applicableHints = new HashMap<>();
    for (Map.Entry<ExpressionContext, ExpressionContext> hint : hints.entrySet()) {
      if (availableColumns.contains(hint.getValue().getIdentifier())) {
        applicableHints.put(hint.getKey(), hint.getValue());
      }
    }
    if (applicableHints.size() == 1) {
      Map.Entry<ExpressionContext, ExpressionContext> hint = applicableHints.entrySet().iterator().next();
      return Map.of(hint.getKey(), hint.getValue());
    }
    return applicableHints;
  }

  private static String getOverrideColumn(ExpressionContext target) {
    Preconditions.checkArgument(target.getType() == ExpressionContext.Type.IDENTIFIER,
        "Expression override target must be an identifier");
    return target.getIdentifier();
  }

  /// Gates structural hashing by function name and arity; expression hashes cannot be cached across mutations.
  @Nullable
  private static Map<String, Set<Integer>> getHintFunctionArities(Map<ExpressionContext, ExpressionContext> hints) {
    if (hints.size() <= 1) {
      return null;
    }
    Map<String, Set<Integer>> arities = new HashMap<>();
    for (ExpressionContext expression : hints.keySet()) {
      FunctionContext function = expression.getFunction();
      if (function != null) {
        arities.computeIfAbsent(function.getFunctionName(), key -> new HashSet<>()).add(function.getArguments().size());
      }
    }
    return arities;
  }

  private static void rewriteQueryContextWithHints(QueryContext queryContext,
      Map<ExpressionContext, ExpressionContext> hints) {
    Set<FunctionContext> changedFunctions = Collections.newSetFromMap(new IdentityHashMap<>());
    Map<String, Set<Integer>> hintFunctionArities = getHintFunctionArities(hints);
    // FILTER predicates have separate contexts; aggregation inputs are visited through SELECT/HAVING/ORDER-BY.
    if (queryContext.hasFilteredAggregations()) {
      List<Pair<AggregationFunction, FilterContext>> filteredAggregations =
          queryContext.getFilteredAggregationFunctions();
      for (Pair<AggregationFunction, FilterContext> aggregation : filteredAggregations) {
        if (aggregation.getRight() != null) {
          overrideWithExpressionHints(aggregation.getRight(), hints, hintFunctionArities, changedFunctions);
        }
      }
    }
    queryContext.getSelectExpressions().replaceAll(
        expression -> overrideWithExpressionHints(expression, hints, hintFunctionArities, changedFunctions));
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    if (CollectionUtils.isNotEmpty(groupByExpressions)) {
      groupByExpressions.replaceAll(
          expression -> overrideWithExpressionHints(expression, hints, hintFunctionArities, changedFunctions));
    }
    List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
    if (CollectionUtils.isNotEmpty(orderByExpressions)) {
      orderByExpressions.replaceAll(orderBy -> {
        ExpressionContext expression = orderBy.getExpression();
        ExpressionContext rewritten =
            overrideWithExpressionHints(expression, hints, hintFunctionArities, changedFunctions);
        return rewritten == expression ? orderBy
            : new OrderByExpressionContext(rewritten, orderBy.isAsc(), orderBy.isNullsLast());
      });
    }
    if (queryContext.getFilter() != null) {
      overrideWithExpressionHints(queryContext.getFilter(), hints, hintFunctionArities, changedFunctions);
    }
    if (queryContext.getHavingFilter() != null) {
      overrideWithExpressionHints(queryContext.getHavingFilter(), hints, hintFunctionArities, changedFunctions);
    }
    if (!changedFunctions.isEmpty()) {
      queryContext.prepareForExecution(changedFunctions);
    }
  }

  @VisibleForTesting
  public static void overrideWithExpressionHints(FilterContext filter, IndexSegment indexSegment,
      Map<ExpressionContext, ExpressionContext> expressionOverrideHints) {
    Map<ExpressionContext, ExpressionContext> hints =
        getApplicableHints(expressionOverrideHints, List.of(new SegmentContext(indexSegment)));
    overrideWithExpressionHints(filter, hints, getHintFunctionArities(hints),
        Collections.newSetFromMap(new IdentityHashMap<>()));
  }

  private static void overrideWithExpressionHints(FilterContext filter,
      Map<ExpressionContext, ExpressionContext> expressionOverrideHints,
      @Nullable Map<String, Set<Integer>> hintFunctionArities, Set<FunctionContext> changedFunctions) {
    if (filter.isConstant()) {
      return;
    }
    if (filter.getChildren() != null) {
      for (FilterContext child : filter.getChildren()) {
        overrideWithExpressionHints(child, expressionOverrideHints, hintFunctionArities, changedFunctions);
      }
    } else {
      Predicate predicate = filter.getPredicate();
      ExpressionContext rewritten = overrideWithExpressionHints(predicate.getLhs(), expressionOverrideHints,
          hintFunctionArities, changedFunctions);
      predicate.setLhs(rewritten);
    }
  }

  @VisibleForTesting
  public static ExpressionContext overrideWithExpressionHints(ExpressionContext expression, IndexSegment indexSegment,
      Map<ExpressionContext, ExpressionContext> expressionOverrideHints) {
    Map<ExpressionContext, ExpressionContext> hints =
        getApplicableHints(expressionOverrideHints, List.of(new SegmentContext(indexSegment)));
    return overrideWithExpressionHints(expression, hints, getHintFunctionArities(hints),
        Collections.newSetFromMap(new IdentityHashMap<>()));
  }

  private static ExpressionContext overrideWithExpressionHints(ExpressionContext expression,
      Map<ExpressionContext, ExpressionContext> expressionOverrideHints,
      @Nullable Map<String, Set<Integer>> hintFunctionArities, Set<FunctionContext> changedFunctions) {
    if (expression.getType() != ExpressionContext.Type.FUNCTION) {
      return expression;
    }
    FunctionContext function = expression.getFunction();
    List<ExpressionContext> arguments = function.getArguments();
    Set<Integer> arities = hintFunctionArities != null ? hintFunctionArities.get(function.getFunctionName()) : null;
    if (hintFunctionArities == null || arities != null && arities.contains(arguments.size())) {
      ExpressionContext overrideExpression = expressionOverrideHints.get(expression);
      if (overrideExpression != null) {
        changedFunctions.add(function);
        return overrideExpression;
      }
    }
    for (int i = 0; i < arguments.size(); i++) {
      ExpressionContext argument = arguments.get(i);
      ExpressionContext rewritten =
          overrideWithExpressionHints(argument, expressionOverrideHints, hintFunctionArities, changedFunctions);
      if (rewritten != argument || changedFunctions.contains(argument.getFunction())) {
        changedFunctions.add(function);
      }
      arguments.set(i, rewritten);
    }
    return expression;
  }
}
