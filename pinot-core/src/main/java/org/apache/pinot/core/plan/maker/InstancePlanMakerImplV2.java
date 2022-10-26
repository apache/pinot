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
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
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
import org.apache.pinot.core.query.config.QueryExecutorConfig;
import org.apache.pinot.core.query.prefetch.FetchPlanner;
import org.apache.pinot.core.query.prefetch.FetchPlannerRegistry;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.core.util.QueryOptionsUtils;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>InstancePlanMakerImplV2</code> class is the default implementation of {@link PlanMaker}.
 */
public class InstancePlanMakerImplV2 implements PlanMaker {
  // Instance config key for maximum number of threads used to execute the query
  // Set as pinot.server.query.executor.max.execution.threads
  public static final String MAX_EXECUTION_THREADS_KEY = "max.execution.threads";
  public static final int DEFAULT_MAX_EXECUTION_THREADS = -1;

  public static final String MAX_INITIAL_RESULT_HOLDER_CAPACITY_KEY = "max.init.group.holder.capacity";
  public static final int DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY = 10_000;
  public static final String NUM_GROUPS_LIMIT_KEY = "num.groups.limit";
  public static final int DEFAULT_NUM_GROUPS_LIMIT = 100_000;

  // Instance config key for minimum segment-level group trim size
  // Set as pinot.server.query.executor.min.segment.group.trim.size
  public static final String MIN_SEGMENT_GROUP_TRIM_SIZE_KEY = "min.segment.group.trim.size";
  public static final int DEFAULT_MIN_SEGMENT_GROUP_TRIM_SIZE = -1;
  // Instance config key for minimum server-level group trim size
  // Caution: Setting it to non-positive value (disable trim) or large value can give more accurate result, but can
  //          potentially cause memory issue
  // Set as pinot.server.query.executor.min.server.group.trim.size
  public static final String MIN_SERVER_GROUP_TRIM_SIZE_KEY = "min.server.group.trim.size";
  public static final int DEFAULT_MIN_SERVER_GROUP_TRIM_SIZE = GroupByUtils.DEFAULT_MIN_NUM_GROUPS;
  // set as pinot.server.query.executor.groupby.trim.threshold
  public static final String GROUPBY_TRIM_THRESHOLD_KEY = "groupby.trim.threshold";
  public static final int DEFAULT_GROUPBY_TRIM_THRESHOLD = 1_000_000;

  private static final Logger LOGGER = LoggerFactory.getLogger(InstancePlanMakerImplV2.class);

  private final int _maxExecutionThreads;
  private final int _maxInitialResultHolderCapacity;
  // Limit on number of groups stored for each segment, beyond which no new group will be created
  private final int _numGroupsLimit;
  // Used for SQL GROUP BY (server combine)
  private final int _minSegmentGroupTrimSize;
  private final int _minServerGroupTrimSize;
  private final int _groupByTrimThreshold;
  private final FetchPlanner _fetchPlanner = FetchPlannerRegistry.getPlanner();

  @VisibleForTesting
  public InstancePlanMakerImplV2() {
    _maxExecutionThreads = DEFAULT_MAX_EXECUTION_THREADS;
    _maxInitialResultHolderCapacity = DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY;
    _numGroupsLimit = DEFAULT_NUM_GROUPS_LIMIT;
    _minSegmentGroupTrimSize = DEFAULT_MIN_SEGMENT_GROUP_TRIM_SIZE;
    _minServerGroupTrimSize = DEFAULT_MIN_SERVER_GROUP_TRIM_SIZE;
    _groupByTrimThreshold = DEFAULT_GROUPBY_TRIM_THRESHOLD;
  }

  @VisibleForTesting
  public InstancePlanMakerImplV2(int maxInitialResultHolderCapacity, int numGroupsLimit, int minSegmentGroupTrimSize,
      int minServerGroupTrimSize, int groupByTrimThreshold) {
    _maxExecutionThreads = DEFAULT_MAX_EXECUTION_THREADS;
    _maxInitialResultHolderCapacity = maxInitialResultHolderCapacity;
    _numGroupsLimit = numGroupsLimit;
    _minSegmentGroupTrimSize = minSegmentGroupTrimSize;
    _minServerGroupTrimSize = minServerGroupTrimSize;
    _groupByTrimThreshold = groupByTrimThreshold;
  }

  /**
   * Constructor for usage when client requires to pass {@link QueryExecutorConfig} to this class.
   * <ul>
   *   <li>Set limit on the initial result holder capacity</li>
   *   <li>Set limit on number of groups returned from each segment and combined result</li>
   * </ul>
   *
   * @param queryExecutorConfig Query executor configuration
   */
  public InstancePlanMakerImplV2(QueryExecutorConfig queryExecutorConfig) {
    PinotConfiguration config = queryExecutorConfig.getConfig();
    _maxExecutionThreads = config.getProperty(MAX_EXECUTION_THREADS_KEY, DEFAULT_MAX_EXECUTION_THREADS);
    _maxInitialResultHolderCapacity =
        config.getProperty(MAX_INITIAL_RESULT_HOLDER_CAPACITY_KEY, DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY);
    _numGroupsLimit = config.getProperty(NUM_GROUPS_LIMIT_KEY, DEFAULT_NUM_GROUPS_LIMIT);
    Preconditions.checkState(_maxInitialResultHolderCapacity <= _numGroupsLimit,
        "Invalid configuration: maxInitialResultHolderCapacity: %d must be smaller or equal to numGroupsLimit: %d",
        _maxInitialResultHolderCapacity, _numGroupsLimit);
    _minSegmentGroupTrimSize = config.getProperty(MIN_SEGMENT_GROUP_TRIM_SIZE_KEY, DEFAULT_MIN_SEGMENT_GROUP_TRIM_SIZE);
    _minServerGroupTrimSize = config.getProperty(MIN_SERVER_GROUP_TRIM_SIZE_KEY, DEFAULT_MIN_SERVER_GROUP_TRIM_SIZE);
    _groupByTrimThreshold = config.getProperty(GROUPBY_TRIM_THRESHOLD_KEY, DEFAULT_GROUPBY_TRIM_THRESHOLD);
    Preconditions.checkState(_groupByTrimThreshold > 0,
        "Invalid configurable: groupByTrimThreshold: %d must be positive", _groupByTrimThreshold);
    LOGGER.info("Initializing plan maker with maxInitialResultHolderCapacity: {}, numGroupsLimit: {}, "
            + "minSegmentGroupTrimSize: {}, minServerGroupTrimSize: {}", _maxInitialResultHolderCapacity,
        _numGroupsLimit,
        _minSegmentGroupTrimSize, _minServerGroupTrimSize);
  }

  @Override
  public Plan makeInstancePlan(List<IndexSegment> indexSegments, QueryContext queryContext,
      ExecutorService executorService, ServerMetrics serverMetrics) {
    applyQueryOptions(queryContext);

    int numSegments = indexSegments.size();
    List<PlanNode> planNodes = new ArrayList<>(numSegments);
    List<FetchContext> fetchContexts;

    if (queryContext.isEnablePrefetch()) {
      fetchContexts = new ArrayList<>(numSegments);
      for (IndexSegment indexSegment : indexSegments) {
        FetchContext fetchContext = _fetchPlanner.planFetchForProcessing(indexSegment, queryContext);
        fetchContexts.add(fetchContext);
        planNodes.add(
            new AcquireReleaseColumnsSegmentPlanNode(makeSegmentPlanNode(indexSegment, queryContext), indexSegment,
                fetchContext));
      }
    } else {
      fetchContexts = Collections.emptyList();
      for (IndexSegment indexSegment : indexSegments) {
        planNodes.add(makeSegmentPlanNode(indexSegment, queryContext));
      }
    }

    CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, queryContext, executorService, null);
    return new GlobalPlanImplV0(
        new InstanceResponsePlanNode(combinePlanNode, indexSegments, fetchContexts, queryContext));
  }

  private void applyQueryOptions(QueryContext queryContext) {
    Map<String, String> queryOptions = queryContext.getQueryOptions();

    // Set skipUpsert
    queryContext.setSkipUpsert(QueryOptionsUtils.isSkipUpsert(queryOptions));

    // Set skipStarTree
    queryContext.setSkipStarTree(QueryOptionsUtils.isSkipStarTree(queryOptions));

    // Set skipScanFilterReorder
    queryContext.setSkipScanFilterReorder(QueryOptionsUtils.isSkipScanFilterReorder(queryOptions));

    // Set maxExecutionThreads
    int maxExecutionThreads;
    Integer maxExecutionThreadsFromQuery = QueryOptionsUtils.getMaxExecutionThreads(queryOptions);
    if (maxExecutionThreadsFromQuery != null && maxExecutionThreadsFromQuery > 0) {
      // Do not allow query to override the execution threads over the instance-level limit
      if (_maxExecutionThreads > 0) {
        maxExecutionThreads = Math.min(_maxExecutionThreads, maxExecutionThreadsFromQuery);
      } else {
        maxExecutionThreads = maxExecutionThreadsFromQuery;
      }
    } else {
      maxExecutionThreads = _maxExecutionThreads;
    }
    queryContext.setMaxExecutionThreads(maxExecutionThreads);

    // Set group-by query options
    if (QueryContextUtils.isAggregationQuery(queryContext) && queryContext.getGroupByExpressions() != null) {

      // Set maxInitialResultHolderCapacity
      queryContext.setMaxInitialResultHolderCapacity(_maxInitialResultHolderCapacity);

      // Set numGroupsLimit
      queryContext.setNumGroupsLimit(_numGroupsLimit);

      // Set minSegmentGroupTrimSize
      Integer minSegmentGroupTrimSizeFromQuery = QueryOptionsUtils.getMinSegmentGroupTrimSize(queryOptions);
      int minSegmentGroupTrimSize =
          minSegmentGroupTrimSizeFromQuery != null ? minSegmentGroupTrimSizeFromQuery : _minSegmentGroupTrimSize;
      queryContext.setMinSegmentGroupTrimSize(minSegmentGroupTrimSize);

      // Set minServerGroupTrimSize
      Integer minServerGroupTrimSizeFromQuery = QueryOptionsUtils.getMinServerGroupTrimSize(queryOptions);
      int minServerGroupTrimSize =
          minServerGroupTrimSizeFromQuery != null ? minServerGroupTrimSizeFromQuery : _minServerGroupTrimSize;
      queryContext.setMinServerGroupTrimSize(minServerGroupTrimSize);

      // Set groupTrimThreshold
      queryContext.setGroupTrimThreshold(_groupByTrimThreshold);
    }
  }

  @Override
  public PlanNode makeSegmentPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    rewriteQueryContextWithHints(queryContext, indexSegment);
    if (QueryContextUtils.isAggregationQuery(queryContext)) {
      List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
      if (groupByExpressions != null) {
        // Group-by query
        return new GroupByPlanNode(indexSegment, queryContext);
      } else {
        // Aggregation query
        return new AggregationPlanNode(indexSegment, queryContext);
      }
    } else if (QueryContextUtils.isSelectionQuery(queryContext)) {
      return new SelectionPlanNode(indexSegment, queryContext);
    } else {
      assert QueryContextUtils.isDistinctQuery(queryContext);
      return new DistinctPlanNode(indexSegment, queryContext);
    }
  }

  @Override
  public Plan makeStreamingInstancePlan(List<IndexSegment> indexSegments, QueryContext queryContext,
      ExecutorService executorService, StreamObserver<Server.ServerResponse> streamObserver,
      ServerMetrics serverMetrics) {
    applyQueryOptions(queryContext);

    List<PlanNode> planNodes = new ArrayList<>(indexSegments.size());
    for (IndexSegment indexSegment : indexSegments) {
      planNodes.add(makeStreamingSegmentPlanNode(indexSegment, queryContext));
    }
    CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, queryContext, executorService, streamObserver);
    if (QueryContextUtils.isSelectionOnlyQuery(queryContext)) {
      // selection-only is streamed in StreamingSelectionPlanNode --> here only metadata block is returned.
      return new GlobalPlanImplV0(
          new InstanceResponsePlanNode(combinePlanNode, indexSegments, Collections.emptyList(), queryContext));
    } else {
      // non-selection-only requires a StreamingInstanceResponsePlanNode to stream data block back and metadata block
      // as final return.
      return new GlobalPlanImplV0(
          new StreamingInstanceResponsePlanNode(combinePlanNode, indexSegments, Collections.emptyList(), queryContext,
              streamObserver));
    }
  }

  @Override
  public PlanNode makeStreamingSegmentPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    if (!QueryContextUtils.isSelectionOnlyQuery(queryContext)) {
      // non-selection-only query goes through normal SegmentPlan.
      // it will be stream back via StreamingInstanceResponsePlanNode
      return makeSegmentPlanNode(indexSegment, queryContext);
    } else {
      // Selection-only query can be directly stream back
      return new StreamingSelectionPlanNode(indexSegment, queryContext);
    }
  }

  /**
   * In-place rewrite QueryContext based on the information from local IndexSegment.
   *
   * @param queryContext
   * @param indexSegment
   */
  @VisibleForTesting
  public static void rewriteQueryContextWithHints(QueryContext queryContext, IndexSegment indexSegment) {
    Map<ExpressionContext, ExpressionContext> expressionOverrideHints = queryContext.getExpressionOverrideHints();
    if (MapUtils.isEmpty(expressionOverrideHints)) {
      return;
    }

    List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
    selectExpressions.replaceAll(
        expression -> overrideWithExpressionHints(expression, indexSegment, expressionOverrideHints));

    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    if (CollectionUtils.isNotEmpty(groupByExpressions)) {
      groupByExpressions.replaceAll(
          expression -> overrideWithExpressionHints(expression, indexSegment, expressionOverrideHints));
    }

    List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
    if (CollectionUtils.isNotEmpty(orderByExpressions)) {
      orderByExpressions.replaceAll(expression -> new OrderByExpressionContext(
          overrideWithExpressionHints(expression.getExpression(), indexSegment, expressionOverrideHints),
          expression.isAsc()));
    }

    // In-place override
    FilterContext filter = queryContext.getFilter();
    if (filter != null) {
      overrideWithExpressionHints(filter, indexSegment, expressionOverrideHints);
    }

    // In-place override
    FilterContext havingFilter = queryContext.getHavingFilter();
    if (havingFilter != null) {
      overrideWithExpressionHints(havingFilter, indexSegment, expressionOverrideHints);
    }
  }

  @VisibleForTesting
  public static void overrideWithExpressionHints(FilterContext filter, IndexSegment indexSegment,
      Map<ExpressionContext, ExpressionContext> expressionOverrideHints) {
    if (filter.getChildren() != null) {
      // AND, OR, NOT
      for (FilterContext child : filter.getChildren()) {
        overrideWithExpressionHints(child, indexSegment, expressionOverrideHints);
      }
    } else {
      // PREDICATE
      Predicate predicate = filter.getPredicate();
      predicate.setLhs(overrideWithExpressionHints(predicate.getLhs(), indexSegment, expressionOverrideHints));
    }
  }

  @VisibleForTesting
  public static ExpressionContext overrideWithExpressionHints(ExpressionContext expression, IndexSegment indexSegment,
      Map<ExpressionContext, ExpressionContext> expressionOverrideHints) {
    if (expression.getType() != ExpressionContext.Type.FUNCTION) {
      return expression;
    }
    ExpressionContext overrideExpression = expressionOverrideHints.get(expression);
    if (overrideExpression != null && overrideExpression.getIdentifier() != null && indexSegment.getColumnNames()
        .contains(overrideExpression.getIdentifier())) {
      return overrideExpression;
    }
    expression.getFunction().getArguments()
        .replaceAll(argument -> overrideWithExpressionHints(argument, indexSegment, expressionOverrideHints));
    return expression;
  }
}
