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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.core.plan.AcquireReleaseColumnsSegmentPlanNode;
import org.apache.pinot.core.plan.AggregationGroupByOrderByPlanNode;
import org.apache.pinot.core.plan.AggregationGroupByPlanNode;
import org.apache.pinot.core.plan.AggregationPlanNode;
import org.apache.pinot.core.plan.CombinePlanNode;
import org.apache.pinot.core.plan.DictionaryBasedAggregationPlanNode;
import org.apache.pinot.core.plan.DictionaryBasedDistinctPlanNode;
import org.apache.pinot.core.plan.DistinctPlanNode;
import org.apache.pinot.core.plan.GlobalPlanImplV0;
import org.apache.pinot.core.plan.InstanceResponsePlanNode;
import org.apache.pinot.core.plan.MetadataBasedAggregationPlanNode;
import org.apache.pinot.core.plan.Plan;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.core.plan.SelectionPlanNode;
import org.apache.pinot.core.plan.StreamingSelectionPlanNode;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.function.DistinctAggregationFunction;
import org.apache.pinot.core.query.config.QueryExecutorConfig;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.core.util.QueryOptions;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
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
  public static final String ENABLE_PREFETCH = "enable.prefetch";
  public static final boolean DEFAULT_ENABLE_PREFETCH = false;

  private static final Logger LOGGER = LoggerFactory.getLogger(InstancePlanMakerImplV2.class);

  private final int _maxExecutionThreads;
  private final int _maxInitialResultHolderCapacity;
  // Limit on number of groups stored for each segment, beyond which no new group will be created
  private final int _numGroupsLimit;
  // Used for SQL GROUP BY (server combine)
  private final int _minSegmentGroupTrimSize;
  private final int _minServerGroupTrimSize;
  private final int _groupByTrimThreshold;
  private final boolean _enablePrefetch;

  @VisibleForTesting
  public InstancePlanMakerImplV2() {
    _maxExecutionThreads = DEFAULT_MAX_EXECUTION_THREADS;
    _maxInitialResultHolderCapacity = DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY;
    _numGroupsLimit = DEFAULT_NUM_GROUPS_LIMIT;
    _minSegmentGroupTrimSize = DEFAULT_MIN_SEGMENT_GROUP_TRIM_SIZE;
    _minServerGroupTrimSize = DEFAULT_MIN_SERVER_GROUP_TRIM_SIZE;
    _groupByTrimThreshold = DEFAULT_GROUPBY_TRIM_THRESHOLD;
    _enablePrefetch = DEFAULT_ENABLE_PREFETCH;
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
    _enablePrefetch = DEFAULT_ENABLE_PREFETCH;
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
    _enablePrefetch = Boolean.parseBoolean(config.getProperty(ENABLE_PREFETCH));
    LOGGER.info("Initializing plan maker with maxInitialResultHolderCapacity: {}, numGroupsLimit: {}, "
            + "minSegmentGroupTrimSize: {}, minServerGroupTrimSize: {}, enablePrefetch: {}",
        _maxInitialResultHolderCapacity, _numGroupsLimit, _minSegmentGroupTrimSize, _minServerGroupTrimSize,
        _enablePrefetch);
  }

  @Override
  public Plan makeInstancePlan(List<IndexSegment> indexSegments, QueryContext queryContext,
      ExecutorService executorService, long endTimeMs) {
    List<PlanNode> planNodes = new ArrayList<>(indexSegments.size());
    List<FetchContext> fetchContexts;

    if (_enablePrefetch) {
      fetchContexts = new ArrayList<>(indexSegments.size());
      List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
      for (IndexSegment indexSegment : indexSegments) {
        Set<String> columns;
        if (selectExpressions.size() == 1 && "*".equals(selectExpressions.get(0).getIdentifier())) {
          columns = indexSegment.getPhysicalColumnNames();
        } else {
          columns = queryContext.getColumns();
        }
        FetchContext fetchContext = new FetchContext(UUID.randomUUID(), indexSegment.getSegmentName(), columns);
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

    CombinePlanNode combinePlanNode =
        new CombinePlanNode(planNodes, queryContext, executorService, endTimeMs, getMaxExecutionThreads(queryContext),
            _numGroupsLimit, _minServerGroupTrimSize, _groupByTrimThreshold, null);
    return new GlobalPlanImplV0(new InstanceResponsePlanNode(combinePlanNode, indexSegments, fetchContexts));
  }

  private int getMaxExecutionThreads(QueryContext queryContext) {
    Map<String, String> queryOptions = queryContext.getQueryOptions();
    if (queryOptions != null) {
      Integer maxExecutionThreadsFromQuery = QueryOptions.getMaxExecutionThreads(queryOptions);
      if (maxExecutionThreadsFromQuery != null && maxExecutionThreadsFromQuery > 0) {
        // Do not allow query to override the execution threads over the instance-level limit
        if (_maxExecutionThreads > 0) {
          return Math.min(_maxExecutionThreads, maxExecutionThreadsFromQuery);
        } else {
          return maxExecutionThreadsFromQuery;
        }
      }
    }
    return _maxExecutionThreads;
  }

  @Override
  public PlanNode makeSegmentPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    if (QueryContextUtils.isAggregationQuery(queryContext)) {
      List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
      if (groupByExpressions != null) {
        // Aggregation group-by query

        Map<String, String> queryOptions = queryContext.getQueryOptions();
        if (queryOptions != null && QueryOptions.isGroupByModeSQL(queryOptions)) {
          return new AggregationGroupByOrderByPlanNode(indexSegment, queryContext, _maxInitialResultHolderCapacity,
              _numGroupsLimit, _minSegmentGroupTrimSize);
        }
        return new AggregationGroupByPlanNode(indexSegment, queryContext, _maxInitialResultHolderCapacity,
            _numGroupsLimit);
      } else {
        // Aggregation only query

        // Use metadata/dictionary to solve the query if possible
        // NOTE: Skip the segment with valid doc index because the valid doc index is equivalent to a filter.
        if (queryContext.getFilter() == null && indexSegment.getValidDocIds() == null) {
          if (isFitForMetadataBasedPlan(queryContext)) {
            return new MetadataBasedAggregationPlanNode(indexSegment, queryContext);
          } else if (isFitForDictionaryBasedPlan(queryContext, indexSegment)) {
            return new DictionaryBasedAggregationPlanNode(indexSegment, queryContext);
          }
        }
        return new AggregationPlanNode(indexSegment, queryContext);
      }
    } else if (QueryContextUtils.isSelectionQuery(queryContext)) {
      return new SelectionPlanNode(indexSegment, queryContext);
    } else {
      assert QueryContextUtils.isDistinctQuery(queryContext);
      return getDistinctPlanNode(indexSegment, queryContext);
    }
  }

  @Override
  public Plan makeStreamingInstancePlan(List<IndexSegment> indexSegments, QueryContext queryContext,
      ExecutorService executorService, StreamObserver<Server.ServerResponse> streamObserver, long endTimeMs) {
    List<PlanNode> planNodes = new ArrayList<>(indexSegments.size());
    for (IndexSegment indexSegment : indexSegments) {
      planNodes.add(makeStreamingSegmentPlanNode(indexSegment, queryContext));
    }
    CombinePlanNode combinePlanNode =
        new CombinePlanNode(planNodes, queryContext, executorService, endTimeMs, getMaxExecutionThreads(queryContext),
            _numGroupsLimit, _minServerGroupTrimSize, _groupByTrimThreshold, streamObserver);
    return new GlobalPlanImplV0(new InstanceResponsePlanNode(combinePlanNode, indexSegments, Collections.emptyList()));
  }

  @Override
  public PlanNode makeStreamingSegmentPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    if (!QueryContextUtils.isSelectionQuery(queryContext)) {
      throw new UnsupportedOperationException("Only selection queries are supported");
    } else {
      // Selection query
      return new StreamingSelectionPlanNode(indexSegment, queryContext);
    }
  }

  /**
   * Returns {@code true} if the given aggregation-only without filter QueryContext can be solved with segment metadata,
   * {@code false} otherwise.
   * <p>Aggregations supported: COUNT
   */
  @VisibleForTesting
  static boolean isFitForMetadataBasedPlan(QueryContext queryContext) {
    List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
    for (ExpressionContext expression : selectExpressions) {
      if (!expression.getFunction().getFunctionName().equals("count")) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns {@code true} if the given aggregation-only without filter QueryContext can be solved with dictionary,
   * {@code false} otherwise.
   * <p>Aggregations supported: MIN, MAX, MIN_MAX_RANGE, DISTINCT_COUNT, SEGMENT_PARTITIONED_DISTINCT_COUNT
   */
  @VisibleForTesting
  static boolean isFitForDictionaryBasedPlan(QueryContext queryContext, IndexSegment indexSegment) {
    List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
    for (ExpressionContext expression : selectExpressions) {
      FunctionContext function = expression.getFunction();
      String functionName = function.getFunctionName();
      if (!AggregationFunctionUtils.isFitForDictionaryBasedComputation(functionName)) {
        return false;
      }

      ExpressionContext argument = function.getArguments().get(0);
      if (argument.getType() != ExpressionContext.Type.IDENTIFIER) {
        return false;
      }
      String column = argument.getIdentifier();
      Dictionary dictionary = indexSegment.getDataSource(column).getDictionary();
      if (dictionary == null) {
        return false;
      }
      // TODO: Remove this check because MutableDictionary maintains min/max value
      // NOTE: DISTINCT_COUNT and SEGMENT_PARTITIONED_DISTINCT_COUNT does not require sorted dictionary
      if (!dictionary.isSorted() && !functionName.equalsIgnoreCase(AggregationFunctionType.DISTINCTCOUNT.name())
          && !functionName.equalsIgnoreCase(AggregationFunctionType.SEGMENTPARTITIONEDDISTINCTCOUNT.name())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns dictionary based distinct plan node iff supported, else distinct plan node
   */
  private PlanNode getDistinctPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();

    // If we have gotten here, it must have been verified that there is only on aggregation function in the context
    // and it is a DistinctAggregationFunction

    DistinctAggregationFunction distinctAggregationFunction = (DistinctAggregationFunction) aggregationFunctions[0];
    List<ExpressionContext> expressions = distinctAggregationFunction.getInputExpressions();

    if (expressions.size() == 1 && queryContext.getFilter() == null) {
      ExpressionContext expression = expressions.get(0);

      if (expression.getType() == ExpressionContext.Type.IDENTIFIER) {
        String column = expression.getIdentifier();
        Dictionary dictionary = indexSegment.getDataSource(column).getDictionary();
        if (dictionary != null) {
          return new DictionaryBasedDistinctPlanNode(indexSegment, queryContext, dictionary);
        }
      }
    }

    return new DistinctPlanNode(indexSegment, queryContext);
  }
}
