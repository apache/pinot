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
package org.apache.pinot.core.query.executor;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerQueryPhase;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.core.common.ExplainPlanRowData;
import org.apache.pinot.core.common.ExplainPlanRows;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExplainResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ResultsBlockUtils;
import org.apache.pinot.core.operator.filter.EmptyFilterOperator;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.core.plan.Plan;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.config.QueryExecutorConfig;
import org.apache.pinot.core.query.pruner.SegmentPrunerService;
import org.apache.pinot.core.query.pruner.SegmentPrunerStatistics;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.TimerContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.query.utils.idset.IdSet;
import org.apache.pinot.core.util.QueryOptionsUtils;
import org.apache.pinot.core.util.trace.TraceContext;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.exception.QueryCancelledException;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ThreadSafe
public class ServerQueryExecutorV1Impl implements QueryExecutor {
  public static final String ENABLE_PREFETCH = "enable.prefetch";

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerQueryExecutorV1Impl.class);
  private static final String IN_PARTITIONED_SUBQUERY = "inPartitionedSubquery";

  private InstanceDataManager _instanceDataManager;
  private ServerMetrics _serverMetrics;
  private SegmentPrunerService _segmentPrunerService;
  private PlanMaker _planMaker;
  private long _defaultTimeoutMs = CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS;
  private boolean _enablePrefetch;

  @Override
  public synchronized void init(PinotConfiguration config, InstanceDataManager instanceDataManager,
      ServerMetrics serverMetrics)
      throws ConfigurationException {
    _instanceDataManager = instanceDataManager;
    _serverMetrics = serverMetrics;
    QueryExecutorConfig queryExecutorConfig = new QueryExecutorConfig(config);
    LOGGER.info("Trying to build SegmentPrunerService");
    _segmentPrunerService = new SegmentPrunerService(queryExecutorConfig.getPrunerConfig());
    LOGGER.info("Trying to build QueryPlanMaker");
    _planMaker = new InstancePlanMakerImplV2(queryExecutorConfig);
    if (queryExecutorConfig.getTimeOut() > 0) {
      _defaultTimeoutMs = queryExecutorConfig.getTimeOut();
    }
    _enablePrefetch = Boolean.parseBoolean(config.getProperty(ENABLE_PREFETCH));
    LOGGER.info("Initialized query executor with defaultTimeoutMs: {}, enablePrefetch: {}", _defaultTimeoutMs,
        _enablePrefetch);
  }

  @Override
  public synchronized void start() {
    LOGGER.info("Query executor started");
  }

  @Override
  public synchronized void shutDown() {
    LOGGER.info("Query executor shut down");
  }

  @Override
  public InstanceResponseBlock execute(ServerQueryRequest queryRequest, ExecutorService executorService,
      @Nullable StreamObserver<Server.ServerResponse> responseObserver) {
    if (!queryRequest.isEnableTrace()) {
      return executeInternal(queryRequest, executorService, responseObserver);
    }
    try {
      long requestId = queryRequest.getRequestId();
      // NOTE: Use negative request id as trace id for REALTIME table to prevent id conflict when the same request
      //       hitting both OFFLINE and REALTIME table (hybrid table setup)
      long traceId =
          TableNameBuilder.isRealtimeTableResource(queryRequest.getTableNameWithType()) ? -requestId : requestId;
      Tracing.getTracer().register(traceId);
      return executeInternal(queryRequest, executorService, responseObserver);
    } finally {
      Tracing.getTracer().unregister();
    }
  }

  private InstanceResponseBlock executeInternal(ServerQueryRequest queryRequest, ExecutorService executorService,
      @Nullable StreamObserver<Server.ServerResponse> responseObserver) {
    TimerContext timerContext = queryRequest.getTimerContext();
    TimerContext.Timer schedulerWaitTimer = timerContext.getPhaseTimer(ServerQueryPhase.SCHEDULER_WAIT);
    if (schedulerWaitTimer != null) {
      schedulerWaitTimer.stopAndRecord();
    }
    TimerContext.Timer queryProcessingTimer = timerContext.startNewPhaseTimer(ServerQueryPhase.QUERY_PROCESSING);

    long requestId = queryRequest.getRequestId();
    String tableNameWithType = queryRequest.getTableNameWithType();
    QueryContext queryContext = queryRequest.getQueryContext();
    LOGGER.debug("Incoming request Id: {}, query: {}", requestId, queryContext);

    // Use the timeout passed from the request if exists, or the instance-level timeout
    long queryTimeoutMs = _defaultTimeoutMs;
    Long timeoutFromQueryOptions = QueryOptionsUtils.getTimeoutMs(queryContext.getQueryOptions());
    if (timeoutFromQueryOptions != null) {
      queryTimeoutMs = timeoutFromQueryOptions;
    }
    long queryArrivalTimeMs = timerContext.getQueryArrivalTimeMs();
    long queryEndTimeMs = timerContext.getQueryArrivalTimeMs() + queryTimeoutMs;
    queryContext.setEndTimeMs(queryEndTimeMs);

    queryContext.setEnablePrefetch(_enablePrefetch);

    // Query scheduler wait time already exceeds query timeout, directly return
    long querySchedulingTimeMs = System.currentTimeMillis() - queryArrivalTimeMs;
    if (querySchedulingTimeMs >= queryTimeoutMs) {
      _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.SCHEDULING_TIMEOUT_EXCEPTIONS, 1);
      String errorMessage =
          String.format("Query scheduling took %dms (longer than query timeout of %dms) on server: %s",
              querySchedulingTimeMs, queryTimeoutMs, _instanceDataManager.getInstanceId());
      InstanceResponseBlock instanceResponse = new InstanceResponseBlock();
      instanceResponse.addException(
          QueryException.getException(QueryException.QUERY_SCHEDULING_TIMEOUT_ERROR, errorMessage));
      LOGGER.error("{} while processing requestId: {}", errorMessage, requestId);
      return instanceResponse;
    }

    TableDataManager tableDataManager = _instanceDataManager.getTableDataManager(tableNameWithType);
    if (tableDataManager == null) {
      String errorMessage = String.format("Failed to find table: %s on server: %s", tableNameWithType,
          _instanceDataManager.getInstanceId());
      InstanceResponseBlock instanceResponse = new InstanceResponseBlock();
      instanceResponse.addException(
          QueryException.getException(QueryException.SERVER_TABLE_MISSING_ERROR, errorMessage));
      LOGGER.error("{} while processing requestId: {}", errorMessage, requestId);
      return instanceResponse;
    }

    List<String> segmentsToQuery = queryRequest.getSegmentsToQuery();
    List<String> notAcquiredSegments = new ArrayList<>();
    List<SegmentDataManager> segmentDataManagers =
        tableDataManager.acquireSegments(segmentsToQuery, notAcquiredSegments);
    int numSegmentsAcquired = segmentDataManagers.size();
    List<IndexSegment> indexSegments = new ArrayList<>(numSegmentsAcquired);
    for (SegmentDataManager segmentDataManager : segmentDataManagers) {
      indexSegments.add(segmentDataManager.getSegment());
    }

    // Gather stats for realtime consuming segments
    int numConsumingSegmentsQueried = 0;
    int numOnlineSegments = 0;
    long minIndexTimeMs = 0;
    long minIngestionTimeMs = 0;
    long maxEndTimeMs = 0;
    if (tableDataManager instanceof RealtimeTableDataManager) {
      numConsumingSegmentsQueried = 0;
      numOnlineSegments = 0;
      minIndexTimeMs = Long.MAX_VALUE;
      minIngestionTimeMs = Long.MAX_VALUE;
      maxEndTimeMs = Long.MIN_VALUE;
      for (IndexSegment indexSegment : indexSegments) {
        if (indexSegment instanceof MutableSegment) {
          numConsumingSegmentsQueried += 1;
          SegmentMetadata segmentMetadata = indexSegment.getSegmentMetadata();
          long indexTimeMs = segmentMetadata.getLastIndexedTimestamp();
          if (indexTimeMs != Long.MIN_VALUE && indexTimeMs < minIndexTimeMs) {
            minIndexTimeMs = indexTimeMs;
          }
          long ingestionTimeMs = segmentMetadata.getLatestIngestionTimestamp();
          if (ingestionTimeMs != Long.MIN_VALUE && ingestionTimeMs < minIngestionTimeMs) {
            minIngestionTimeMs = ingestionTimeMs;
          }
        } else if (indexSegment instanceof ImmutableSegment) {
          SegmentMetadata segmentMetadata = indexSegment.getSegmentMetadata();
          long indexCreationTime = segmentMetadata.getIndexCreationTime();
          numOnlineSegments++;
          if (indexCreationTime != Long.MIN_VALUE) {
            maxEndTimeMs = Math.max(maxEndTimeMs, indexCreationTime);
          } else {
            // NOTE: the endTime may be totally inaccurate based on the value added in the timeColumn
            Interval timeInterval = segmentMetadata.getTimeInterval();
            if (timeInterval != null) {
              maxEndTimeMs = Math.max(maxEndTimeMs, timeInterval.getEndMillis());
            }
          }
        }
      }
    }

    InstanceResponseBlock instanceResponse = null;
    try {
      instanceResponse = executeInternal(indexSegments, queryContext, timerContext, executorService, responseObserver,
          queryRequest.isEnableStreaming());
    } catch (Exception e) {
      _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.QUERY_EXECUTION_EXCEPTIONS, 1);
      instanceResponse = new InstanceResponseBlock();
      // Do not log verbose error for BadQueryRequestException and QueryCancelledException.
      if (e instanceof BadQueryRequestException) {
        LOGGER.info("Caught BadQueryRequestException while processing requestId: {}, {}", requestId, e.getMessage());
        instanceResponse.addException(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
      } else if (e instanceof QueryCancelledException) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Cancelled while processing requestId: {}", requestId, e);
        } else {
          LOGGER.info("Cancelled while processing requestId: {}, {}", requestId, e.getMessage());
        }
        // NOTE most likely the onFailure() callback registered on query future in InstanceRequestHandler would
        // return the error table to broker sooner than here. But in case of race condition, we construct the error
        // table here too.
        instanceResponse.addException(QueryException.getException(QueryException.QUERY_CANCELLATION_ERROR,
            "Query cancelled on: " + _instanceDataManager.getInstanceId()));
      } else {
        LOGGER.error("Exception processing requestId {}", requestId, e);
        instanceResponse.addException(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
      }
    } finally {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
      if (queryRequest.isEnableTrace()) {
        if (TraceContext.traceEnabled() && instanceResponse != null) {
          instanceResponse.addMetadata(MetadataKey.TRACE_INFO.getName(), TraceContext.getTraceInfo());
        }
      }
    }

    queryProcessingTimer.stopAndRecord();
    long queryProcessingTime = queryProcessingTimer.getDurationMs();
    instanceResponse.addMetadata(MetadataKey.NUM_SEGMENTS_QUERIED.getName(), Integer.toString(numSegmentsAcquired));
    instanceResponse.addMetadata(MetadataKey.TIME_USED_MS.getName(), Long.toString(queryProcessingTime));

    // When segment is removed from the IdealState:
    // 1. Controller schedules a state transition to server to turn segment OFFLINE
    // 2. Server gets the state transition, removes the segment data manager and update its CurrentState
    // 3. Controller gathers the CurrentState and update the ExternalView
    // 4. Broker watches ExternalView change and updates the routing table to stop querying the segment
    //
    // After step 2 but before step 4, segment will be missing on server side
    // TODO: Change broker to watch both IdealState and ExternalView to not query the removed segments
    if (notAcquiredSegments.size() > 0) {
      List<String> missingSegments =
          notAcquiredSegments.stream().filter(segmentName -> !tableDataManager.isSegmentDeletedRecently(segmentName))
              .collect(Collectors.toList());
      int numMissingSegments = missingSegments.size();
      if (numMissingSegments > 0) {
        instanceResponse.addException(QueryException.getException(QueryException.SERVER_SEGMENT_MISSING_ERROR,
            String.format("%d segments %s missing on server: %s", numMissingSegments, missingSegments,
                _instanceDataManager.getInstanceId())));
        _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.NUM_MISSING_SEGMENTS, numMissingSegments);
      }
    }

    if (tableDataManager instanceof RealtimeTableDataManager) {
      long minConsumingFreshnessTimeMs;
      if (numConsumingSegmentsQueried > 0) {
        minConsumingFreshnessTimeMs = minIngestionTimeMs != Long.MAX_VALUE ? minIngestionTimeMs : minIndexTimeMs;
        instanceResponse.addMetadata(MetadataKey.NUM_CONSUMING_SEGMENTS_QUERIED.getName(),
            Integer.toString(numConsumingSegmentsQueried));
        instanceResponse.addMetadata(MetadataKey.MIN_CONSUMING_FRESHNESS_TIME_MS.getName(),
            Long.toString(minConsumingFreshnessTimeMs));
        LOGGER.debug("Request {} queried {} consuming segments with minConsumingFreshnessTimeMs: {}", requestId,
            numConsumingSegmentsQueried, minConsumingFreshnessTimeMs);
      } else if (numConsumingSegmentsQueried == 0 && maxEndTimeMs != Long.MIN_VALUE) {
        minConsumingFreshnessTimeMs = maxEndTimeMs;
        instanceResponse.addMetadata(MetadataKey.MIN_CONSUMING_FRESHNESS_TIME_MS.getName(),
            Long.toString(maxEndTimeMs));
        LOGGER.debug("Request {} queried {} consuming segments with minConsumingFreshnessTimeMs: {}", requestId,
            numConsumingSegmentsQueried, minConsumingFreshnessTimeMs);
      }
    }

    LOGGER.debug("Query processing time for request Id - {}: {}", requestId, queryProcessingTime);
    return instanceResponse;
  }

  // NOTE: This method might change indexSegments. Do not use it after calling this method.
  private InstanceResponseBlock executeInternal(List<IndexSegment> indexSegments, QueryContext queryContext,
      TimerContext timerContext, ExecutorService executorService,
      @Nullable StreamObserver<Server.ServerResponse> responseObserver, boolean enableStreaming)
      throws Exception {
    handleSubquery(queryContext, indexSegments, timerContext, executorService);

    // Compute total docs for the table before pruning the segments
    long numTotalDocs = 0;
    for (IndexSegment indexSegment : indexSegments) {
      numTotalDocs += indexSegment.getSegmentMetadata().getTotalDocs();
    }

    TimerContext.Timer segmentPruneTimer = timerContext.startNewPhaseTimer(ServerQueryPhase.SEGMENT_PRUNING);
    int numTotalSegments = indexSegments.size();
    SegmentPrunerStatistics prunerStats = new SegmentPrunerStatistics();
    List<IndexSegment> selectedSegments = _segmentPrunerService.prune(indexSegments, queryContext, prunerStats);
    segmentPruneTimer.stopAndRecord();
    int numSelectedSegments = selectedSegments.size();
    LOGGER.debug("Matched {} segments after pruning", numSelectedSegments);
    InstanceResponseBlock instanceResponse;
    if (numSelectedSegments == 0) {
      if (queryContext.isExplain()) {
        instanceResponse = getExplainResponseForNoMatchingSegment(numTotalSegments, queryContext);
      } else {
        instanceResponse =
            new InstanceResponseBlock(ResultsBlockUtils.buildEmptyQueryResults(queryContext), queryContext);
      }
    } else {
      TimerContext.Timer planBuildTimer = timerContext.startNewPhaseTimer(ServerQueryPhase.BUILD_QUERY_PLAN);
      Plan queryPlan =
          enableStreaming ? _planMaker.makeStreamingInstancePlan(selectedSegments, queryContext, executorService,
              responseObserver, _serverMetrics)
              : _planMaker.makeInstancePlan(selectedSegments, queryContext, executorService, _serverMetrics);
      planBuildTimer.stopAndRecord();

      TimerContext.Timer planExecTimer = timerContext.startNewPhaseTimer(ServerQueryPhase.QUERY_PLAN_EXECUTION);
      instanceResponse = queryContext.isExplain() ? executeExplainQuery(queryPlan, queryContext) : queryPlan.execute();
      planExecTimer.stopAndRecord();
    }

    // Update the total docs in the metadata based on the un-pruned segments
    instanceResponse.addMetadata(MetadataKey.TOTAL_DOCS.getName(), Long.toString(numTotalDocs));

    // Set the number of pruned segments. This count does not include the segments which returned empty filters
    int prunedSegments = numTotalSegments - numSelectedSegments;
    instanceResponse.addMetadata(MetadataKey.NUM_SEGMENTS_PRUNED_BY_SERVER.getName(), String.valueOf(prunedSegments));
    addPrunerStats(instanceResponse, prunerStats);

    return instanceResponse;
  }

  private static InstanceResponseBlock getExplainResponseForNoMatchingSegment(int numTotalSegments,
      QueryContext queryContext) {
    ExplainResultsBlock explainResults = new ExplainResultsBlock();
    explainResults.addOperator(String.format(ExplainPlanRows.PLAN_START_FORMAT, numTotalSegments),
        ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS);
    explainResults.addOperator(ExplainPlanRows.ALL_SEGMENTS_PRUNED_ON_SERVER, 3, 2);
    return new InstanceResponseBlock(explainResults, queryContext);
  }

  /**
   * Get a mapping of explain plan depth to a unique list of explain plans for each depth
   */
  private static Map<Integer, List<ExplainPlanRows>> getAllSegmentsUniqueExplainPlanRowData(Operator root) {
    Map<Integer, List<ExplainPlanRows>> operatorDepthToRowDataMap = new HashMap<>();
    if (root == null) {
      return operatorDepthToRowDataMap;
    }

    Map<Integer, HashSet<Integer>> uniquePlanNodeHashCodes = new HashMap<>();

    // Obtain the list of all possible segment plans after the combine root node
    List<Operator> children = root.getChildOperators();
    for (Operator child : children) {
      int[] operatorId = {3};
      ExplainPlanRows explainPlanRows = new ExplainPlanRows();
      // Get the segment explain plan for a single segment
      getSegmentExplainPlanRowData(child, explainPlanRows, operatorId, 2);
      int numRows = explainPlanRows.getExplainPlanRowData().size();
      if (numRows > 0) {
        operatorDepthToRowDataMap.putIfAbsent(numRows, new ArrayList<>());
        uniquePlanNodeHashCodes.putIfAbsent(numRows, new HashSet<>());
        int explainPlanRowsHashCode = explainPlanRows.hashCode();
        if (!uniquePlanNodeHashCodes.get(numRows).contains(explainPlanRowsHashCode)) {
          // If the hashcode of the explain plan rows returned for this segment is unique, add it to the data structure
          // and update the set of hashCodes
          explainPlanRows.incrementNumSegmentsMatchingThisPlan();
          operatorDepthToRowDataMap.get(numRows).add(explainPlanRows);
          uniquePlanNodeHashCodes.get(numRows).add(explainPlanRowsHashCode);
        } else {
          // If the hashCode for this segment isn't unique, find the explain plan with the matching hashCode and
          // increment the number of segments that match it provided the plan is the same.
          boolean explainPlanMatchFound = false;
          int operatorDepthToRowMapSize = operatorDepthToRowDataMap.get(numRows).size();
          for (int i = 0; i < operatorDepthToRowMapSize; i++) {
            ExplainPlanRows explainPlanRowsPotentialMatch = operatorDepthToRowDataMap.get(numRows).get(i);
            if ((explainPlanRowsPotentialMatch.hashCode() == explainPlanRowsHashCode)
                && (explainPlanRowsPotentialMatch.equals(explainPlanRows))) {
              explainPlanRowsPotentialMatch.incrementNumSegmentsMatchingThisPlan();
              explainPlanMatchFound = true;
              break;
            }
          }

          // HashCode can lead to collisions, which is why an equality check is required to ensure that we don't miss
          // any potential plans with matching hashcodes. If a match isn't found, add a new entry.
          if (!explainPlanMatchFound) {
            explainPlanRows.incrementNumSegmentsMatchingThisPlan();
            operatorDepthToRowDataMap.get(numRows).add(explainPlanRows);
          }
        }
      }
    }

    return operatorDepthToRowDataMap;
  }

  /**
   * Get the list of Explain Plan rows for a single segment
   */
  private static void getSegmentExplainPlanRowData(Operator node, ExplainPlanRows explainPlanRows, int[] globalId,
      int parentId) {
    if (node == null) {
      return;
    }

    String explainPlanString = node.toExplainString();
    if (explainPlanString != null) {
      ExplainPlanRowData explainPlanRowData = new ExplainPlanRowData(explainPlanString, globalId[0], parentId);
      parentId = globalId[0]++;
      explainPlanRows.appendExplainPlanRowData(explainPlanRowData);
      if (node instanceof EmptyFilterOperator) {
        explainPlanRows.setHasEmptyFilter(true);
      }
      if (node instanceof MatchAllFilterOperator) {
        explainPlanRows.setHasMatchAllFilter(true);
      }
    }

    List<Operator> children = node.getChildOperators();
    for (Operator child : children) {
      getSegmentExplainPlanRowData(child, explainPlanRows, globalId, parentId);
    }
  }

  public static InstanceResponseBlock executeExplainQuery(Plan queryPlan, QueryContext queryContext) {
    ExplainResultsBlock explainResults = new ExplainResultsBlock();
    List<Operator> childOperators = queryPlan.getPlanNode().run().getChildOperators();
    assert childOperators.size() == 1;
    Operator root = childOperators.get(0);
    Map<Integer, List<ExplainPlanRows>> operatorDepthToRowDataMap;
    int numEmptyFilterSegments = 0;
    int numMatchAllFilterSegments = 0;

    // Get the list of unique explain plans
    operatorDepthToRowDataMap = getAllSegmentsUniqueExplainPlanRowData(root);
    List<ExplainPlanRows> listOfExplainPlans = new ArrayList<>();
    operatorDepthToRowDataMap.forEach((key, value) -> listOfExplainPlans.addAll(value));

    // Setup the combine root's explain string
    explainResults.addOperator(root.toExplainString(), 2, 1);

    // Walk through all the explain plans and create the entries in the explain plan output for each plan
    for (ExplainPlanRows explainPlanRows : listOfExplainPlans) {
      numEmptyFilterSegments +=
          explainPlanRows.isHasEmptyFilter() ? explainPlanRows.getNumSegmentsMatchingThisPlan() : 0;
      numMatchAllFilterSegments +=
          explainPlanRows.isHasMatchAllFilter() ? explainPlanRows.getNumSegmentsMatchingThisPlan() : 0;
      explainResults.addOperator(
          String.format(ExplainPlanRows.PLAN_START_FORMAT, explainPlanRows.getNumSegmentsMatchingThisPlan()),
          ExplainPlanRows.PLAN_START_IDS, ExplainPlanRows.PLAN_START_IDS);
      for (ExplainPlanRowData explainPlanRowData : explainPlanRows.getExplainPlanRowData()) {
        explainResults.addOperator(explainPlanRowData.getExplainPlanString(), explainPlanRowData.getOperatorId(),
            explainPlanRowData.getParentId());
      }
    }

    InstanceResponseBlock instanceResponse = new InstanceResponseBlock(explainResults, queryContext);
    instanceResponse.addMetadata(MetadataKey.EXPLAIN_PLAN_NUM_EMPTY_FILTER_SEGMENTS.getName(),
        String.valueOf(numEmptyFilterSegments));
    instanceResponse.addMetadata(MetadataKey.EXPLAIN_PLAN_NUM_MATCH_ALL_FILTER_SEGMENTS.getName(),
        String.valueOf(numMatchAllFilterSegments));
    return instanceResponse;
  }

  /**
   * Handles the subquery in the given query.
   * <p>Currently only supports subquery within the filter.
   */
  private void handleSubquery(QueryContext queryContext, List<IndexSegment> indexSegments, TimerContext timerContext,
      ExecutorService executorService)
      throws Exception {
    FilterContext filter = queryContext.getFilter();
    if (filter != null) {
      handleSubquery(filter, indexSegments, timerContext, executorService, queryContext.getEndTimeMs());
    }
  }

  /**
   * Handles the subquery in the given filter.
   * <p>Currently only supports subquery within the lhs of the predicate.
   */
  private void handleSubquery(FilterContext filter, List<IndexSegment> indexSegments, TimerContext timerContext,
      ExecutorService executorService, long endTimeMs)
      throws Exception {
    List<FilterContext> children = filter.getChildren();
    if (children != null) {
      for (FilterContext child : children) {
        handleSubquery(child, indexSegments, timerContext, executorService, endTimeMs);
      }
    } else {
      handleSubquery(filter.getPredicate().getLhs(), indexSegments, timerContext, executorService, endTimeMs);
    }
  }

  /**
   * Handles the subquery in the given expression.
   * <p>When subquery is detected, first executes the subquery on the given segments and gets the response, then
   * rewrites the expression with the subquery response.
   * <p>Currently only supports ID_SET subquery within the IN_PARTITIONED_SUBQUERY transform function, which will be
   * rewritten to an IN_ID_SET transform function.
   */
  private void handleSubquery(ExpressionContext expression, List<IndexSegment> indexSegments, TimerContext timerContext,
      ExecutorService executorService, long endTimeMs)
      throws Exception {
    FunctionContext function = expression.getFunction();
    if (function == null) {
      return;
    }
    List<ExpressionContext> arguments = function.getArguments();
    if (StringUtils.remove(function.getFunctionName(), '_').equalsIgnoreCase(IN_PARTITIONED_SUBQUERY)) {
      Preconditions.checkArgument(arguments.size() == 2,
          "IN_PARTITIONED_SUBQUERY requires 2 arguments: expression, subquery");
      ExpressionContext subqueryExpression = arguments.get(1);
      Preconditions.checkState(subqueryExpression.getType() == ExpressionContext.Type.LITERAL,
          "Second argument of IN_PARTITIONED_SUBQUERY must be a literal (subquery)");
      QueryContext subquery = QueryContextConverterUtils.getQueryContext(subqueryExpression.getLiteralString());
      // Subquery should be an ID_SET aggregation only query
      //noinspection rawtypes
      AggregationFunction[] aggregationFunctions = subquery.getAggregationFunctions();
      Preconditions.checkArgument(aggregationFunctions != null && aggregationFunctions.length == 1
              && aggregationFunctions[0].getType() == AggregationFunctionType.IDSET
              && subquery.getGroupByExpressions() == null,
          "Subquery in IN_PARTITIONED_SUBQUERY should be an ID_SET aggregation only query, found: %s",
          subqueryExpression.toString());
      // Execute the subquery
      subquery.setEndTimeMs(endTimeMs);
      // Make a clone of indexSegments because the method might modify the list
      InstanceResponseBlock instanceResponse =
          executeInternal(new ArrayList<>(indexSegments), subquery, timerContext, executorService, null, false);
      BaseResultsBlock resultsBlock = instanceResponse.getResultsBlock();
      Preconditions.checkState(resultsBlock instanceof AggregationResultsBlock,
          "Got unexpected results block type: %s, expecting aggregation results",
          resultsBlock != null ? resultsBlock.getClass().getSimpleName() : null);
      Object result = ((AggregationResultsBlock) resultsBlock).getResults().get(0);
      Preconditions.checkState(result instanceof IdSet, "Got unexpected result type: %s, expecting IdSet",
          result != null ? result.getClass().getSimpleName() : null);
      // Rewrite the expression
      function.setFunctionName(TransformFunctionType.INIDSET.name());
      arguments.set(1,
          ExpressionContext.forLiteralContext(FieldSpec.DataType.STRING, ((IdSet) result).toBase64String()));
    } else {
      for (ExpressionContext argument : arguments) {
        handleSubquery(argument, indexSegments, timerContext, executorService, endTimeMs);
      }
    }
  }

  private void addPrunerStats(InstanceResponseBlock instanceResponse, SegmentPrunerStatistics prunerStats) {
    instanceResponse.addMetadata(MetadataKey.NUM_SEGMENTS_PRUNED_INVALID.getName(),
        String.valueOf(prunerStats.getInvalidSegments()));
    instanceResponse.addMetadata(MetadataKey.NUM_SEGMENTS_PRUNED_BY_LIMIT.getName(),
        String.valueOf(prunerStats.getLimitPruned()));
    instanceResponse.addMetadata(MetadataKey.NUM_SEGMENTS_PRUNED_BY_VALUE.getName(),
        String.valueOf(prunerStats.getValuePruned()));
  }
}
