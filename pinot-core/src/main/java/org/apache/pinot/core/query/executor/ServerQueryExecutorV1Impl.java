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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerQueryPhase;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.common.utils.DataTable.MetadataKey;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableUtils;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.plan.Plan;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.config.QueryExecutorConfig;
import org.apache.pinot.core.query.pruner.SegmentPrunerService;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.TimerContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.query.utils.idset.IdSet;
import org.apache.pinot.core.util.QueryOptions;
import org.apache.pinot.core.util.trace.TraceContext;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ThreadSafe
public class ServerQueryExecutorV1Impl implements QueryExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerQueryExecutorV1Impl.class);
  private static final String IN_PARTITIONED_SUBQUERY = "inPartitionedSubquery";

  private InstanceDataManager _instanceDataManager;
  private SegmentPrunerService _segmentPrunerService;
  private PlanMaker _planMaker;
  private long _defaultTimeOutMs = CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS;
  private ServerMetrics _serverMetrics;

  @Override
  public synchronized void init(PinotConfiguration config, InstanceDataManager instanceDataManager,
      ServerMetrics serverMetrics)
      throws ConfigurationException {
    _instanceDataManager = instanceDataManager;
    _serverMetrics = serverMetrics;
    QueryExecutorConfig queryExecutorConfig = new QueryExecutorConfig(config);
    if (queryExecutorConfig.getTimeOut() > 0) {
      _defaultTimeOutMs = queryExecutorConfig.getTimeOut();
    }
    LOGGER.info("Default timeout for query executor : {}", _defaultTimeOutMs);
    LOGGER.info("Trying to build SegmentPrunerService");
    _segmentPrunerService = new SegmentPrunerService(queryExecutorConfig.getPrunerConfig());
    LOGGER.info("Trying to build QueryPlanMaker");
    _planMaker = new InstancePlanMakerImplV2(queryExecutorConfig);
    LOGGER.info("Trying to build QueryExecutorTimer");
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
  public DataTable processQuery(ServerQueryRequest queryRequest, ExecutorService executorService,
      @Nullable StreamObserver<Server.ServerResponse> responseObserver) {
    TimerContext timerContext = queryRequest.getTimerContext();
    TimerContext.Timer schedulerWaitTimer = timerContext.getPhaseTimer(ServerQueryPhase.SCHEDULER_WAIT);
    if (schedulerWaitTimer != null) {
      schedulerWaitTimer.stopAndRecord();
    }
    long queryArrivalTimeMs = timerContext.getQueryArrivalTimeMs();
    long querySchedulingTimeMs = System.currentTimeMillis() - queryArrivalTimeMs;
    TimerContext.Timer queryProcessingTimer = timerContext.startNewPhaseTimer(ServerQueryPhase.QUERY_PROCESSING);

    long requestId = queryRequest.getRequestId();
    String tableNameWithType = queryRequest.getTableNameWithType();
    QueryContext queryContext = queryRequest.getQueryContext();
    LOGGER.debug("Incoming request Id: {}, query: {}", requestId, queryContext);
    // Use the timeout passed from the request if exists, or the instance-level timeout
    long queryTimeoutMs = _defaultTimeOutMs;
    Map<String, String> queryOptions = queryContext.getQueryOptions();
    if (queryOptions != null) {
      Long timeoutFromQueryOptions = QueryOptions.getTimeoutMs(queryOptions);
      if (timeoutFromQueryOptions != null) {
        queryTimeoutMs = timeoutFromQueryOptions;
      }
    }

    // Query scheduler wait time already exceeds query timeout, directly return
    if (querySchedulingTimeMs >= queryTimeoutMs) {
      _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.SCHEDULING_TIMEOUT_EXCEPTIONS, 1);
      String errorMessage = String
          .format("Query scheduling took %dms (longer than query timeout of %dms)", querySchedulingTimeMs,
              queryTimeoutMs);
      DataTable dataTable = DataTableBuilder.getEmptyDataTable();
      dataTable.addException(QueryException.getException(QueryException.QUERY_SCHEDULING_TIMEOUT_ERROR, errorMessage));
      LOGGER.error("{} while processing requestId: {}", errorMessage, requestId);
      return dataTable;
    }

    TableDataManager tableDataManager = _instanceDataManager.getTableDataManager(tableNameWithType);
    if (tableDataManager == null) {
      String errorMessage = "Failed to find table: " + tableNameWithType;
      DataTable dataTable = DataTableBuilder.getEmptyDataTable();
      dataTable.addException(QueryException.getException(QueryException.SERVER_TABLE_MISSING_ERROR, errorMessage));
      LOGGER.error("{} while processing requestId: {}", errorMessage, requestId);
      return dataTable;
    }

    List<String> segmentsToQuery = queryRequest.getSegmentsToQuery();
    List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireSegments(segmentsToQuery);

    // When segment is removed from the IdealState:
    // 1. Controller schedules a state transition to server to turn segment OFFLINE
    // 2. Server gets the state transition, removes the segment data manager and update its CurrentState
    // 3. Controller gathers the CurrentState and update the ExternalView
    // 4. Broker watches ExternalView change and updates the routing table to stop querying the segment
    //
    // After step 2 but before step 4, segment will be missing on server side
    // TODO: Change broker to watch both IdealState and ExternalView to not query the removed segments
    int numSegmentsQueried = segmentsToQuery.size();
    int numSegmentsAcquired = segmentDataManagers.size();
    if (numSegmentsQueried > numSegmentsAcquired) {
      _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.NUM_MISSING_SEGMENTS,
          numSegmentsQueried - numSegmentsAcquired);
    }
    List<IndexSegment> indexSegments = new ArrayList<>(numSegmentsAcquired);
    for (SegmentDataManager segmentDataManager : segmentDataManagers) {
      indexSegments.add(segmentDataManager.getSegment());
    }

    // Gather stats for realtime consuming segments
    int numConsumingSegmentsProcessed = 0;
    long minIndexTimeMs = Long.MAX_VALUE;
    long minIngestionTimeMs = Long.MAX_VALUE;
    for (IndexSegment indexSegment : indexSegments) {
      if (indexSegment instanceof MutableSegment) {
        numConsumingSegmentsProcessed += 1;
        SegmentMetadata segmentMetadata = indexSegment.getSegmentMetadata();
        long indexTimeMs = segmentMetadata.getLastIndexedTimestamp();
        if (indexTimeMs != Long.MIN_VALUE && indexTimeMs < minIndexTimeMs) {
          minIndexTimeMs = indexTimeMs;
        }
        long ingestionTimeMs = segmentMetadata.getLatestIngestionTimestamp();
        if (ingestionTimeMs != Long.MIN_VALUE && ingestionTimeMs < minIngestionTimeMs) {
          minIngestionTimeMs = ingestionTimeMs;
        }
      }
    }

    long minConsumingFreshnessTimeMs = minIngestionTimeMs;
    if (numConsumingSegmentsProcessed > 0) {
      if (minIngestionTimeMs == Long.MAX_VALUE) {
        LOGGER.debug("Did not find valid ingestionTimestamp across consuming segments! Using indexTime instead");
        minConsumingFreshnessTimeMs = minIndexTimeMs;
      }
      LOGGER
          .debug("Querying: {} consuming segments with minConsumingFreshnessTimeMs: {}", numConsumingSegmentsProcessed,
              minConsumingFreshnessTimeMs);
    }

    boolean enableTrace = queryRequest.isEnableTrace();
    if (enableTrace) {
      TraceContext.register(requestId);
    }

    DataTable dataTable = null;
    try {
      dataTable = processQuery(indexSegments, queryContext, timerContext, executorService, responseObserver,
          queryArrivalTimeMs + queryTimeoutMs, queryRequest.isEnableStreaming());
    } catch (Exception e) {
      _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.QUERY_EXECUTION_EXCEPTIONS, 1);

      // Do not log error for BadQueryRequestException because it's caused by bad query
      if (e instanceof BadQueryRequestException) {
        LOGGER.info("Caught BadQueryRequestException while processing requestId: {}, {}", requestId, e.getMessage());
      } else {
        LOGGER.error("Exception processing requestId {}", requestId, e);
      }

      dataTable = DataTableBuilder.getEmptyDataTable();
      dataTable.addException(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
    } finally {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
      if (enableTrace) {
        if (dataTable != null) {
          dataTable.getMetadata().put(MetadataKey.TRACE_INFO.getName(), TraceContext.getTraceInfo());
        }
        TraceContext.unregister();
      }
    }

    queryProcessingTimer.stopAndRecord();
    long queryProcessingTime = queryProcessingTimer.getDurationMs();
    dataTable.getMetadata().put(MetadataKey.NUM_SEGMENTS_QUERIED.getName(), Integer.toString(numSegmentsQueried));
    dataTable.getMetadata().put(MetadataKey.TIME_USED_MS.getName(), Long.toString(queryProcessingTime));

    if (numConsumingSegmentsProcessed > 0) {
      dataTable.getMetadata()
          .put(MetadataKey.NUM_CONSUMING_SEGMENTS_PROCESSED.getName(), Integer.toString(numConsumingSegmentsProcessed));
      dataTable.getMetadata()
          .put(MetadataKey.MIN_CONSUMING_FRESHNESS_TIME_MS.getName(), Long.toString(minConsumingFreshnessTimeMs));
    }

    LOGGER.debug("Query processing time for request Id - {}: {}", requestId, queryProcessingTime);
    LOGGER.debug("InstanceResponse for request Id - {}: {}", requestId, dataTable);
    return dataTable;
  }

  private DataTable processQuery(List<IndexSegment> indexSegments, QueryContext queryContext, TimerContext timerContext,
      ExecutorService executorService, @Nullable StreamObserver<Server.ServerResponse> responseObserver, long endTimeMs,
      boolean enableStreaming)
      throws Exception {
    handleSubquery(queryContext, indexSegments, timerContext, executorService, endTimeMs);

    // Compute total docs for the table before pruning the segments
    long numTotalDocs = 0;
    for (IndexSegment indexSegment : indexSegments) {
      numTotalDocs += indexSegment.getSegmentMetadata().getTotalDocs();
    }

    TimerContext.Timer segmentPruneTimer = timerContext.startNewPhaseTimer(ServerQueryPhase.SEGMENT_PRUNING);
    List<IndexSegment> selectedSegments = _segmentPrunerService.prune(indexSegments, queryContext);
    segmentPruneTimer.stopAndRecord();
    int numSelectedSegments = selectedSegments.size();
    LOGGER.debug("Matched {} segments after pruning", numSelectedSegments);
    if (numSelectedSegments == 0) {
      // Only return metadata for streaming query
      DataTable dataTable =
          enableStreaming ? DataTableBuilder.getEmptyDataTable() : DataTableUtils.buildEmptyDataTable(queryContext);
      Map<String, String> metadata = dataTable.getMetadata();
      metadata.put(MetadataKey.TOTAL_DOCS.getName(), String.valueOf(numTotalDocs));
      metadata.put(MetadataKey.NUM_DOCS_SCANNED.getName(), "0");
      metadata.put(MetadataKey.NUM_ENTRIES_SCANNED_IN_FILTER.getName(), "0");
      metadata.put(MetadataKey.NUM_ENTRIES_SCANNED_POST_FILTER.getName(), "0");
      metadata.put(MetadataKey.NUM_SEGMENTS_PROCESSED.getName(), "0");
      metadata.put(MetadataKey.NUM_SEGMENTS_MATCHED.getName(), "0");
      return dataTable;
    } else {
      TimerContext.Timer planBuildTimer = timerContext.startNewPhaseTimer(ServerQueryPhase.BUILD_QUERY_PLAN);
      Plan queryPlan = enableStreaming ? _planMaker
          .makeStreamingInstancePlan(selectedSegments, queryContext, executorService, responseObserver, endTimeMs)
          : _planMaker.makeInstancePlan(selectedSegments, queryContext, executorService, endTimeMs);
      planBuildTimer.stopAndRecord();

      TimerContext.Timer planExecTimer = timerContext.startNewPhaseTimer(ServerQueryPhase.QUERY_PLAN_EXECUTION);
      DataTable dataTable = queryPlan.execute();
      planExecTimer.stopAndRecord();

      // Update the total docs in the metadata based on the un-pruned segments
      dataTable.getMetadata().put(MetadataKey.TOTAL_DOCS.getName(), Long.toString(numTotalDocs));

      return dataTable;
    }
  }

  /**
   * Handles the subquery in the given query.
   * <p>Currently only supports subquery within the filter.
   */
  private void handleSubquery(QueryContext queryContext, List<IndexSegment> indexSegments, TimerContext timerContext,
      ExecutorService executorService, long endTimeMs)
      throws Exception {
    FilterContext filter = queryContext.getFilter();
    if (filter != null) {
      handleSubquery(filter, indexSegments, timerContext, executorService, endTimeMs);
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
      Preconditions
          .checkState(arguments.size() == 2, "IN_PARTITIONED_SUBQUERY requires 2 arguments: expression, subquery");
      ExpressionContext subqueryExpression = arguments.get(1);
      Preconditions.checkState(subqueryExpression.getType() == ExpressionContext.Type.LITERAL,
          "Second argument of IN_PARTITIONED_SUBQUERY must be a literal (subquery)");
      QueryContext subquery = QueryContextConverterUtils.getQueryContextFromSQL(subqueryExpression.getLiteral());
      // Subquery should be an ID_SET aggregation only query
      //noinspection rawtypes
      AggregationFunction[] aggregationFunctions = subquery.getAggregationFunctions();
      Preconditions.checkState(aggregationFunctions != null && aggregationFunctions.length == 1
              && aggregationFunctions[0].getType() == AggregationFunctionType.IDSET
              && subquery.getGroupByExpressions() == null,
          "Subquery in IN_PARTITIONED_SUBQUERY should be an ID_SET aggregation only query, found: %s",
          subqueryExpression.getLiteral());
      // Execute the subquery
      DataTable dataTable =
          processQuery(indexSegments, subquery, timerContext, executorService, null, endTimeMs, false);
      IdSet idSet = dataTable.getObject(0, 0);
      String serializedIdSet = idSet.toBase64String();
      // Rewrite the expression
      function.setFunctionName(TransformFunctionType.INIDSET.name());
      arguments.set(1, ExpressionContext.forLiteral(serializedIdSet));
    } else {
      for (ExpressionContext argument : arguments) {
        handleSubquery(argument, indexSegments, timerContext, executorService, endTimeMs);
      }
    }
  }
}
