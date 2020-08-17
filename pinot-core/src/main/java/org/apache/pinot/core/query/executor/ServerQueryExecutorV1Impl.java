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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerQueryPhase;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.datatable.DataTableImplV2;
import org.apache.pinot.core.common.datatable.DataTableUtils;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.core.data.manager.TableDataManager;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.mutable.MutableSegment;
import org.apache.pinot.core.plan.Plan;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.config.QueryExecutorConfig;
import org.apache.pinot.core.query.exception.BadQueryRequestException;
import org.apache.pinot.core.query.pruner.SegmentPrunerService;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.TimerContext;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadata;
import org.apache.pinot.core.util.QueryOptions;
import org.apache.pinot.core.util.trace.TraceContext;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ThreadSafe
public class ServerQueryExecutorV1Impl implements QueryExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerQueryExecutorV1Impl.class);

  private InstanceDataManager _instanceDataManager = null;
  private SegmentPrunerService _segmentPrunerService = null;
  private PlanMaker _planMaker = null;
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
  public DataTable processQuery(ServerQueryRequest queryRequest, ExecutorService executorService) {
    TimerContext timerContext = queryRequest.getTimerContext();
    TimerContext.Timer schedulerWaitTimer = timerContext.getPhaseTimer(ServerQueryPhase.SCHEDULER_WAIT);
    if (schedulerWaitTimer != null) {
      schedulerWaitTimer.stopAndRecord();
    }
    long querySchedulingTimeMs = System.currentTimeMillis() - timerContext.getQueryArrivalTimeMs();
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
    long remainingTimeMs = queryTimeoutMs - querySchedulingTimeMs;

    // Query scheduler wait time already exceeds query timeout, directly return
    if (remainingTimeMs <= 0) {
      _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.SCHEDULING_TIMEOUT_EXCEPTIONS, 1);
      String errorMessage = String
          .format("Query scheduling took %dms (longer than query timeout of %dms)", querySchedulingTimeMs,
              queryTimeoutMs);
      DataTable dataTable = new DataTableImplV2();
      dataTable.addException(QueryException.getException(QueryException.QUERY_SCHEDULING_TIMEOUT_ERROR, errorMessage));
      LOGGER.error("{} while processing requestId: {}", errorMessage, requestId);
      return dataTable;
    }

    TableDataManager tableDataManager = _instanceDataManager.getTableDataManager(tableNameWithType);
    Preconditions.checkState(tableDataManager != null, "Failed to find data manager for table: " + tableNameWithType);

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

    boolean enableTrace = queryRequest.isEnableTrace();
    if (enableTrace) {
      TraceContext.register(requestId);
    }

    int numConsumingSegmentsProcessed = 0;
    long minIndexTimeMs = Long.MAX_VALUE;
    long minIngestionTimeMs = Long.MAX_VALUE;
    // gather stats for realtime consuming segments
    for (SegmentDataManager segmentMgr : segmentDataManagers) {
      if (segmentMgr.getSegment() instanceof MutableSegment) {
        numConsumingSegmentsProcessed += 1;
        SegmentMetadata metadata = segmentMgr.getSegment().getSegmentMetadata();
        long indexedTime = metadata.getLastIndexedTimestamp();
        if (indexedTime != Long.MIN_VALUE && indexedTime < minIndexTimeMs) {
          minIndexTimeMs = metadata.getLastIndexedTimestamp();
        }
        long ingestionTime = metadata.getLatestIngestionTimestamp();
        if (ingestionTime != Long.MIN_VALUE && ingestionTime < minIngestionTimeMs) {
          minIngestionTimeMs = ingestionTime;
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

    DataTable dataTable = null;
    try {
      // Compute total docs for the table before pruning the segments
      long numTotalDocs = 0;
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        numTotalDocs += segmentDataManager.getSegment().getSegmentMetadata().getTotalDocs();
      }
      TimerContext.Timer segmentPruneTimer = timerContext.startNewPhaseTimer(ServerQueryPhase.SEGMENT_PRUNING);
      segmentDataManagers = _segmentPrunerService.prune(tableDataManager, segmentDataManagers, queryRequest);
      segmentPruneTimer.stopAndRecord();
      int numSegmentsMatchedAfterPruning = segmentDataManagers.size();
      LOGGER.debug("Matched {} segments after pruning", numSegmentsMatchedAfterPruning);
      if (numSegmentsMatchedAfterPruning == 0) {
        dataTable = DataTableUtils.buildEmptyDataTable(queryContext);
        Map<String, String> metadata = dataTable.getMetadata();
        metadata.put(DataTable.TOTAL_DOCS_METADATA_KEY, String.valueOf(numTotalDocs));
        metadata.put(DataTable.NUM_DOCS_SCANNED_METADATA_KEY, "0");
        metadata.put(DataTable.NUM_ENTRIES_SCANNED_IN_FILTER_METADATA_KEY, "0");
        metadata.put(DataTable.NUM_ENTRIES_SCANNED_POST_FILTER_METADATA_KEY, "0");
        metadata.put(DataTable.NUM_SEGMENTS_PROCESSED, "0");
        metadata.put(DataTable.NUM_SEGMENTS_MATCHED, "0");
      } else {
        TimerContext.Timer planBuildTimer = timerContext.startNewPhaseTimer(ServerQueryPhase.BUILD_QUERY_PLAN);
        List<IndexSegment> indexSegments = new ArrayList<>(numSegmentsMatchedAfterPruning);
        for (SegmentDataManager segmentDataManager : segmentDataManagers) {
          indexSegments.add(segmentDataManager.getSegment());
        }
        Plan globalQueryPlan =
            _planMaker.makeInstancePlan(indexSegments, queryContext, executorService, remainingTimeMs);
        planBuildTimer.stopAndRecord();

        TimerContext.Timer planExecTimer = timerContext.startNewPhaseTimer(ServerQueryPhase.QUERY_PLAN_EXECUTION);
        dataTable = globalQueryPlan.execute();
        planExecTimer.stopAndRecord();

        // Update the total docs in the metadata based on un-pruned segments.
        dataTable.getMetadata().put(DataTable.TOTAL_DOCS_METADATA_KEY, Long.toString(numTotalDocs));
      }
    } catch (Exception e) {
      _serverMetrics.addMeteredTableValue(tableNameWithType, ServerMeter.QUERY_EXECUTION_EXCEPTIONS, 1);

      // Do not log error for BadQueryRequestException because it's caused by bad query
      if (e instanceof BadQueryRequestException) {
        LOGGER.info("Caught BadQueryRequestException while processing requestId: {}, {}", requestId, e.getMessage());
      } else {
        LOGGER.error("Exception processing requestId {}", requestId, e);
      }

      dataTable = new DataTableImplV2();
      dataTable.addException(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
    } finally {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
      if (enableTrace) {
        if (dataTable != null) {
          dataTable.getMetadata().put(DataTable.TRACE_INFO_METADATA_KEY, TraceContext.getTraceInfo());
        }
        TraceContext.unregister();
      }
    }

    queryProcessingTimer.stopAndRecord();
    long queryProcessingTime = queryProcessingTimer.getDurationMs();
    dataTable.getMetadata().put(DataTable.NUM_SEGMENTS_QUERIED, Integer.toString(numSegmentsQueried));
    dataTable.getMetadata().put(DataTable.TIME_USED_MS_METADATA_KEY, Long.toString(queryProcessingTime));

    if (numConsumingSegmentsProcessed > 0) {
      dataTable.getMetadata()
          .put(DataTable.NUM_CONSUMING_SEGMENTS_PROCESSED, Integer.toString(numConsumingSegmentsProcessed));
      dataTable.getMetadata()
          .put(DataTable.MIN_CONSUMING_FRESHNESS_TIME_MS, Long.toString(minConsumingFreshnessTimeMs));
    }

    LOGGER.debug("Query processing time for request Id - {}: {}", requestId, queryProcessingTime);
    LOGGER.debug("InstanceResponse for request Id - {}: {}", requestId, dataTable);
    return dataTable;
  }
}
