/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.executor;

import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.metrics.ServerMeter;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.metrics.ServerQueryPhase;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.query.ServerQueryRequest;
import com.linkedin.pinot.common.query.context.TimerContext;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.common.datatable.DataTableBuilder;
import com.linkedin.pinot.core.common.datatable.DataTableImplV2;
import com.linkedin.pinot.core.data.manager.offline.InstanceDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.Plan;
import com.linkedin.pinot.core.plan.maker.InstancePlanMakerImplV2;
import com.linkedin.pinot.core.plan.maker.PlanMaker;
import com.linkedin.pinot.core.query.config.QueryExecutorConfig;
import com.linkedin.pinot.core.query.exception.BadQueryRequestException;
import com.linkedin.pinot.core.query.pruner.SegmentPrunerService;
import com.linkedin.pinot.core.query.pruner.SegmentPrunerServiceImpl;
import com.linkedin.pinot.core.util.trace.TraceContext;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.parsing.combinator.token.StdTokens;


public class ServerQueryExecutorV1Impl implements QueryExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerQueryExecutorV1Impl.class);

  private InstanceDataManager _instanceDataManager = null;
  private SegmentPrunerService _segmentPrunerService = null;
  private PlanMaker _planMaker = null;
  private volatile boolean _isStarted = false;
  private long _defaultTimeOutMs = 15000;
  private boolean _printQueryPlan = false;
  private final Map<String, Long> _resourceTimeOutMsMap = new ConcurrentHashMap<>();
  private ServerMetrics _serverMetrics;

  public ServerQueryExecutorV1Impl() {
  }

  public ServerQueryExecutorV1Impl(boolean printQueryPlan) {
    _printQueryPlan = printQueryPlan;
  }

  @Override
  public void init(Configuration configuration, DataManager dataManager, ServerMetrics serverMetrics)
      throws ConfigurationException {
    _serverMetrics = serverMetrics;
    _instanceDataManager = (InstanceDataManager) dataManager;
    QueryExecutorConfig queryExecutorConfig = new QueryExecutorConfig(configuration);
    if (queryExecutorConfig.getTimeOut() > 0) {
      _defaultTimeOutMs = queryExecutorConfig.getTimeOut();
    }
    LOGGER.info("Default timeout for query executor : {}", _defaultTimeOutMs);
    LOGGER.info("Trying to build SegmentPrunerService");
    _segmentPrunerService = new SegmentPrunerServiceImpl(queryExecutorConfig.getPrunerConfig());
    LOGGER.info("Trying to build QueryPlanMaker");
    _planMaker = new InstancePlanMakerImplV2(queryExecutorConfig);
    LOGGER.info("Trying to build QueryExecutorTimer");
  }

  @Override
  public DataTable processQuery(final ServerQueryRequest queryRequest, ExecutorService executorService) {
    TimerContext timerContext = queryRequest.getTimerContext();
    TimerContext.Timer schedulerWaitTimer = timerContext.getPhaseTimer(ServerQueryPhase.SCHEDULER_WAIT);
    if (schedulerWaitTimer != null) {
      schedulerWaitTimer.stopAndRecord();
    }

    ThreadMXBean bean = ManagementFactory.getThreadMXBean( );
    long tid = Thread.currentThread().getId();

    long startCpuTime = 0;
    if(bean.isCurrentThreadCpuTimeSupported())
    {
      startCpuTime = bean.getCurrentThreadCpuTime();
    }


    TimerContext.Timer queryProcessingTimer = timerContext.startNewPhaseTimer(ServerQueryPhase.QUERY_PROCESSING);

    DataTable dataTable;
    List<SegmentDataManager> queryableSegmentDataManagerList = null;
    InstanceRequest instanceRequest = queryRequest.getInstanceRequest();
    final long requestId = instanceRequest.getRequestId();

    try {
      TraceContext.register(instanceRequest);
      final BrokerRequest brokerRequest = instanceRequest.getQuery();
      LOGGER.debug("Incoming query is : {}", brokerRequest);

      TimerContext.Timer segmentPruneTimer = timerContext.startNewPhaseTimer(ServerQueryPhase.SEGMENT_PRUNING);

      final String tableName = instanceRequest.getQuery().getQuerySource().getTableName();
      TableDataManager tableDataManager = _instanceDataManager.getTableDataManager(tableName);
      queryableSegmentDataManagerList = acquireQueryableSegments(tableDataManager, instanceRequest);
      long totalRawDocs = pruneSegments(tableDataManager, queryableSegmentDataManagerList, queryRequest);
      segmentPruneTimer.stopAndRecord();

      int numSegmentsMatched = queryableSegmentDataManagerList.size();
      List<String> selectedSegments = new ArrayList<>();
      for(int i=0;i<numSegmentsMatched;i++)
      {
        selectedSegments.add(queryableSegmentDataManagerList.get(i).getSegmentName());
      }
      queryRequest.setSegmentsAfterPruning(selectedSegments);


      queryRequest.setSegmentCountAfterPruning(numSegmentsMatched);
      LOGGER.debug("Matched {} segments", numSegmentsMatched);
      if (numSegmentsMatched == 0) {
        DataTable emptyDataTable = DataTableBuilder.buildEmptyDataTable(brokerRequest);
        emptyDataTable.getMetadata().put(DataTable.TOTAL_DOCS_METADATA_KEY, String.valueOf(totalRawDocs));

        // Stop and record the query processing timer for early bailout.
        queryProcessingTimer.stopAndRecord();
        return emptyDataTable;
      }

      TimerContext.Timer planBuildTimer = timerContext.startNewPhaseTimer(ServerQueryPhase.BUILD_QUERY_PLAN);
      final Plan globalQueryPlan =
          _planMaker.makeInterSegmentPlan(queryableSegmentDataManagerList, brokerRequest, executorService,
              getResourceTimeOut(instanceRequest.getQuery()));
      planBuildTimer.stopAndRecord();

      if (_printQueryPlan) {
        LOGGER.debug("***************************** Query Plan for Request {} ***********************************",
            instanceRequest.getRequestId());
        globalQueryPlan.print();
        LOGGER.debug("*********************************** End Query Plan ***********************************");
      }

      TimerContext.Timer planExecTimer = timerContext.startNewPhaseTimer(ServerQueryPhase.QUERY_PLAN_EXECUTION);
      globalQueryPlan.execute();
      planExecTimer.stopAndRecord();

      dataTable = globalQueryPlan.getInstanceResponse();
      Map<String, String> dataTableMetadata = dataTable.getMetadata();
      queryProcessingTimer.stopAndRecord();

      LOGGER.debug("Searching Instance for Request Id - {}, browse took: {}", instanceRequest.getRequestId(),
          queryProcessingTimer.getDurationNs());
      LOGGER.debug("InstanceResponse for Request Id - {} : {}", instanceRequest.getRequestId(), dataTable.toString());
      dataTableMetadata.put(DataTable.TIME_USED_MS_METADATA_KEY, Long.toString(queryProcessingTimer.getDurationMs()));
      dataTableMetadata.put(DataTable.REQUEST_ID_METADATA_KEY, Long.toString(instanceRequest.getRequestId()));
      dataTableMetadata.put(DataTable.TRACE_INFO_METADATA_KEY,
          TraceContext.getTraceInfoOfRequestId(instanceRequest.getRequestId()));

      // Update the total docs in the metadata based on un-pruned segments.
      dataTableMetadata.put(DataTable.TOTAL_DOCS_METADATA_KEY, String.valueOf(totalRawDocs));


      long cpuTime = 0;
      if(bean.isCurrentThreadCpuTimeSupported())
      {
        long endCpuTime = bean.getCurrentThreadCpuTime();
        if(endCpuTime >= startCpuTime)
        {
          cpuTime = endCpuTime-startCpuTime;
        }
        else
        {
          cpuTime = Long.MIN_VALUE - startCpuTime;
          cpuTime += endCpuTime;
        }

      }
      else
      {
       cpuTime = -1;
      }
      dataTableMetadata.put(DataTable.EXECUTOR_CPU_TIME,Long.toString(cpuTime));

      return dataTable;
    } catch (Exception e) {
      _serverMetrics.addMeteredQueryValue(instanceRequest.getQuery(), ServerMeter.QUERY_EXECUTION_EXCEPTIONS, 1);

      // Do not log error for BadQueryRequestException because it's caused by bad query
      if (e instanceof BadQueryRequestException) {
        LOGGER.info("Caught BadQueryRequestException while processing requestId: {}, {}", requestId, e.getMessage());
      } else {
        LOGGER.error("Exception processing requestId {}", requestId, e);
      }

      dataTable = new DataTableImplV2();
      Map<String, String> dataTableMetadata = dataTable.getMetadata();
      dataTable.addException(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
      TraceContext.logException("ServerQueryExecutorV1Impl", "Exception occurs in processQuery");
      queryProcessingTimer.stopAndRecord();

      LOGGER.info("Searching Instance for Request Id - {}, browse took: {}, instanceResponse: {}", requestId,
          queryProcessingTimer.getDurationMs(), dataTable.toString());
      dataTableMetadata.put(DataTable.TIME_USED_MS_METADATA_KEY, Long.toString(queryProcessingTimer.getDurationNs()));
      dataTableMetadata.put(DataTable.REQUEST_ID_METADATA_KEY, Long.toString(instanceRequest.getRequestId()));
      dataTableMetadata.put(DataTable.TRACE_INFO_METADATA_KEY,
          TraceContext.getTraceInfoOfRequestId(instanceRequest.getRequestId()));
      return dataTable;
    } finally {
      TableDataManager tableDataManager = _instanceDataManager.getTableDataManager(queryRequest.getTableName());
      if (tableDataManager != null && queryableSegmentDataManagerList != null) {
        for (SegmentDataManager segmentDataManager : queryableSegmentDataManagerList) {
          tableDataManager.releaseSegment(segmentDataManager);
        }
      }
      TraceContext.unregister(instanceRequest);
    }
  }

  /**
   * Helper method to identify if a query is simple count(*) query without any predicates and group by's.
   *
   * @param brokerRequest Broker request for the query
   * @return True if simple count star query, false otherwise.
   */

  /**
   * Helper method to acquire segments that can be queried for the request.
   *
   * @param tableDataManager Table data manager
   * @param instanceRequest Instance request
   * @return List of segment data managers that can be queried.
   */
  private List<SegmentDataManager> acquireQueryableSegments(TableDataManager tableDataManager,
      final InstanceRequest instanceRequest) {
    LOGGER.debug("InstanceRequest contains {} segments", instanceRequest.getSearchSegmentsSize());

    if (tableDataManager == null || instanceRequest.getSearchSegmentsSize() == 0) {
      return new ArrayList<>();
    }
    List<SegmentDataManager> listOfQueryableSegments =
        tableDataManager.acquireSegments(instanceRequest.getSearchSegments());
    LOGGER.debug("TableDataManager found {} segments before pruning", listOfQueryableSegments.size());
    return listOfQueryableSegments;
  }

  /**
   * Helper method to prune segments.
   *
   * @param tableDataManager Table data manager
   * @param segments List of segments to prune
   * @param serverQueryRequest Server query request
   * @return Total number of docs across all segments (including the ones that were pruned).
   */
  private long pruneSegments(TableDataManager tableDataManager, List<SegmentDataManager> segments,
      ServerQueryRequest serverQueryRequest) {
    long totalRawDocs = 0;
    Iterator<SegmentDataManager> it = segments.iterator();

    while (it.hasNext()) {
      SegmentDataManager segmentDataManager = it.next();
      final IndexSegment indexSegment = segmentDataManager.getSegment();
      // We need to compute the total raw docs for the table before any pruning.
      totalRawDocs += indexSegment.getSegmentMetadata().getTotalRawDocs();
      if (_segmentPrunerService.prune(indexSegment, serverQueryRequest)) {
        it.remove();
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }
    return totalRawDocs;
  }

  @Override
  public synchronized void shutDown() {
    if (isStarted()) {
      _isStarted = false;
      LOGGER.info("QueryExecutor is shutDown!");
    } else {
      LOGGER.warn("QueryExecutor is already shutDown, won't do anything!");
    }
  }

  @Override
  public boolean isStarted() {
    return _isStarted;
  }

  @Override
  public synchronized void start() {
    _isStarted = true;
    LOGGER.info("QueryExecutor is started!");
  }

  @Override
  public void updateResourceTimeOutInMs(String resource, long timeOutMs) {
    _resourceTimeOutMsMap.put(resource, timeOutMs);
  }

  private long getResourceTimeOut(BrokerRequest brokerRequest) {
    try {
      String resourceName = brokerRequest.getQuerySource().getTableName();
      if (_resourceTimeOutMsMap.containsKey(resourceName)) {
        return _resourceTimeOutMsMap.get(resourceName);
      }
    } catch (Exception e) {
      // Return the default timeout value
      LOGGER.warn("Caught exception while obtaining resource timeout", e);
    }
    return _defaultTimeOutMs;
  }
}
