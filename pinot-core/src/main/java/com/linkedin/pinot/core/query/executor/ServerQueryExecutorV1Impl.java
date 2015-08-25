/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.linkedin.pinot.core.trace.TraceContext;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.metrics.ServerMeter;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.metrics.ServerQueryPhase;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.data.manager.offline.InstanceDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.Plan;
import com.linkedin.pinot.core.plan.maker.InstancePlanMakerImplV2;
import com.linkedin.pinot.core.plan.maker.PlanMaker;
import com.linkedin.pinot.core.query.config.QueryExecutorConfig;
import com.linkedin.pinot.core.query.pruner.SegmentPrunerService;
import com.linkedin.pinot.core.query.pruner.SegmentPrunerServiceImpl;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

public class ServerQueryExecutorV1Impl implements QueryExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerQueryExecutorV1Impl.class);

  private static final String Domain = "com.linkedin.pinot";
  private QueryExecutorConfig _queryExecutorConfig = null;
  private InstanceDataManager _instanceDataManager = null;
  private SegmentPrunerService _segmentPrunerService = null;
  private PlanMaker _planMaker = null;
  private Timer _queryExecutorTimer = null;
  private volatile boolean _isStarted = false;
  private long _defaultTimeOutMs = 15000;
  private boolean _printQueryPlan = true;
  private final Map<String, Long> _resourceTimeOutMsMap = new ConcurrentHashMap<String, Long>();
  private ServerMetrics _serverMetrics;

  public ServerQueryExecutorV1Impl() {
  }

  public ServerQueryExecutorV1Impl(boolean printQueryPlan) {
    _printQueryPlan = printQueryPlan;
  }

  @Override
  public void init(Configuration queryExecutorConfig, DataManager dataManager, ServerMetrics serverMetrics)
      throws ConfigurationException {
    _serverMetrics = serverMetrics;
    _queryExecutorConfig = new QueryExecutorConfig(queryExecutorConfig);
    _instanceDataManager = (InstanceDataManager) dataManager;
    if (_queryExecutorConfig.getTimeOut() > 0) {
      _defaultTimeOutMs = _queryExecutorConfig.getTimeOut();
    }
    LOGGER.info("Default timeout for query executor : {}", _defaultTimeOutMs);
    LOGGER.info("Trying to build SegmentPrunerService");
    if (_segmentPrunerService == null) {
      _segmentPrunerService = new SegmentPrunerServiceImpl(_queryExecutorConfig.getPrunerConfig());
    }
    LOGGER.info("Trying to build QueryPlanMaker");
    _planMaker = new InstancePlanMakerImplV2();
    LOGGER.info("Trying to build QueryExecutorTimer");
    if (_queryExecutorTimer == null) {
      _queryExecutorTimer =
          Metrics.newTimer(new MetricName(Domain, "timer", "query-executor-time-"), TimeUnit.MILLISECONDS,
              TimeUnit.SECONDS);
    }
  }

  @Override
  public DataTable processQuery(final InstanceRequest instanceRequest) {
    DataTable instanceResponse;
    long start = System.currentTimeMillis();
    try {
      TraceContext.register(instanceRequest);
      final BrokerRequest brokerRequest = instanceRequest.getQuery();
      LOGGER.info("Incoming query is : {}", brokerRequest);
      long startPruningTime = System.nanoTime();
      final List<IndexSegment> queryableSegmentDataManagerList = getPrunedQueryableSegments(instanceRequest);
      long pruningTime = System.nanoTime() - startPruningTime;
      _serverMetrics.addPhaseTiming(brokerRequest, ServerQueryPhase.SEGMENT_PRUNING, pruningTime);
      LOGGER.info("Matched {} segments! ", queryableSegmentDataManagerList.size());
      if (queryableSegmentDataManagerList.isEmpty()) {
        return null;
      }
      long startPlanTime = System.nanoTime();
      final Plan globalQueryPlan = _planMaker.makeInterSegmentPlan(
          queryableSegmentDataManagerList,
          brokerRequest,
          _instanceDataManager.getTableDataManager(brokerRequest.getQuerySource().getTableName())
              .getExecutorService(),
          getResourceTimeOut(instanceRequest.getQuery()));
      long planTime = System.nanoTime() - startPlanTime;
      _serverMetrics.addPhaseTiming(brokerRequest, ServerQueryPhase.BUILD_QUERY_PLAN, planTime);

      if (_printQueryPlan) {
        LOGGER.debug("***************************** Query Plan for Request {} ***********************************", instanceRequest.getRequestId());
        globalQueryPlan.print();
        LOGGER.debug("*********************************** End Query Plan ***********************************");
      }

      long executeStartTime = System.nanoTime();
      globalQueryPlan.execute();
      long executeTime = System.nanoTime() - executeStartTime;
      _serverMetrics.addPhaseTiming(brokerRequest, ServerQueryPhase.QUERY_PLAN_EXECUTION, executeTime);
      instanceResponse = globalQueryPlan.getInstanceResponse();
      long end = System.currentTimeMillis();
      LOGGER.info("Searching Instance for Request Id - {}, browse took: {}", instanceRequest.getRequestId(), (end - start));
      LOGGER.debug("InstanceResponse for Request Id - {} : {}", instanceRequest.getRequestId(), instanceResponse.toString());
      instanceResponse.getMetadata().put("timeUsedMs", Long.toString((end - start)));
      instanceResponse.getMetadata().put("requestId", Long.toString(instanceRequest.getRequestId()));
      instanceResponse.getMetadata().put("traceInfo", TraceContext.getTraceInfoOfRequestId(instanceRequest.getRequestId()));
      return instanceResponse;
    } catch (Exception e) {
      _serverMetrics.addMeteredValue(instanceRequest.getQuery(), ServerMeter.QUERY_EXECUTION_EXCEPTIONS, 1);
      LOGGER.error(e.getMessage(), e);
      instanceResponse = new DataTable();
      instanceResponse.addException(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
      long end = System.currentTimeMillis();
      LOGGER.info("Searching Instance for Request Id - {}, browse took: {}", instanceRequest.getRequestId(), (end - start));
      LOGGER.debug("InstanceResponse for Request Id - {} : {}", instanceRequest.getRequestId(), instanceResponse.toString());
      instanceResponse.getMetadata().put("timeUsedMs", Long.toString((end - start)));
      instanceResponse.getMetadata().put("requestId", Long.toString(instanceRequest.getRequestId()));
      return instanceResponse;
    } finally {
      if (_instanceDataManager.getTableDataManager(instanceRequest.getQuery().getQuerySource().getTableName()) != null) {
        _instanceDataManager.getTableDataManager(instanceRequest.getQuery().getQuerySource().getTableName())
            .returnSegmentReaders(instanceRequest.getSearchSegments());
      }
      TraceContext.unregister(instanceRequest);
    }
  }

  private List<IndexSegment> getPrunedQueryableSegments(final InstanceRequest instanceRequest) {
    LOGGER
        .info("InstanceRequest request {} segments", instanceRequest.getSearchSegments().size());

    final String tableName = instanceRequest.getQuery().getQuerySource().getTableName();
    final TableDataManager tableDataManager = _instanceDataManager.getTableDataManager(tableName);
    if (tableDataManager == null || instanceRequest.getSearchSegmentsSize() == 0) {
      return new ArrayList<IndexSegment>();
    }
    final List<SegmentDataManager> matchedSegmentDataManagerFromServer = tableDataManager.getSegments(instanceRequest.getSearchSegments());
    LOGGER.info("TableDataManager found {} segments before pruning", matchedSegmentDataManagerFromServer.size());
    final List<IndexSegment> queryableSegmentDataManagerList = new ArrayList<IndexSegment>();
    for (final SegmentDataManager segmentDataManager : matchedSegmentDataManagerFromServer) {
      final IndexSegment indexSegment = segmentDataManager.getSegment();
      if (!_segmentPrunerService.prune(indexSegment, instanceRequest.getQuery())) {
        queryableSegmentDataManagerList.add(indexSegment);
      }
    }
    return queryableSegmentDataManagerList;
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
        return _resourceTimeOutMsMap.get(brokerRequest);
      }
    } catch (Exception e) {
      // Return the default timeout value
      LOGGER.warn("Caught exception while obtaining resource timeout", e);
    }
    return _defaultTimeOutMs;
  }
}
