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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

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
import com.linkedin.pinot.core.data.manager.offline.TableDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
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
    LOGGER.info("Default timeout for query executor : " + _defaultTimeOutMs);
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
      final BrokerRequest brokerRequest = instanceRequest.getQuery();
      LOGGER.info("Incoming query is :" + brokerRequest);
      final List<IndexSegment> queryableSegmentDataManagerList = _serverMetrics.timePhase(brokerRequest,
          ServerQueryPhase.SEGMENT_PRUNING, new Callable<List<IndexSegment>>() {
            @Override
            public List<IndexSegment> call() throws Exception {
              return getPrunedQueryableSegments(instanceRequest);
            }
          });
      LOGGER.info("Matched " + queryableSegmentDataManagerList.size() + " segments! ");
      if (queryableSegmentDataManagerList.isEmpty()) {
        return null;
      }
      final Plan globalQueryPlan = _serverMetrics.timePhase(brokerRequest, ServerQueryPhase.BUILD_QUERY_PLAN, new Callable<Plan>() {
        @Override
        public Plan call() throws Exception {
          return _planMaker.makeInterSegmentPlan(
              queryableSegmentDataManagerList,
              brokerRequest,
              _instanceDataManager.getResourceDataManager(brokerRequest.getQuerySource().getResourceName())
                  .getExecutorService(),
              getResourceTimeOut(instanceRequest.getQuery()));
        }
      });

      if (_printQueryPlan) {
        LOGGER.debug("***************************** Query Plan for Request " + instanceRequest.getRequestId() + "***********************************");
        globalQueryPlan.print();
        LOGGER.debug("*********************************** End Query Plan ***********************************");
      }
      _serverMetrics.timePhase(brokerRequest, ServerQueryPhase.QUERY_PLAN_EXECUTION, new Callable<Object>() {
        @Override
        public Object call()
            throws Exception {
          globalQueryPlan.execute();
          return null;
        }
      });
      instanceResponse = globalQueryPlan.getInstanceResponse();
      long end = System.currentTimeMillis();
      LOGGER.info("Searching Instance for Request Id - {}, browse took: {}", instanceRequest.getRequestId(), (end - start));
      LOGGER.debug("InstanceResponse for Request Id - {} : {}", instanceRequest.getRequestId(), instanceResponse.toString());
      instanceResponse.getMetadata().put("timeUsedMs", Long.toString((end - start)));
      instanceResponse.getMetadata().put("requestId", Long.toString(instanceRequest.getRequestId()));
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
      if (_instanceDataManager.getResourceDataManager(instanceRequest.getQuery().getQuerySource().getResourceName()) != null) {
        _instanceDataManager.getResourceDataManager(instanceRequest.getQuery().getQuerySource().getResourceName())
            .returnSegmentReaders(instanceRequest.getSearchSegments());
      }
    }
  }

  private List<IndexSegment> getPrunedQueryableSegments(final InstanceRequest instanceRequest) {
    final String resourceName = instanceRequest.getQuery().getQuerySource().getResourceName();
    final TableDataManager resourceDataManager = _instanceDataManager.getResourceDataManager(resourceName);
    if (resourceDataManager == null || instanceRequest.getSearchSegmentsSize() == 0) {
      return new ArrayList<IndexSegment>();
    }

    final List<IndexSegment> queryableSegmentDataManagerList = new ArrayList<IndexSegment>();
    final List<SegmentDataManager> matchedSegmentDataManagerFromServer =
        resourceDataManager.getSegments(instanceRequest.getSearchSegments());

    LOGGER.info("InstanceRequest request " + instanceRequest.getSearchSegments().size() + " segments : "
        + Arrays.toString(instanceRequest.getSearchSegments().toArray(new String[0])));
    LOGGER.info("ResourceDataManager found " + matchedSegmentDataManagerFromServer.size() + " segments before pruning : "
        + Arrays.toString(matchedSegmentDataManagerFromServer.toArray(new SegmentDataManager[0])));
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
      String resourceName = brokerRequest.getQuerySource().getResourceName();
      if (_resourceTimeOutMsMap.containsKey(resourceName)) {
        return _resourceTimeOutMsMap.get(brokerRequest);
      }
    } catch (Exception e) {
      // Return the default timeout value
    }
    return _defaultTimeOutMs;
  }
}
