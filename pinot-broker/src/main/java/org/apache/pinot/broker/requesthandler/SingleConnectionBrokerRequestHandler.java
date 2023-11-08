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
package org.apache.pinot.broker.requesthandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.failuredetector.FailureDetector;
import org.apache.pinot.broker.failuredetector.FailureDetectorFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.config.NettyConfig;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerQueryPhase;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.query.reduce.BrokerReduceService;
import org.apache.pinot.core.transport.AsyncQueryResponse;
import org.apache.pinot.core.transport.QueryResponse;
import org.apache.pinot.core.transport.QueryRouter;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerResponse;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListener;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>SingleConnectionBrokerRequestHandler</code> class is a thread-safe broker request handler using a single
 * connection per server to route the queries.
 */
@ThreadSafe
public class SingleConnectionBrokerRequestHandler extends BaseBrokerRequestHandler implements FailureDetector.Listener {
  private static final Logger LOGGER = LoggerFactory.getLogger(SingleConnectionBrokerRequestHandler.class);

  private final BrokerReduceService _brokerReduceService;
  private final QueryRouter _queryRouter;
  private final FailureDetector _failureDetector;

  public SingleConnectionBrokerRequestHandler(PinotConfiguration config, String brokerId,
      BrokerRoutingManager routingManager, AccessControlFactory accessControlFactory,
      QueryQuotaManager queryQuotaManager, TableCache tableCache, BrokerMetrics brokerMetrics, NettyConfig nettyConfig,
      TlsConfig tlsConfig, ServerRoutingStatsManager serverRoutingStatsManager,
      BrokerQueryEventListener brokerQueryEventListener) {
    super(config, brokerId, routingManager, accessControlFactory, queryQuotaManager, tableCache, brokerMetrics,
        brokerQueryEventListener);
    LOGGER.info("Using Netty BrokerRequestHandler.");

    _brokerReduceService = new BrokerReduceService(_config);
    _queryRouter = new QueryRouter(_brokerId, brokerMetrics, nettyConfig, tlsConfig, serverRoutingStatsManager);
    _failureDetector = FailureDetectorFactory.getFailureDetector(config, brokerMetrics);
  }

  @Override
  public void start() {
    _failureDetector.register(this);
    _failureDetector.start();
  }

  @Override
  public synchronized void shutDown() {
    _failureDetector.stop();
    _queryRouter.shutDown();
    _brokerReduceService.shutDown();
  }

  @Override
  protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
      BrokerRequest serverBrokerRequest, @Nullable BrokerRequest offlineBrokerRequest,
      @Nullable Map<ServerInstance, List<String>> offlineRoutingTable, @Nullable BrokerRequest realtimeBrokerRequest,
      @Nullable Map<ServerInstance, List<String>> realtimeRoutingTable, long timeoutMs, ServerStats serverStats,
      RequestContext requestContext)
      throws Exception {
    assert offlineBrokerRequest != null || realtimeBrokerRequest != null;
    if (requestContext.isSampledRequest()) {
      serverBrokerRequest.getPinotQuery().putToQueryOptions(CommonConstants.Broker.Request.TRACE, "true");
    }

    String rawTableName = TableNameBuilder.extractRawTableName(serverBrokerRequest.getQuerySource().getTableName());
    long scatterGatherStartTimeNs = System.nanoTime();
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, rawTableName, offlineBrokerRequest, offlineRoutingTable,
            realtimeBrokerRequest, realtimeRoutingTable, timeoutMs);
    _failureDetector.notifyQuerySubmitted(asyncQueryResponse);
    Map<ServerRoutingInstance, ServerResponse> finalResponses = asyncQueryResponse.getFinalResponses();
    if (asyncQueryResponse.getStatus() == QueryResponse.Status.TIMED_OUT) {
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_TIMEOUTS, 1);
    }
    _failureDetector.notifyQueryFinished(asyncQueryResponse);
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.SCATTER_GATHER,
        System.nanoTime() - scatterGatherStartTimeNs);
    // TODO Use scatterGatherStats as serverStats
    serverStats.setServerStats(asyncQueryResponse.getServerStats());

    int numServersQueried = finalResponses.size();
    long totalResponseSize = 0;
    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>(HashUtil.getHashMapCapacity(numServersQueried));
    List<ServerRoutingInstance> serversNotResponded = new ArrayList<>();
    for (Map.Entry<ServerRoutingInstance, ServerResponse> entry : finalResponses.entrySet()) {
      ServerResponse serverResponse = entry.getValue();
      DataTable dataTable = serverResponse.getDataTable();
      if (dataTable != null) {
        dataTableMap.put(entry.getKey(), dataTable);
        totalResponseSize += serverResponse.getResponseSize();
      } else {
        serversNotResponded.add(entry.getKey());
      }
    }
    int numServersResponded = dataTableMap.size();

    long reduceStartTimeNs = System.nanoTime();
    long reduceTimeOutMs = timeoutMs - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - scatterGatherStartTimeNs);
    BrokerResponseNative brokerResponse =
        _brokerReduceService.reduceOnDataTable(originalBrokerRequest, serverBrokerRequest, dataTableMap,
            reduceTimeOutMs, _brokerMetrics);
    final long reduceTimeNanos = System.nanoTime() - reduceStartTimeNs;
    requestContext.setTraceInfo(brokerResponse.getTraceInfo());
    requestContext.setReduceTimeNanos(reduceTimeNanos);
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.REDUCE, reduceTimeNanos);

    brokerResponse.setNumServersQueried(numServersQueried);
    brokerResponse.setNumServersResponded(numServersResponded);

    Exception brokerRequestSendException = asyncQueryResponse.getException();
    if (brokerRequestSendException != null) {
      String errorMsg = QueryException.getTruncatedStackTrace(brokerRequestSendException);
      brokerResponse.addToExceptions(
          new QueryProcessingException(QueryException.BROKER_REQUEST_SEND_ERROR_CODE, errorMsg));
    }
    int numServersNotResponded = serversNotResponded.size();
    if (numServersNotResponded != 0) {
      brokerResponse.addToExceptions(new QueryProcessingException(QueryException.SERVER_NOT_RESPONDING_ERROR_CODE,
          String.format("%d servers %s not responded", numServersNotResponded, serversNotResponded)));
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED, 1);
    }
    if (brokerResponse.getExceptionsSize() > 0) {
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_PROCESSING_EXCEPTIONS, 1);
    }
    _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.TOTAL_SERVER_RESPONSE_SIZE, totalResponseSize);

    return brokerResponse;
  }

  @Override
  public void notifyUnhealthyServer(String instanceId, FailureDetector failureDetector) {
    _routingManager.excludeServerFromRouting(instanceId);
  }

  @Override
  public void retryUnhealthyServer(String instanceId, FailureDetector failureDetector) {
    LOGGER.info("Retrying unhealthy server: {}", instanceId);
    ServerInstance serverInstance = _routingManager.getEnabledServerInstanceMap().get(instanceId);
    if (serverInstance == null) {
      LOGGER.info("Failed to find enabled server: {} in routing manager, skipping the retry", instanceId);
      return;
    }
    if (_queryRouter.connect(serverInstance)) {
      LOGGER.info("Successfully connect to server: {}, marking it healthy", instanceId);
      failureDetector.markServerHealthy(instanceId);
    } else {
      LOGGER.warn("Still cannot connect to server: {}, retry later", instanceId);
    }
  }

  @Override
  public void notifyHealthyServer(String instanceId, FailureDetector failureDetector) {
    _routingManager.includeServerToRouting(instanceId);
  }
}
