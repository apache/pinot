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

import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.config.NettyConfig;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerQueryPhase;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.query.reduce.BrokerReduceService;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.transport.AsyncQueryResponse;
import org.apache.pinot.core.transport.QueryResponse;
import org.apache.pinot.core.transport.QueryRouter;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerResponse;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryThreadContext;
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
public class SingleConnectionBrokerRequestHandler extends BaseSingleStageBrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(SingleConnectionBrokerRequestHandler.class);

  private final BrokerReduceService _brokerReduceService;
  private final QueryRouter _queryRouter;
  private final FailureDetector _failureDetector;

  public SingleConnectionBrokerRequestHandler(PinotConfiguration config, String brokerId,
      BrokerRoutingManager routingManager, AccessControlFactory accessControlFactory,
      QueryQuotaManager queryQuotaManager, TableCache tableCache, NettyConfig nettyConfig, TlsConfig tlsConfig,
      ServerRoutingStatsManager serverRoutingStatsManager, FailureDetector failureDetector) {
    super(config, brokerId, routingManager, accessControlFactory, queryQuotaManager, tableCache);
    _brokerReduceService = new BrokerReduceService(_config);
    _queryRouter = new QueryRouter(_brokerId, _brokerMetrics, nettyConfig, tlsConfig, serverRoutingStatsManager);
    _failureDetector = failureDetector;
    _failureDetector.registerUnhealthyServerRetrier(this::retryUnhealthyServer);
  }

  @Override
  public void start() {
    super.start();
  }

  @Override
  public void shutDown() {
    super.shutDown();
    _queryRouter.shutDown();
    _brokerReduceService.shutDown();
  }

  @Override
  protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
      BrokerRequest serverBrokerRequest, @Nullable BrokerRequest offlineBrokerRequest,
      @Nullable Map<ServerInstance, ServerRouteInfo> offlineRoutingTable,
      @Nullable BrokerRequest realtimeBrokerRequest,
      @Nullable Map<ServerInstance, ServerRouteInfo> realtimeRoutingTable, long timeoutMs,
      ServerStats serverStats, RequestContext requestContext)
      throws Exception {
    QueryThreadContext.setQueryEngine("sse-http");
    assert offlineBrokerRequest != null || realtimeBrokerRequest != null;
    if (requestContext.isSampledRequest()) {
      serverBrokerRequest.getPinotQuery().putToQueryOptions(CommonConstants.Broker.Request.TRACE, "true");
    }

    String rawTableName = TableNameBuilder.extractRawTableName(serverBrokerRequest.getQuerySource().getTableName());
    long scatterGatherStartTimeNs = System.nanoTime();
    AsyncQueryResponse asyncQueryResponse =
        _queryRouter.submitQuery(requestId, rawTableName, offlineBrokerRequest, offlineRoutingTable,
            realtimeBrokerRequest, realtimeRoutingTable, timeoutMs);
    Map<ServerRoutingInstance, ServerResponse> finalResponses = asyncQueryResponse.getFinalResponses();
    if (asyncQueryResponse.getStatus() == QueryResponse.Status.TIMED_OUT) {
      BrokerMeter meter = QueryOptionsUtils.isSecondaryWorkload(serverBrokerRequest.getPinotQuery().getQueryOptions())
          ? BrokerMeter.SECONDARY_WORKLOAD_BROKER_RESPONSES_WITH_TIMEOUTS : BrokerMeter.BROKER_RESPONSES_WITH_TIMEOUTS;
      _brokerMetrics.addMeteredTableValue(rawTableName, meter, 1);
    }
    if (asyncQueryResponse.getFailedServer() != null) {
      _failureDetector.markServerUnhealthy(asyncQueryResponse.getFailedServer().getInstanceId());
    }
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.SCATTER_GATHER,
        System.nanoTime() - scatterGatherStartTimeNs);
    // TODO Use scatterGatherStats as serverStats
    serverStats.setServerStats(asyncQueryResponse.getServerStats());

    int numServersQueried = finalResponses.size();
    long totalResponseSize = 0;
    Map<ServerRoutingInstance, DataTable> dataTableMap = Maps.newHashMapWithExpectedSize(numServersQueried);
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
    long reduceTimeoutMs = timeoutMs - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - scatterGatherStartTimeNs);
    BrokerResponseNative brokerResponse =
        _brokerReduceService.reduceOnDataTable(originalBrokerRequest, serverBrokerRequest, dataTableMap,
            reduceTimeoutMs, _brokerMetrics);
    long reduceTimeNanos = System.nanoTime() - reduceStartTimeNs;
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.REDUCE, reduceTimeNanos);

    brokerResponse.setNumServersQueried(numServersQueried);
    brokerResponse.setNumServersResponded(numServersResponded);
    brokerResponse.setBrokerReduceTimeMs(TimeUnit.NANOSECONDS.toMillis(reduceTimeNanos));

    Exception brokerRequestSendException = asyncQueryResponse.getException();
    if (brokerRequestSendException != null) {
      String errorMsg = QueryException.getTruncatedStackTrace(brokerRequestSendException);
      brokerResponse.addException(
          new QueryProcessingException(QueryException.BROKER_REQUEST_SEND_ERROR_CODE, errorMsg));
    }
    int numServersNotResponded = serversNotResponded.size();
    if (numServersNotResponded != 0) {
      brokerResponse.addException(new QueryProcessingException(QueryException.SERVER_NOT_RESPONDING_ERROR_CODE,
          String.format("%d servers %s not responded", numServersNotResponded, serversNotResponded)));

      BrokerMeter meter = QueryOptionsUtils.isSecondaryWorkload(serverBrokerRequest.getPinotQuery().getQueryOptions())
          ? BrokerMeter.SECONDARY_WORKLOAD_BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED
          : BrokerMeter.BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED;
      _brokerMetrics.addMeteredTableValue(rawTableName, meter, 1);
    }
    if (brokerResponse.getExceptionsSize() > 0) {
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_PROCESSING_EXCEPTIONS, 1);
    }
    _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.TOTAL_SERVER_RESPONSE_SIZE, totalResponseSize);

    return brokerResponse;
  }

  /**
   * Check if a server that was previously detected as unhealthy is now healthy.
   */
  public FailureDetector.ServerState retryUnhealthyServer(String instanceId) {
    LOGGER.info("Retrying unhealthy server: {}", instanceId);
    ServerInstance serverInstance = _routingManager.getEnabledServerInstanceMap().get(instanceId);

    if (serverInstance == null) {
      LOGGER.info("Failed to find enabled server: {} in routing manager, skipping the retry", instanceId);
      return FailureDetector.ServerState.UNHEALTHY;
    }

    // Could occur if the cluster is only serving multi-stage queries
    if (!_queryRouter.hasChannel(serverInstance)) {
      return FailureDetector.ServerState.UNKNOWN;
    }

    if (_queryRouter.connect(serverInstance)) {
      LOGGER.info("Successfully connect to server: {}, marking it healthy", instanceId);
      return FailureDetector.ServerState.HEALTHY;
    } else {
      LOGGER.warn("Still cannot connect to server: {}, retry later", instanceId);
      return FailureDetector.ServerState.UNHEALTHY;
    }
  }
}
