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
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.common.config.NettyConfig;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerQueryPhase;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.query.reduce.BrokerReduceService;
import org.apache.pinot.core.routing.MultiClusterRoutingContext;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.core.transport.AsyncQueryResponse;
import org.apache.pinot.core.transport.QueryResponse;
import org.apache.pinot.core.transport.QueryRouter;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerResponse;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
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

  protected final BrokerReduceService _brokerReduceService;
  protected final QueryRouter _queryRouter;
  protected final FailureDetector _failureDetector;

  public SingleConnectionBrokerRequestHandler(PinotConfiguration config, String brokerId,
      BrokerRequestIdGenerator requestIdGenerator, RoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache,
      NettyConfig nettyConfig, TlsConfig tlsConfig, ServerRoutingStatsManager serverRoutingStatsManager,
      FailureDetector failureDetector, ThreadAccountant threadAccountant,
      MultiClusterRoutingContext multiClusterRoutingContext) {
    super(config, brokerId, requestIdGenerator, routingManager, accessControlFactory, queryQuotaManager, tableCache,
        threadAccountant, multiClusterRoutingContext);
    _brokerReduceService = new BrokerReduceService(_config);
    _queryRouter = new QueryRouter(_brokerId, nettyConfig, tlsConfig, serverRoutingStatsManager, threadAccountant);
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
      BrokerRequest serverBrokerRequest, TableRouteInfo route, long timeoutMs,
      ServerStats serverStats, RequestContext requestContext)
      throws Exception {
    assert route.getOfflineBrokerRequest() != null || route.getRealtimeBrokerRequest() != null;
    if (requestContext.isSampledRequest()) {
      serverBrokerRequest.getPinotQuery().putToQueryOptions(CommonConstants.Broker.Request.TRACE, "true");
    }
    String rawTableName = TableNameBuilder.extractRawTableName(serverBrokerRequest.getQuerySource().getTableName());
    long scatterGatherStartTimeNs = System.nanoTime();
    ScatterResult scatterResult = doScatter(requestId, rawTableName, route, timeoutMs, serverStats);
    return doReduce(originalBrokerRequest, serverBrokerRequest, scatterResult, scatterGatherStartTimeNs, timeoutMs,
        rawTableName);
  }

  /**
   * Executes scatter-gather: sends the query to servers and collects per-server DataTables.
   * Subclasses may override to replace or augment the scatter step.
   */
  protected ScatterResult doScatter(long requestId, String rawTableName, TableRouteInfo route, long timeoutMs,
      ServerStats serverStats)
      throws Exception {
    AsyncQueryResponse asyncQueryResponse = _queryRouter.submitQuery(requestId, rawTableName, route, timeoutMs);
    Map<ServerRoutingInstance, ServerResponse> finalResponses = asyncQueryResponse.getFinalResponses();
    boolean timedOut = asyncQueryResponse.getStatus() == QueryResponse.Status.TIMED_OUT;
    ServerRoutingInstance failedServer = asyncQueryResponse.getFailedServer();
    if (failedServer != null) {
      _failureDetector.markServerUnhealthy(failedServer.getInstanceId(), failedServer.getHostname());
    }
    // TODO Use scatterGatherStats as serverStats
    serverStats.setServerStats(asyncQueryResponse.getServerStats());

    long totalResponseSize = 0;
    Map<ServerRoutingInstance, DataTable> dataTableMap = Maps.newHashMapWithExpectedSize(finalResponses.size());
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
    ScatterResultStats stats = new ScatterResultStats(
        dataTableMap.size() + serversNotResponded.size(), dataTableMap.size(), totalResponseSize);
    return new ScatterResult(dataTableMap, serversNotResponded, stats, timedOut, asyncQueryResponse.getException());
  }

  /**
   * Executes the reduce step on the scatter result and populates the response with server stats.
   * Subclasses may override to perform custom reduce logic, or construct a {@link ScatterResult}
   * with a substituted data table map using {@link ScatterResultStats} to preserve server stats.
   */
  protected BrokerResponseNative doReduce(BrokerRequest originalBrokerRequest, BrokerRequest serverBrokerRequest,
      ScatterResult scatterResult, long scatterGatherStartTimeNs, long timeoutMs, String rawTableName)
      throws Exception {
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.SCATTER_GATHER,
        System.nanoTime() - scatterGatherStartTimeNs);

    if (scatterResult.isTimedOut()) {
      BrokerMeter meter = QueryOptionsUtils.isSecondaryWorkload(serverBrokerRequest.getPinotQuery().getQueryOptions())
          ? BrokerMeter.SECONDARY_WORKLOAD_BROKER_RESPONSES_WITH_TIMEOUTS : BrokerMeter.BROKER_RESPONSES_WITH_TIMEOUTS;
      _brokerMetrics.addMeteredTableValue(rawTableName, meter, 1);
    }

    long reduceStartTimeNs = System.nanoTime();
    long reduceTimeoutMs = timeoutMs - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - scatterGatherStartTimeNs);
    BrokerResponseNative brokerResponse =
        _brokerReduceService.reduceOnDataTable(originalBrokerRequest, serverBrokerRequest,
            scatterResult.getDataTableMap(), reduceTimeoutMs, _brokerMetrics);
    long reduceTimeNanos = System.nanoTime() - reduceStartTimeNs;
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.REDUCE, reduceTimeNanos);

    brokerResponse.setNumServersQueried(scatterResult.getNumServersQueried());
    brokerResponse.setNumServersResponded(scatterResult.getNumServersResponded());
    brokerResponse.setBrokerReduceTimeMs(TimeUnit.NANOSECONDS.toMillis(reduceTimeNanos));

    if (scatterResult.getSendException() != null) {
      brokerResponse.addException(new QueryProcessingException(QueryErrorCode.BROKER_REQUEST_SEND,
          scatterResult.getSendException().getMessage()));
    }
    List<ServerRoutingInstance> serversNotResponded = scatterResult.getServersNotResponded();
    if (!serversNotResponded.isEmpty()) {
      brokerResponse.addException(new QueryProcessingException(QueryErrorCode.SERVER_NOT_RESPONDING,
          String.format("%d servers %s not responded", serversNotResponded.size(), serversNotResponded)));
      BrokerMeter meter = QueryOptionsUtils.isSecondaryWorkload(serverBrokerRequest.getPinotQuery().getQueryOptions())
          ? BrokerMeter.SECONDARY_WORKLOAD_BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED
          : BrokerMeter.BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED;
      _brokerMetrics.addMeteredTableValue(rawTableName, meter, 1);
    }
    if (brokerResponse.getExceptionsSize() > 0) {
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_PROCESSING_EXCEPTIONS, 1);
    }
    _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.TOTAL_SERVER_RESPONSE_SIZE,
        scatterResult.getTotalResponseSize());

    return brokerResponse;
  }

  /**
   * Snapshot of server-side scatter statistics. Passed to {@link ScatterResult} so that server
   * counts are always derived from the live scatter, not from a data table map that may have been
   * augmented by a subclass.
   */
  public static final class ScatterResultStats {
    private final int _numServersQueried;
    private final int _numServersResponded;
    private final long _totalResponseSize;

    public ScatterResultStats(int numServersQueried, int numServersResponded, long totalResponseSize) {
      _numServersQueried = numServersQueried;
      _numServersResponded = numServersResponded;
      _totalResponseSize = totalResponseSize;
    }

    public int getNumServersQueried() {
      return _numServersQueried;
    }

    public int getNumServersResponded() {
      return _numServersResponded;
    }

    public long getTotalResponseSize() {
      return _totalResponseSize;
    }
  }

  /**
   * Carries the scatter-gather result before the reduce step.
   */
  public static final class ScatterResult {
    private final Map<ServerRoutingInstance, DataTable> _dataTableMap;
    private final List<ServerRoutingInstance> _serversNotResponded;
    private final long _totalResponseSize;
    private final boolean _timedOut;
    private final Exception _sendException;
    private final int _numServersQueried;
    private final int _numServersResponded;

    public ScatterResult(Map<ServerRoutingInstance, DataTable> dataTableMap,
        List<ServerRoutingInstance> serversNotResponded, ScatterResultStats stats,
        boolean timedOut, Exception sendException) {
      _dataTableMap = dataTableMap;
      _serversNotResponded = serversNotResponded;
      _totalResponseSize = stats.getTotalResponseSize();
      _timedOut = timedOut;
      _sendException = sendException;
      _numServersQueried = stats.getNumServersQueried();
      _numServersResponded = stats.getNumServersResponded();
    }

    public Map<ServerRoutingInstance, DataTable> getDataTableMap() {
      return _dataTableMap;
    }

    public List<ServerRoutingInstance> getServersNotResponded() {
      return _serversNotResponded;
    }

    public int getNumServersQueried() {
      return _numServersQueried;
    }

    public int getNumServersResponded() {
      return _numServersResponded;
    }

    public long getTotalResponseSize() {
      return _totalResponseSize;
    }

    public boolean isTimedOut() {
      return _timedOut;
    }

    public Exception getSendException() {
      return _sendException;
    }
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
