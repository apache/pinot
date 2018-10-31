/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.broker.requesthandler;

import com.linkedin.pinot.broker.broker.AccessControlFactory;
import com.linkedin.pinot.broker.broker.helix.LiveInstancesChangeListenerImpl;
import com.linkedin.pinot.broker.queryquota.TableQueryQuotaManager;
import com.linkedin.pinot.broker.routing.RoutingTable;
import com.linkedin.pinot.broker.routing.TimeBoundaryService;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.metrics.BrokerMeter;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.metrics.BrokerQueryPhase;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.response.broker.BrokerResponseNative;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.common.datatable.DataTableFactory;
import com.linkedin.pinot.serde.SerDe;
import com.linkedin.pinot.transport.common.CompositeFuture;
import com.linkedin.pinot.transport.conf.TransportClientConf;
import com.linkedin.pinot.transport.config.ConnectionPoolConfig;
import com.linkedin.pinot.transport.metrics.NettyClientMetrics;
import com.linkedin.pinot.transport.netty.PooledNettyClientResourceManager;
import com.linkedin.pinot.transport.pool.KeyedPool;
import com.linkedin.pinot.transport.pool.KeyedPoolImpl;
import com.linkedin.pinot.transport.scattergather.ScatterGather;
import com.linkedin.pinot.transport.scattergather.ScatterGatherImpl;
import com.linkedin.pinot.transport.scattergather.ScatterGatherRequest;
import com.linkedin.pinot.transport.scattergather.ScatterGatherStats;
import com.yammer.metrics.core.MetricsRegistry;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>ConnectionPoolBrokerRequestHandler</code> class is a thread-safe broker request handler using connection
 * pool to route the queries.
 */
@ThreadSafe
public class ConnectionPoolBrokerRequestHandler extends BaseBrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionPoolBrokerRequestHandler.class);
  private static final String TRANSPORT_CONFIG_PREFIX = "pinot.broker.transport";

  private final LiveInstancesChangeListenerImpl _liveInstanceChangeListener;
  private final EventLoopGroup _eventLoopGroup;
  private final ScheduledThreadPoolExecutor _poolTimeoutExecutor;
  private final ExecutorService _requestSenderPool;
  private final KeyedPool<PooledNettyClientResourceManager.PooledClientConnection> _connPool;
  private final ScatterGather _scatterGather;

  public ConnectionPoolBrokerRequestHandler(Configuration config, RoutingTable routingTable,
      TimeBoundaryService timeBoundaryService, AccessControlFactory accessControlFactory,
      TableQueryQuotaManager tableQueryQuotaManager, BrokerMetrics brokerMetrics,
      LiveInstancesChangeListenerImpl liveInstanceChangeListener, MetricsRegistry metricsRegistry) {
    super(config, routingTable, timeBoundaryService, accessControlFactory, tableQueryQuotaManager, brokerMetrics);
    _liveInstanceChangeListener = liveInstanceChangeListener;

    TransportClientConf transportClientConf = new TransportClientConf();
    transportClientConf.init(_config.subset(TRANSPORT_CONFIG_PREFIX));

    // Set up connection pool
    _eventLoopGroup = new NioEventLoopGroup();
    // Some of the client metrics use histogram which is doing synchronous operation, which are fixed overhead per
    // request/response
    // TODO: measure the overhead of this
    NettyClientMetrics clientMetrics = new NettyClientMetrics(metricsRegistry, "client_");
    PooledNettyClientResourceManager resourceManager =
        new PooledNettyClientResourceManager(_eventLoopGroup, new HashedWheelTimer(), clientMetrics);
    _poolTimeoutExecutor = new ScheduledThreadPoolExecutor(50);
    _requestSenderPool = Executors.newCachedThreadPool();
    ConnectionPoolConfig connectionPoolConfig = transportClientConf.getConnPool();
    _connPool = new KeyedPoolImpl<>(connectionPoolConfig.getMinConnectionsPerServer(),
        connectionPoolConfig.getMaxConnectionsPerServer(), connectionPoolConfig.getIdleTimeoutMs(),
        connectionPoolConfig.getMaxBacklogPerServer(), resourceManager, _poolTimeoutExecutor, _requestSenderPool,
        metricsRegistry);
    resourceManager.setPool(_connPool);

    _scatterGather = new ScatterGatherImpl(_connPool, _requestSenderPool);
  }

  @Override
  public synchronized void start() {
    _connPool.start();
    _liveInstanceChangeListener.init(_connPool, CommonConstants.Broker.DEFAULT_BROKER_TIMEOUT_MS);
  }

  @Override
  public synchronized void shutDown() {
    _connPool.shutdown();
    _requestSenderPool.shutdown();
    _poolTimeoutExecutor.shutdown();
    _eventLoopGroup.shutdownGracefully();
  }

  @Override
  protected BrokerResponse processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
      @Nullable BrokerRequest offlineBrokerRequest, @Nullable Map<String, List<String>> offlineRoutingTable,
      @Nullable BrokerRequest realtimeBrokerRequest, @Nullable Map<String, List<String>> realtimeRoutingTable,
      long timeoutMs, ServerStats serverStats) throws Exception {
    ScatterGatherStats scatterGatherStats = new ScatterGatherStats();
    PhaseTimes phaseTimes = new PhaseTimes();

    // Step 1: scatter request to the servers
    String offlineTableName = null;
    String realtimeTableName = null;
    CompositeFuture<byte[]> offlineCompositeFuture = null;
    CompositeFuture<byte[]> realtimeCompositeFuture = null;
    if (offlineBrokerRequest != null) {
      assert offlineRoutingTable != null;
      offlineTableName = offlineBrokerRequest.getQuerySource().getTableName();
      offlineCompositeFuture =
          scatterBrokerRequest(requestId, offlineBrokerRequest, offlineRoutingTable, true, timeoutMs,
              scatterGatherStats, phaseTimes);
    }
    if (realtimeBrokerRequest != null) {
      assert realtimeRoutingTable != null;
      realtimeTableName = realtimeBrokerRequest.getQuerySource().getTableName();
      realtimeCompositeFuture =
          scatterBrokerRequest(requestId, realtimeBrokerRequest, realtimeRoutingTable, false, timeoutMs,
              scatterGatherStats, phaseTimes);
    }

    // Step 2: gather response from the servers
    int numServersQueried = 0;
    long gatherStartTimeNs = System.nanoTime();
    List<ProcessingException> processingExceptions = new ArrayList<>();
    Map<ServerInstance, byte[]> offlineServerResponseMap = null;
    Map<ServerInstance, byte[]> realtimeServerResponseMap = null;
    if (offlineCompositeFuture != null) {
      numServersQueried += offlineCompositeFuture.getNumFutures();
      offlineServerResponseMap =
          gatherServerResponses(offlineCompositeFuture, scatterGatherStats, true, offlineTableName,
              processingExceptions);
    }
    if (realtimeCompositeFuture != null) {
      numServersQueried += realtimeCompositeFuture.getNumFutures();
      realtimeServerResponseMap =
          gatherServerResponses(realtimeCompositeFuture, scatterGatherStats, false, realtimeTableName,
              processingExceptions);
    }
    if ((offlineServerResponseMap == null) && (realtimeServerResponseMap == null)) {
      // No response gathered
      return new BrokerResponseNative(processingExceptions);
    }
    long gatherEndTimeNs = System.nanoTime();
    phaseTimes.addToGatherTime(gatherEndTimeNs - gatherStartTimeNs);
    serverStats.setServerStats(scatterGatherStats.toString());

    //Step 3: deserialize the server responses
    int numServersResponded = 0;
    Map<ServerInstance, DataTable> dataTableMap = new HashMap<>();
    // Add a long variable to sum the total response sizes from both realtime and offline servers.
    long totalServerResponseSize = 0;
    if (offlineServerResponseMap != null) {
      numServersResponded += offlineServerResponseMap.size();
      totalServerResponseSize +=
          deserializeServerResponses(offlineServerResponseMap, true, dataTableMap, offlineTableName,
              processingExceptions);
    }
    if (realtimeServerResponseMap != null) {
      numServersResponded += realtimeServerResponseMap.size();
      totalServerResponseSize +=
          deserializeServerResponses(realtimeServerResponseMap, false, dataTableMap, realtimeTableName,
              processingExceptions);
    }
    long deserializationEndTimeNs = System.nanoTime();
    phaseTimes.addToDeserializationTime(deserializationEndTimeNs - gatherEndTimeNs);

    // Step 4: reduce (merge) the server responses and create a broker response to be returned
    BrokerResponse brokerResponse =
        _brokerReduceService.reduceOnDataTable(originalBrokerRequest, dataTableMap, _brokerMetrics);
    phaseTimes.addToReduceTime(System.nanoTime() - deserializationEndTimeNs);

    // Set processing exceptions and number of servers queried/responded
    brokerResponse.setExceptions(processingExceptions);
    brokerResponse.setNumServersQueried(numServersQueried);
    brokerResponse.setNumServersResponded(numServersResponded);

    // Update broker metrics
    String rawTableName = TableNameBuilder.extractRawTableName(originalBrokerRequest.getQuerySource().getTableName());
    phaseTimes.addPhaseTimesToBrokerMetrics(rawTableName);
    if (brokerResponse.getExceptionsSize() > 0) {
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_PROCESSING_EXCEPTIONS, 1);
    }
    if (numServersQueried > numServersResponded) {
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED, 1);
    }
    _brokerMetrics.addMeteredQueryValue(originalBrokerRequest, BrokerMeter.TOTAL_SERVER_RESPONSE_SIZE,
        totalServerResponseSize);

    return brokerResponse;
  }

  /**
   * Scatter broker request to the servers.
   *
   * @return composite future used to gather responses.
   */
  private CompositeFuture<byte[]> scatterBrokerRequest(long requestId, BrokerRequest brokerRequest,
      Map<String, List<String>> routingTable, boolean isOfflineTable, long timeoutMs,
      ScatterGatherStats scatterGatherStats, PhaseTimes phaseTimes) throws InterruptedException {
    long scatterStartTimeNs = System.nanoTime();
    ScatterGatherRequest scatterRequest =
        new ScatterGatherRequestImpl(brokerRequest, routingTable, requestId, timeoutMs, _brokerId);
    CompositeFuture<byte[]> compositeFuture =
        _scatterGather.scatterGather(scatterRequest, scatterGatherStats, isOfflineTable, _brokerMetrics);
    phaseTimes.addToScatterTime(System.nanoTime() - scatterStartTimeNs);
    return compositeFuture;
  }

  /**
   * Gather responses from servers, append processing exceptions to the processing exception list passed in.
   *
   * @param compositeFuture composite future returned from scatter phase.
   * @param scatterGatherStats scatter-gather statistics.
   * @param isOfflineTable whether the scatter-gather target is an OFFLINE table.
   * @param tableNameWithType table name with type suffix.
   * @param processingExceptions list of processing exceptions.
   * @return server response map.
   */
  private Map<ServerInstance, byte[]> gatherServerResponses(CompositeFuture<byte[]> compositeFuture,
      ScatterGatherStats scatterGatherStats, boolean isOfflineTable, String tableNameWithType,
      List<ProcessingException> processingExceptions) {
    try {
      Map<ServerInstance, byte[]> serverResponseMap = compositeFuture.get();
      Iterator<Entry<ServerInstance, byte[]>> iterator = serverResponseMap.entrySet().iterator();
      while (iterator.hasNext()) {
        Entry<ServerInstance, byte[]> entry = iterator.next();
        if (entry.getValue().length == 0) {
          LOGGER.warn("Got empty response from server: {]", entry.getKey().getShortHostName());
          iterator.remove();
        }
      }
      Map<ServerInstance, Long> responseTimes = compositeFuture.getResponseTimes();
      scatterGatherStats.setResponseTimeMillis(responseTimes, isOfflineTable);
      return serverResponseMap;
    } catch (Exception e) {
      LOGGER.error("Caught exception while fetching responses for table: {}", tableNameWithType, e);
      _brokerMetrics.addMeteredTableValue(tableNameWithType, BrokerMeter.RESPONSE_FETCH_EXCEPTIONS, 1);
      processingExceptions.add(QueryException.getException(QueryException.BROKER_GATHER_ERROR, e));
      return null;
    }
  }

  /**
   * De-serialize the server responses, put the de-serialized data table into the data table map passed in, append
   * processing exceptions to the processing exception list passed in, and return the total response size from pinot
   * servers.
   * <p>For hybrid use case, multiple responses might be from the same instance. Use response sequence to distinguish
   * them.
   *
   * @param responseMap map from server to response.
   * @param isOfflineTable whether the responses are from an OFFLINE table.
   * @param dataTableMap map from server to data table.
   * @param tableNameWithType table name with type suffix.
   * @param processingExceptions list of processing exceptions.
   * @return total server response size.
   */
  private long deserializeServerResponses(Map<ServerInstance, byte[]> responseMap, boolean isOfflineTable,
      Map<ServerInstance, DataTable> dataTableMap, String tableNameWithType,
      List<ProcessingException> processingExceptions) {
    long totalResponseSize = 0L;
    for (Entry<ServerInstance, byte[]> entry : responseMap.entrySet()) {
      ServerInstance serverInstance = entry.getKey();
      if (!isOfflineTable) {
        serverInstance = serverInstance.withSeq(1);
      }
      byte[] responseInBytes = entry.getValue();
      totalResponseSize += responseInBytes.length;
      try {
        dataTableMap.put(serverInstance, DataTableFactory.getDataTable(responseInBytes));
      } catch (Exception e) {
        LOGGER.error("Caught exceptions while deserializing response for table: {} from server: {}", tableNameWithType,
            serverInstance, e);
        _brokerMetrics.addMeteredTableValue(tableNameWithType, BrokerMeter.DATA_TABLE_DESERIALIZATION_EXCEPTIONS, 1);
        processingExceptions.add(QueryException.getException(QueryException.DATA_TABLE_DESERIALIZATION_ERROR, e));
      }
    }
    return totalResponseSize;
  }

  /**
   * Container for time statistics in all phases.
   */
  private class PhaseTimes {
    private long _scatterTimeNs = 0L;
    private long _gatherTimeNs = 0L;
    private long _deserializationTimeNs = 0L;
    private long _reduceTimeNs = 0L;

    public void addToScatterTime(long scatterTimeNs) {
      _scatterTimeNs += scatterTimeNs;
    }

    public void addToGatherTime(long gatherTimeNs) {
      _gatherTimeNs += gatherTimeNs;
    }

    public void addToDeserializationTime(long deserializationTimeNs) {
      _deserializationTimeNs += deserializationTimeNs;
    }

    public void addToReduceTime(long reduceTimeNs) {
      _reduceTimeNs += reduceTimeNs;
    }

    public void addPhaseTimesToBrokerMetrics(String rawTableName) {
      _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.SCATTER_GATHER, _scatterTimeNs + _gatherTimeNs);
      _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.DESERIALIZATION, _deserializationTimeNs);
      _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.REDUCE, _reduceTimeNs);
    }
  }

  private static class ScatterGatherRequestImpl implements ScatterGatherRequest {
    private final BrokerRequest _brokerRequest;
    private final Map<String, List<String>> _routingTable;
    private final long _requestId;
    private final long _requestTimeoutMs;
    private final String _brokerId;

    public ScatterGatherRequestImpl(BrokerRequest request, Map<String, List<String>> routingTable, long requestId,
        long requestTimeoutMs, String brokerId) {
      _brokerRequest = request;
      _routingTable = routingTable;
      _requestId = requestId;
      _requestTimeoutMs = requestTimeoutMs;
      _brokerId = brokerId;
    }

    @Override
    public Map<String, List<String>> getRoutingTable() {
      return _routingTable;
    }

    @Override
    public byte[] getRequestForService(List<String> segments) {
      InstanceRequest r = new InstanceRequest();
      r.setRequestId(_requestId);
      r.setEnableTrace(_brokerRequest.isEnableTrace());
      r.setQuery(_brokerRequest);
      r.setSearchSegments(segments);
      r.setBrokerId(_brokerId);
      // SerDe is not threadsafe
      return new SerDe(new TCompactProtocol.Factory()).serialize(r);
    }

    @Override
    public long getRequestId() {
      return _requestId;
    }

    @Override
    public long getRequestTimeoutMs() {
      return _requestTimeoutMs;
    }

    @Override
    public BrokerRequest getBrokerRequest() {
      return _brokerRequest;
    }
  }
}
