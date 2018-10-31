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
package com.linkedin.pinot.broker.broker;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.broker.broker.helix.LiveInstancesChangeListenerImpl;
import com.linkedin.pinot.broker.pruner.SegmentZKMetadataPrunerService;
import com.linkedin.pinot.broker.requesthandler.BrokerRequestHandler;
import com.linkedin.pinot.broker.routing.CfgBasedRouting;
import com.linkedin.pinot.broker.routing.HelixExternalViewBasedRouting;
import com.linkedin.pinot.broker.routing.RoutingTable;
import com.linkedin.pinot.broker.routing.TimeBoundaryService;
import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.metrics.MetricsHelper;
import com.linkedin.pinot.common.query.ReduceServiceRegistry;
import com.linkedin.pinot.common.response.BrokerResponseFactory;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.core.query.reduce.BrokerReduceService;
import com.linkedin.pinot.transport.conf.TransportClientConf;
import com.linkedin.pinot.transport.conf.TransportClientConf.RoutingMode;
import com.linkedin.pinot.transport.config.ConnectionPoolConfig;
import com.linkedin.pinot.transport.metrics.NettyClientMetrics;
import com.linkedin.pinot.transport.netty.PooledNettyClientResourceManager;
import com.linkedin.pinot.transport.pool.KeyedPool;
import com.linkedin.pinot.transport.pool.KeyedPoolImpl;
import com.linkedin.pinot.transport.scattergather.ScatterGather;
import com.linkedin.pinot.transport.scattergather.ScatterGatherImpl;
import com.yammer.metrics.core.MetricsRegistry;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerServerBuilder {
  private static final String TRANSPORT_CONFIG_PREFIX = "pinot.broker.transport";
  private static final String CLIENT_CONFIG_PREFIX = "pinot.broker.client";
  private static final String METRICS_CONFIG_PREFIX = "pinot.broker.metrics";
  private static final long DEFAULT_BROKER_DELAY_SHUTDOWN_TIME_MS = 10 * 1000L;
  private static final String BROKER_DELAY_SHUTDOWN_TIME_CONFIG = "pinot.broker.delayShutdownTimeMs";
  private static final String PINOT_BROKER_TABLE_LEVEL_METRICS = "pinot.broker.enableTableLevelMetrics";
  private static final String PINOT_BROKER_TABLE_LEVEL_METRICS_LIST = "pinot.broker.tablelevel.metrics.whitelist";
  private static final String BROKER_SEGMENT_PRUNERS = "pinot.broker.segment.pruners";
  private static final String[] DEFAULT_BROKER_SEGMENT_PRUNERS = {};
  private static final String BROKER_ACCESS_CONTROL_PREFIX = "pinot.broker.access.control";

  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerServerBuilder.class);
  // Connection Pool Related
  private KeyedPool<PooledNettyClientResourceManager.PooledClientConnection> _connPool;
  private ScheduledThreadPoolExecutor _poolTimeoutExecutor;
  private ExecutorService _requestSenderPool;

  // Netty Specific
  private EventLoopGroup _eventLoopGroup;
  private PooledNettyClientResourceManager _resourceManager;

  private TimeBoundaryService _timeBoundaryService;

  private RoutingTable _routingTable;

  private ScatterGather _scatterGather;

  private MetricsRegistry _registry;
  private BrokerMetrics _brokerMetrics;
  private AccessControlFactory _accessControlFactory;

  // Broker Request Handler
  private BrokerRequestHandler _requestHandler;

  private long delayedShutdownTimeMs = DEFAULT_BROKER_DELAY_SHUTDOWN_TIME_MS;

  private BrokerAdminApiApplication _brokerAdminApplication;

  private final Configuration _config;
  private final LiveInstancesChangeListenerImpl listener;

  public static enum State {
    INIT,
    STARTING,
    RUNNING,
    SHUTTING_DOWN,
    SHUTDOWN
  }

  // Running State Of broker
  private AtomicReference<State> _state = new AtomicReference<State>();

  public BrokerServerBuilder(Configuration configuration, HelixExternalViewBasedRouting helixExternalViewBasedRouting,
      TimeBoundaryService timeBoundaryService, LiveInstancesChangeListenerImpl listener) throws ConfigurationException {
    _config = configuration;
    if (_config.containsKey(BROKER_DELAY_SHUTDOWN_TIME_CONFIG)) {
      delayedShutdownTimeMs = _config.getLong(BROKER_DELAY_SHUTDOWN_TIME_CONFIG, DEFAULT_BROKER_DELAY_SHUTDOWN_TIME_MS);
    }
    _routingTable = helixExternalViewBasedRouting;
    _timeBoundaryService = timeBoundaryService;
    this.listener = listener;
  }

  public void buildNetwork() throws ConfigurationException {
    // build transport
    Configuration transportConfigs = _config.subset(TRANSPORT_CONFIG_PREFIX);
    TransportClientConf conf = new TransportClientConf();
    conf.init(transportConfigs);

    _registry = new MetricsRegistry();
    MetricsHelper.initializeMetrics(_config.subset(METRICS_CONFIG_PREFIX));
    MetricsHelper.registerMetricsRegistry(_registry);
    _brokerMetrics = new BrokerMetrics(_registry, !emitTableLevelMetrics());
    _brokerMetrics.initializeGlobalMeters();
    _state.set(State.INIT);
    _eventLoopGroup = new NioEventLoopGroup();
    /**
     * Some of the client metrics uses histogram which is doing synchronous operation.
     * These are fixed overhead per request/response.
     * TODO: Measure the overhead of this.
     */
    final NettyClientMetrics clientMetrics = new NettyClientMetrics(_registry, "client_");

    // Setup Netty Connection Pool
    _resourceManager = new PooledNettyClientResourceManager(_eventLoopGroup, new HashedWheelTimer(), clientMetrics);
    _poolTimeoutExecutor = new ScheduledThreadPoolExecutor(50);
    // _requestSenderPool = MoreExecutors.sameThreadExecutor();

    _requestSenderPool = Executors.newCachedThreadPool();

    final ConnectionPoolConfig connPoolCfg = conf.getConnPool();

    _connPool = new KeyedPoolImpl<PooledNettyClientResourceManager.PooledClientConnection>(connPoolCfg.getMinConnectionsPerServer(),
        connPoolCfg.getMaxConnectionsPerServer(), connPoolCfg.getIdleTimeoutMs(), connPoolCfg.getMaxBacklogPerServer(),
        _resourceManager, _poolTimeoutExecutor, _requestSenderPool, _registry);
    // MoreExecutors.sameThreadExecutor(), _registry);
    _resourceManager.setPool(_connPool);

    // Setup Routing Table
    if (conf.getRoutingMode() == RoutingMode.CONFIG) {
      final CfgBasedRouting rt = new CfgBasedRouting();
      rt.init(conf.getCfgBasedRouting());
      _routingTable = rt;
    } else {
      // Helix based routing is already initialized.
    }

    // Setup ScatterGather
    _scatterGather = new ScatterGatherImpl(_connPool, _requestSenderPool);

    // Setup the broker pruner service
    String[] prunerNames = _config.getStringArray(BROKER_SEGMENT_PRUNERS);
    if (prunerNames == null) {
      prunerNames = DEFAULT_BROKER_SEGMENT_PRUNERS;
    }
    SegmentZKMetadataPrunerService brokerPrunerService = new SegmentZKMetadataPrunerService(prunerNames);

    // Setup Broker Request Handler
    ReduceServiceRegistry reduceServiceRegistry = buildReduceServiceRegistry();
    _accessControlFactory = AccessControlFactory.loadFactory(_config.subset(BROKER_ACCESS_CONTROL_PREFIX));
    _requestHandler = new BrokerRequestHandler(_routingTable, _timeBoundaryService, _scatterGather,
        reduceServiceRegistry, brokerPrunerService, _brokerMetrics, _config, _accessControlFactory);

    LOGGER.info("Network initialized !!");
  }

  public AccessControlFactory getAccessControlFactory() {
    return _accessControlFactory;
  }

  /**
   * Build the reduce service registry for each broker response.
   */
  private ReduceServiceRegistry buildReduceServiceRegistry() {
    ReduceServiceRegistry reduceServiceRegistry = new ReduceServiceRegistry();
    BrokerReduceService reduceService = new BrokerReduceService();
    reduceServiceRegistry.register(BrokerResponseFactory.ResponseType.BROKER_RESPONSE_TYPE_NATIVE,
        reduceService);

    reduceServiceRegistry.registerDefault(reduceService);
    return reduceServiceRegistry;
  }

  public void buildHTTP() {
    // build server which has servlet
    Configuration c = _config.subset(CLIENT_CONFIG_PREFIX);
    BrokerClientConf clientConfig = new BrokerClientConf(c);

    Preconditions.checkArgument(clientConfig.getQueryPort() > 0);
    URI baseUri = URI.create("http://0.0.0.0:" + Integer.toString(clientConfig.getQueryPort()) + "/");

    _brokerAdminApplication = new BrokerAdminApiApplication(this, _brokerMetrics, _requestHandler, _timeBoundaryService);
    _brokerAdminApplication.start(clientConfig.getQueryPort());
  }

  public void start() throws Exception {
    Utils.logVersions();

    LOGGER.info("Network starting !!");
    if (_state.get() != State.INIT) {
      LOGGER.warn("Network already initialized. Skipping !!");
      return;
    }
    _state.set(State.STARTING);
    _connPool.start();
    _state.set(State.RUNNING);
    if (listener != null) {
      listener.init(_connPool, CommonConstants.Broker.DEFAULT_BROKER_TIMEOUT_MS);
    }
    LOGGER.info("Network running !!");
  }

  public void stop() throws Exception {
    LOGGER.info("Shutting down Network !!");
    try {
      Thread.sleep(delayedShutdownTimeMs);
    } catch (Exception e) {
      LOGGER.error("Interrupted while waiting for shutdown delay period of {} ms.", delayedShutdownTimeMs, e);
    }
    _state.set(State.SHUTTING_DOWN);
    _connPool.shutdown();
    _eventLoopGroup.shutdownGracefully();
    _poolTimeoutExecutor.shutdown();
    _requestSenderPool.shutdown();
    _state.set(State.SHUTDOWN);
    LOGGER.info("Network shutdown!!");

    _brokerAdminApplication.stop();
  }

  public MetricsRegistry getMetricsRegistry()  {
    return _registry;
  }

  public State getCurrentState() {
    return _state.get();
  }

  public RoutingTable getRoutingTable() {
    return _routingTable;
  }

  public BrokerMetrics getBrokerMetrics() {
    return _brokerMetrics;
  }

  public BrokerRequestHandler getBrokerRequestHandler() {
    return _requestHandler;
  }

  private boolean emitTableLevelMetrics() {
    return _config.getBoolean(PINOT_BROKER_TABLE_LEVEL_METRICS, true);
  }
}
