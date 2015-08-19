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
package com.linkedin.pinot.broker.broker;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.broker.broker.helix.LiveInstancesChangeListenerImpl;
import com.linkedin.pinot.broker.servlet.PinotBrokerServletContextChangeListener;
import com.linkedin.pinot.broker.servlet.PinotClientRequestServlet;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.metrics.MetricsHelper;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.core.query.reduce.DefaultReduceService;
import com.linkedin.pinot.requestHandler.BrokerRequestHandler;
import com.linkedin.pinot.routing.CfgBasedRouting;
import com.linkedin.pinot.routing.HelixExternalViewBasedRouting;
import com.linkedin.pinot.routing.RoutingTable;
import com.linkedin.pinot.routing.TimeBoundaryService;
import com.linkedin.pinot.transport.conf.TransportClientConf;
import com.linkedin.pinot.transport.conf.TransportClientConf.RoutingMode;
import com.linkedin.pinot.transport.config.ConnectionPoolConfig;
import com.linkedin.pinot.transport.metrics.NettyClientMetrics;
import com.linkedin.pinot.transport.netty.NettyClientConnection;
import com.linkedin.pinot.transport.netty.PooledNettyClientResourceManager;
import com.linkedin.pinot.transport.pool.KeyedPool;
import com.linkedin.pinot.transport.pool.KeyedPoolImpl;
import com.linkedin.pinot.transport.scattergather.ScatterGather;
import com.linkedin.pinot.transport.scattergather.ScatterGatherImpl;
import com.yammer.metrics.core.MetricsRegistry;


/**
 * Aug 4, 2014
 */
public class BrokerServerBuilder {
  private static final String TRANSPORT_CONFIG_PREFIX = "pinot.broker.transport";
  private static final String CLIENT_CONFIG_PREFIX = "pinot.broker.client";
  private static final String METRICS_CONFIG_PREFIX = "pinot.broker.metrics";
  private static final String BROKER_TIME_OUT_CONFIG = "pinot.broker.time.out";

  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerServerBuilder.class);
  private static final long DEFAULT_BROKER_TIME_OUT = 10 * 10 * 1000L;

  // Connection Pool Related
  private KeyedPool<ServerInstance, NettyClientConnection> _connPool;
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

  // Broker Request Handler
  private BrokerRequestHandler _requestHandler;

  private Server _server;
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
    _brokerMetrics = new BrokerMetrics(_registry);
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
    final ConnectionPoolConfig cfg = conf.getConnPool();

    _requestSenderPool = Executors.newCachedThreadPool();

    ConnectionPoolConfig connPoolCfg = conf.getConnPool();

    _connPool =
        new KeyedPoolImpl<ServerInstance, NettyClientConnection>(connPoolCfg.getMinConnectionsPerServer(),
            connPoolCfg.getMaxConnectionsPerServer(), connPoolCfg.getIdleTimeoutMs(),
            connPoolCfg.getMaxBacklogPerServer(), _resourceManager, _poolTimeoutExecutor, _requestSenderPool, _registry);
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

    // Setup Broker Request Handler
    long brokerTimeOut = DEFAULT_BROKER_TIME_OUT;
    if (_config.containsKey(BROKER_TIME_OUT_CONFIG)) {
      try {
        brokerTimeOut = _config.getLong(BROKER_TIME_OUT_CONFIG);
      } catch (Exception e) {
        LOGGER.warn("Caught exception while reading broker timeout from config, using default value", e);
      }
    }
    LOGGER.info("Broker timeout is - " + brokerTimeOut + " ms");

    _requestHandler =
        new BrokerRequestHandler(_routingTable, _timeBoundaryService, _scatterGather, new DefaultReduceService(),
            _brokerMetrics, brokerTimeOut);

    //TODO: Start Broker Server : Code goes here. Broker Server part should use request handler to submit requests

    LOGGER.info("Network initialized !!");
  }

  public void buildHTTP() {
    // build server which has servlet
    Configuration c = _config.subset(CLIENT_CONFIG_PREFIX);
    BrokerClientConf clientConfig = new BrokerClientConf(c);

    _server = new Server(clientConfig.getQueryPort());

    WebAppContext context = new WebAppContext();
    context.addServlet(PinotClientRequestServlet.class, "/query");

    if (clientConfig.enableConsole()) {
      context.setResourceBase(clientConfig.getConsoleWebappPath());
    } else {
      context.setResourceBase("");
    }

    context.addEventListener(new PinotBrokerServletContextChangeListener(_requestHandler, _brokerMetrics));

    _server.setHandler(context);
  }

  public void start() throws Exception {
    LOGGER.info("Network starting !!");
    if (_state.get() != State.INIT) {
      LOGGER.warn("Network already initialized. Skipping !!");
      return;
    }
    _state.set(State.STARTING);
    _connPool.start();
    _routingTable.start();
    _state.set(State.RUNNING);
    if (listener != null) {
      listener.init(_connPool, DEFAULT_BROKER_TIME_OUT);
    }
    LOGGER.info("Network running !!");

    LOGGER.info("Starting Jetty server !!");
    _server.start();
    LOGGER.info("Started Jetty server !!");
  }

  public void stop() throws Exception {
    LOGGER.info("Shutting down Network !!");

    _state.set(State.SHUTTING_DOWN);
    _connPool.shutdown();
    _eventLoopGroup.shutdownGracefully();
    _routingTable.shutdown();
    _poolTimeoutExecutor.shutdown();
    _requestSenderPool.shutdown();
    _state.set(State.SHUTDOWN);
    LOGGER.info("Network shutdown!!");

    LOGGER.info("Stopping Jetty server !!");
    _server.stop();
    LOGGER.info("Stopped Jetty server !!");
  }

  public boolean isStart() {
    return _state.get() == State.STARTING;
  }

  public RoutingTable getRoutingTable() {
    return _routingTable;
  }

}
