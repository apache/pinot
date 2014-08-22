package com.linkedin.pinot.broker.broker;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.MoreExecutors;
import com.linkedin.pinot.broker.servlet.PinotBrokerServletContextChangeListener;
import com.linkedin.pinot.broker.servlet.PinotClientRequestServlet;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.core.query.reduce.DefaultReduceService;
import com.linkedin.pinot.requestHandler.BrokerRequestHandler;
import com.linkedin.pinot.routing.CfgBasedRouting;
import com.linkedin.pinot.routing.RoutingTable;
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
 * @author Dhaval Patel<dpatel@linkedin.com
 * Aug 4, 2014
 */
public class BrokerServerBuilder {
  private static final String TRANSPORT_CONFIG_PREFIX = "pinot.broker.transport";
  private static final String CLIENT_CONFIG_PREFIX = "pinot.broker.client";

  private static Logger LOGGER = LoggerFactory.getLogger(BrokerServerBuilder.class);

  private static final String BROKER_CONFIG_OPT_NAME = "broker_conf";

  // Connection Pool Related
  private KeyedPool<ServerInstance, NettyClientConnection> _connPool;
  private ScheduledThreadPoolExecutor _poolTimeoutExecutor;
  private ExecutorService _requestSenderPool;

  // Netty Specific
  private EventLoopGroup _eventLoopGroup;
  private PooledNettyClientResourceManager _resourceManager;

  private RoutingTable _routingTable;

  private ScatterGather _scatterGather;

  private MetricsRegistry _registry;

  // Broker Request Handler
  private BrokerRequestHandler _requestHandler;

  private int port;
  private Server server;
  private final PropertiesConfiguration config;

  public static enum State {
    INIT,
    STARTING,
    RUNNING,
    SHUTTING_DOWN,
    SHUTDOWN
  }

  // Running State Of broker
  private State _state;

  public BrokerServerBuilder(PropertiesConfiguration configuration) throws ConfigurationException {
    config = configuration;
  }

  public void buildNetwork() throws ConfigurationException {
    // build transport
    Configuration transportConfigs = config.subset(TRANSPORT_CONFIG_PREFIX);
    TransportClientConf conf = new TransportClientConf();
    conf.init(transportConfigs);

    _registry = new MetricsRegistry();
    _state = State.INIT;
    _eventLoopGroup = new NioEventLoopGroup();
    /**
     * Some of the client metrics uses histogram which is doing synchronous operation. 
     * These are fixed overhead per request/response.
     * TODO: Measure the overhead of this.
     */
    final NettyClientMetrics clientMetrics = new NettyClientMetrics(_registry, "client_");

    // Setup Netty Connection Pool
    _resourceManager = new PooledNettyClientResourceManager(_eventLoopGroup, new HashedWheelTimer(), clientMetrics);
    _poolTimeoutExecutor = new ScheduledThreadPoolExecutor(1);
    _requestSenderPool  = MoreExecutors.sameThreadExecutor();
    final ConnectionPoolConfig cfg = conf.getConnPool();
    _requestSenderPool =
        new ThreadPoolExecutor(cfg.getThreadPool().getCorePoolSize(), cfg.getThreadPool().getMaxPoolSize(), cfg
            .getThreadPool().getIdleTimeoutMs(), TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    ConnectionPoolConfig connPoolCfg = conf.getConnPool();

    _connPool =
        new KeyedPoolImpl<ServerInstance, NettyClientConnection>(connPoolCfg.getMinConnectionsPerServer(),
            connPoolCfg.getMaxConnectionsPerServer(),
            connPoolCfg.getIdleTimeoutMs(),
            connPoolCfg.getMaxBacklogPerServer(),
            _resourceManager,
            _poolTimeoutExecutor,
            MoreExecutors.sameThreadExecutor(), _registry);
    _resourceManager.setPool(_connPool);

    // Setup Routing Table
    if (conf.getRoutingMode() == RoutingMode.CONFIG) {
      final CfgBasedRouting rt = new CfgBasedRouting();
      rt.init(conf.getCfgBasedRouting());
      _routingTable = rt;
    } else {
      throw new ConfigurationException("Helix based routing not yet implemented !!");
    }

    // Setup ScatterGather
    _scatterGather = new ScatterGatherImpl(_connPool,_requestSenderPool);

    // Setup Broker Request Handler
    _requestHandler = new BrokerRequestHandler(_routingTable, _scatterGather, new DefaultReduceService());

    //TODO: Start Broker Server : Code goes here. Broker Server part should use request handler to submit requests

    LOGGER.info("Network initialized !!");
  }

  public void buildHTTP() {
    // build server which has servlet
    Configuration c = config.subset(CLIENT_CONFIG_PREFIX);
    BrokerClientConf clientConfig = new BrokerClientConf(c);

    server = new Server(clientConfig.getQueryPort());

    WebAppContext context = new WebAppContext();
    context.addServlet(PinotClientRequestServlet.class, "/query");

    if (clientConfig.enableConsole()) {
      context.setResourceBase(clientConfig.getConsoleWebappPath());
    } else {
      context.setResourceBase("");
    }

    context.addEventListener(new PinotBrokerServletContextChangeListener(_requestHandler));

    server.setHandler(context);
  }

  public void start() throws Exception {
    LOGGER.info("Network starting !!");
    if (_state != State.INIT) {
      LOGGER.warn("Network already initialized. Skipping !!");
      return;
    }
    _state = State.STARTING;
    _connPool.start();
    _routingTable.start();
    _state = State.RUNNING;
    LOGGER.info("Network running !!");

    LOGGER.info("Starting Jetty server !!");
    server.start();
    LOGGER.info("Started Jetty server !!");
  }

  public void stop() throws Exception {
    LOGGER.info("Shutting down Network !!");

    _state = State.SHUTTING_DOWN;
    _connPool.shutdown();
    _eventLoopGroup.shutdownGracefully();
    _routingTable.shutdown();
    _poolTimeoutExecutor.shutdown();
    _requestSenderPool.shutdown();
    _state = State.SHUTDOWN;
    LOGGER.info("Network shutdown!!");

    LOGGER.info("Stopping Jetty server !!");
    server.stop();
    LOGGER.info("Stopped Jetty server !!");
  }
}
