package com.linkedin.pinot.server.starter;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.query.response.ServerInstance;
import com.linkedin.pinot.requestHandler.BrokerRequestHandler;
import com.linkedin.pinot.routing.CfgBasedRouting;
import com.linkedin.pinot.routing.RoutingTable;
import com.linkedin.pinot.server.conf.BrokerConf;
import com.linkedin.pinot.server.conf.BrokerConf.RoutingMode;
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
 * 
 * Starts Broker 
 * @author bvaradar
 *
 */
public class BrokerStarter {

  /** Static members **/
  private static Logger LOGGER = LoggerFactory.getLogger(BrokerStarter.class);
  private static final String BROKER_CONFIG_OPT_NAME = "broker_conf";

  // Broker Config File Path
  private static String _brokerConfigPath;

  // Connection Pool Related
  private KeyedPool<ServerInstance, NettyClientConnection> _connPool;
  private ScheduledThreadPoolExecutor _poolTimeoutExecutor;
  private ThreadPoolExecutor _connPoolThreads;

  // Netty Specific
  private EventLoopGroup _eventLoopGroup;
  private PooledNettyClientResourceManager _resourceManager;

  private RoutingTable _routingTable;

  private ScatterGather _scatterGather;

  private MetricsRegistry _registry;

  // Broker Request Handler
  private BrokerRequestHandler _requestHandler;

  public static enum State {
    INIT,
    STARTING,
    RUNNING,
    SHUTTING_DOWN,
    SHUTDOWN
  }

  // Running State Of broker
  private State _state;

  public void init(BrokerConf conf) throws ConfigurationException {
    LOGGER.info("Initializing broker with config (" + conf + ")");
    _registry = new MetricsRegistry();
    _state = State.INIT;
    _eventLoopGroup = new NioEventLoopGroup();
    final NettyClientMetrics clientMetrics = new NettyClientMetrics(_registry, "client_");

    // Setup Netty Connection Pool
    _resourceManager = new PooledNettyClientResourceManager(_eventLoopGroup, new HashedWheelTimer(), clientMetrics);
    _poolTimeoutExecutor = new ScheduledThreadPoolExecutor(1);
    final ConnectionPoolConfig cfg = conf.getConnPool();
    _connPoolThreads =
        new ThreadPoolExecutor(cfg.getThreadPool().getCorePoolSize(), cfg.getThreadPool().getMaxPoolSize(), cfg
            .getThreadPool().getIdleTimeoutMs(), TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    _connPool =
        new KeyedPoolImpl<ServerInstance, NettyClientConnection>(conf.getConnPool(), _resourceManager,
            _connPoolThreads, _poolTimeoutExecutor, _registry);
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
    _scatterGather = new ScatterGatherImpl(_connPool);

    // Setup Broker Request Handler
    _requestHandler = new BrokerRequestHandler(_routingTable, _scatterGather);

    //TODO: Start Broker Server : Code goes here. Broker Server part should use request handler to submit requests

    LOGGER.info("Broker initialized !!");
  }

  public void start() {
    LOGGER.info("Broker starting !!");
    if (_state != State.INIT) {
      Log.warn("Broker already initialized. Skipping !!");
      return;
    }
    _state = State.STARTING;
    _connPool.start();
    _routingTable.start();
    _state = State.RUNNING;
    LOGGER.info("Broker running !!");
  }

  public void shutdown() {
    LOGGER.info("Shutting down broker !!");

    _state = State.SHUTTING_DOWN;
    _connPool.shutdown();
    _eventLoopGroup.shutdownGracefully();
    _routingTable.shutdown();
    _poolTimeoutExecutor.shutdown();
    _connPoolThreads.shutdown();
    _state = State.SHUTDOWN;
    LOGGER.info("Broker shutdown!!");
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {

    //Process Command Line to get config and port
    processCommandLineArgs(args);

    // build  brokerConf
    final PropertiesConfiguration cfg = new PropertiesConfiguration();
    cfg.setDelimiterParsingDisabled(false);
    cfg.load(_brokerConfigPath);
    final Configuration brkConfig = cfg.subset("pinot.broker");
    final BrokerConf conf = new BrokerConf();
    conf.init(brkConfig);

    final BrokerStarter starter = new BrokerStarter();
    starter.init(conf);

    // Add Shutdown Hook
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        Log.info("Running shutdown hook");
        starter.shutdown();
      }
    });

    starter.start();

    // Should invoke Broker server start and wait till it shutsdown

    //TODO: Remove this after integrating with DHaval's code
    Thread.sleep(300 * 1000);

  }

  private static Options buildCommandLineOptions() {
    final Options options = new Options();
    options.addOption(BROKER_CONFIG_OPT_NAME, true, "Broker Config file");
    return options;
  }

  private static void processCommandLineArgs(String[] cliArgs) throws ParseException {
    final CommandLineParser cliParser = new GnuParser();
    final Options cliOptions = buildCommandLineOptions();

    final CommandLine cmd = cliParser.parse(cliOptions, cliArgs, true);

    if (!cmd.hasOption(BROKER_CONFIG_OPT_NAME)) {
      System.err.println("Missing required arguments !!");
      System.err.println(cliOptions);
      throw new RuntimeException("Missing required arguments !!");
    }

    _brokerConfigPath = cmd.getOptionValue(BROKER_CONFIG_OPT_NAME);
  }
}
