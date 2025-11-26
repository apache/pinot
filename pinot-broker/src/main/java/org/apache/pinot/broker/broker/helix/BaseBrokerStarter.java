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
package org.apache.pinot.broker.broker.helix;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.constant.ZkSystemPropertyKeys;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.broker.BrokerAdminApiApplication;
import org.apache.pinot.broker.grpc.BrokerGrpcServer;
import org.apache.pinot.broker.queryquota.HelixExternalViewBasedQueryQuotaManager;
import org.apache.pinot.broker.requesthandler.BaseSingleStageBrokerRequestHandler;
import org.apache.pinot.broker.requesthandler.BrokerRequestHandler;
import org.apache.pinot.broker.requesthandler.BrokerRequestHandlerDelegate;
import org.apache.pinot.broker.requesthandler.BrokerRequestIdGenerator;
import org.apache.pinot.broker.requesthandler.GrpcBrokerRequestHandler;
import org.apache.pinot.broker.requesthandler.MultiStageBrokerRequestHandler;
import org.apache.pinot.broker.requesthandler.MultiStageQueryThrottler;
import org.apache.pinot.broker.requesthandler.SingleConnectionBrokerRequestHandler;
import org.apache.pinot.broker.requesthandler.TimeSeriesRequestHandler;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.broker.routing.FederatedRoutingManager;
import org.apache.pinot.broker.routing.SecondaryBrokerRoutingManager;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.audit.AuditServiceBinder;
import org.apache.pinot.common.config.DefaultClusterConfigChangeHandler;
import org.apache.pinot.common.config.NettyConfig;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.config.provider.ZkTableCache;
import org.apache.pinot.common.cursors.AbstractResponseStore;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.failuredetector.FailureDetectorFactory;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerTimer;
import org.apache.pinot.common.utils.PinotAppConfigs;
import org.apache.pinot.common.utils.ServiceStartableUtils;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.tls.PinotInsecureMode;
import org.apache.pinot.common.utils.tls.TlsUtils;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.core.query.executor.sql.SqlQueryExecutor;
import org.apache.pinot.core.query.utils.rewriter.ResultRewriterFactory;
import org.apache.pinot.core.routing.CrossClusterFederationProvider;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.core.util.ListenerConfigUtil;
import org.apache.pinot.core.util.trace.ContinuousJfrStarter;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.segment.local.function.GroovyFunctionEvaluator;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.accounting.WorkloadBudgetManager;
import org.apache.pinot.spi.cursors.ResponseStoreService;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.spi.services.ServiceStartable;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.sql.parsers.rewriter.QueryRewriterFactory;
import org.apache.pinot.tsdb.spi.PinotTimeSeriesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base class for broker startable implementations
 */
@SuppressWarnings("unused")
public abstract class BaseBrokerStarter implements ServiceStartable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseBrokerStarter.class);

  protected PinotConfiguration _brokerConf;
  protected List<ListenerConfig> _listenerConfigs;
  protected String _clusterName;
  protected List<String> _secondaryClusterNames;
  protected String _zkServers;
  protected Map<String, String> _secondaryZkServers;
  protected String _hostname;
  protected int _port;
  protected int _tlsPort;
  protected int _grpcPort;
  protected String _instanceId;
  protected String _secondaryInstanceId;
  private volatile boolean _isStarting = false;
  private volatile boolean _isShuttingDown = false;

  // Dedicated handler for listening to cluster config changes
  protected final DefaultClusterConfigChangeHandler _defaultClusterConfigChangeHandler =
      new DefaultClusterConfigChangeHandler();

  // TODO To be removed in favor of _defaultClusterConfigChangeHandler to manage config related changes.
  //      Please use this only if you are reliant specifically on the ClusterChangeMediator infra.
  protected final List<ClusterChangeHandler> _clusterConfigChangeHandlers = new ArrayList<>();
  protected final List<ClusterChangeHandler> _idealStateChangeHandlers = new ArrayList<>();
  protected final List<ClusterChangeHandler> _externalViewChangeHandlers = new ArrayList<>();
  protected final List<ClusterChangeHandler> _instanceConfigChangeHandlers = new ArrayList<>();
  protected final List<ClusterChangeHandler> _liveInstanceChangeHandlers = new ArrayList<>();
  // Spectator Helix manager handles the custom change listeners, properties read/write
  protected HelixManager _spectatorHelixManager;
  protected Map<String, HelixManager> _secondarySpectatorHelixManager;
  protected HelixAdmin _helixAdmin;
  protected ZkHelixPropertyStore<ZNRecord> _propertyStore;
  protected HelixDataAccessor _helixDataAccessor;
  protected PinotMetricsRegistry _metricsRegistry;
  protected BrokerMetrics _brokerMetrics;
  protected BrokerRoutingManager _routingManager;
  protected Map<String, SecondaryBrokerRoutingManager> _secondaryRoutingManagers;
  protected FederatedRoutingManager _federatedRoutingManager;
  protected CrossClusterFederationProvider _crossClusterFederationProvider;
  protected AccessControlFactory _accessControlFactory;
  protected BrokerRequestHandler _brokerRequestHandler;
  protected SqlQueryExecutor _sqlQueryExecutor;
  protected BrokerAdminApiApplication _brokerAdminApplication;
  protected ClusterChangeMediator _clusterChangeMediator;
  protected Map<String, ClusterChangeMediator> _secondaryClusterChangeMediator;
  // Participant Helix manager handles Helix functionality such as state transitions and messages
  protected HelixManager _participantHelixManager;
  // Handles the server routing stats.
  protected ServerRoutingStatsManager _serverRoutingStatsManager;
  protected HelixExternalViewBasedQueryQuotaManager _queryQuotaManager;
  protected MultiStageQueryThrottler _multiStageQueryThrottler;
  protected AbstractResponseStore _responseStore;
  protected BrokerGrpcServer _brokerGrpcServer;
  protected FailureDetector _failureDetector;
  protected ThreadAccountant _threadAccountant;

  @Override
  public void init(PinotConfiguration brokerConf)
      throws Exception {
    _brokerConf = brokerConf;
    // Remove all white-spaces from the list of zkServers (if any).
    _zkServers = brokerConf.getProperty(Helix.CONFIG_OF_ZOOKEEPER_SERVER).replaceAll("\\s+", "");
    _clusterName = brokerConf.getProperty(Helix.CONFIG_OF_CLUSTER_NAME);
    initSecondaryClusterNamesAndZk(brokerConf);

    ServiceStartableUtils.applyClusterConfig(_brokerConf, _zkServers, _clusterName, ServiceRole.BROKER);
    applyCustomConfigs(brokerConf);

    PinotInsecureMode.setPinotInInsecureMode(
        _brokerConf.getProperty(CommonConstants.CONFIG_OF_PINOT_INSECURE_MODE, false));

    if (_brokerConf.getProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT,
        MultiStageQueryRunner.DEFAULT_QUERY_RUNNER_PORT) == 0) {
      _brokerConf.setProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, NetUtils.findOpenPort());
    }
    setupHelixSystemProperties();
    _listenerConfigs = ListenerConfigUtil.buildBrokerConfigs(brokerConf);
    _hostname = brokerConf.getProperty(Broker.CONFIG_OF_BROKER_HOSTNAME);
    if (_hostname == null) {
      _hostname =
          _brokerConf.getProperty(Helix.SET_INSTANCE_ID_TO_HOSTNAME_KEY, false) ? NetUtils.getHostnameOrAddress()
              : NetUtils.getHostAddress();
    }
    // Override multi-stage query runner hostname if not set explicitly
    if (!_brokerConf.containsKey(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME)) {
      _brokerConf.setProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME, _hostname);
    }
    _port = _listenerConfigs.get(0).getPort();
    _tlsPort = ListenerConfigUtil.findLastTlsPort(_listenerConfigs, -1);
    _grpcPort = _brokerConf.getProperty(CommonConstants.Broker.Grpc.KEY_OF_GRPC_PORT, -1);

    _instanceId = _brokerConf.getProperty(Broker.CONFIG_OF_BROKER_ID);
    if (_instanceId == null) {
      _instanceId = _brokerConf.getProperty(Helix.Instance.INSTANCE_ID_KEY);
    }
    if (_instanceId == null) {
      _instanceId = Helix.PREFIX_OF_BROKER_INSTANCE + _hostname + "_" + _port;
    }
    _secondaryInstanceId = _instanceId + "_secondary";
    // NOTE: Force all instances to have the same prefix in order to derive the instance type based on the instance id
    Preconditions.checkState(InstanceTypeUtils.isBroker(_instanceId), "Instance id must have prefix '%s', got '%s'",
        Helix.PREFIX_OF_BROKER_INSTANCE, _instanceId);

    _brokerConf.setProperty(Broker.CONFIG_OF_BROKER_ID, _instanceId);
  }

  private void initSecondaryClusterNamesAndZk(PinotConfiguration brokerConf) {
    LOGGER.info("[federation] Initializing secondary cluster configuration");
    String secondaryClusterNames = brokerConf.getProperty(Helix.CONFIG_OF_SECONDARY_CLUSTER_NAME);

    if (secondaryClusterNames == null || secondaryClusterNames.trim().isEmpty()) {
      LOGGER.info("[federation] No secondary cluster configured - federation is disabled");
      return;
    }

    _secondaryClusterNames = Arrays.asList(secondaryClusterNames.replaceAll("\\s+", "").split(","));
    if (_secondaryClusterNames.isEmpty()) {
      LOGGER.warn("[federation] Secondary cluster names list is empty after parsing");
      return;
    }
    LOGGER.info("[federation] Configured secondary cluster names: {}", _secondaryClusterNames);

    _secondaryZkServers = new HashMap<>();
    for (String name : _secondaryClusterNames) {
      String zkConfig = String.format(Helix.CONFIG_OF_SECONDARY_ZOOKEEPER_SERVER, name);
      String zkServers = brokerConf.getProperty(zkConfig);

      if (zkServers == null || zkServers.trim().isEmpty()) {
        LOGGER.error("[federation] Missing ZooKeeper configuration for cluster '{}', expected: {}", name, zkConfig);
        continue;
      }
      _secondaryZkServers.put(name, zkServers.replaceAll("\\s+", ""));
    }

    if (_secondaryZkServers.isEmpty()) {
      LOGGER.error("[federation] No valid ZooKeeper configurations found - federation will not be functional");
      _secondaryClusterNames = null;
    } else {
      LOGGER.info("[federation] Initialized {} secondary cluster(s): {}", _secondaryZkServers.size(),
          _secondaryZkServers.keySet());
    }
  }

  /// Can be overridden to apply custom configs to the broker conf.
  protected void applyCustomConfigs(PinotConfiguration brokerConf) {
  }

  private void setupHelixSystemProperties() {
    // NOTE: Helix will disconnect the manager and disable the instance if it detects flapping (too frequent disconnect
    // from ZooKeeper). Setting flapping time window to a small value can avoid this from happening. Helix ignores the
    // non-positive value, so set the default value as 1.
    System.setProperty(SystemPropertyKeys.FLAPPING_TIME_WINDOW,
        _brokerConf.getProperty(Helix.CONFIG_OF_BROKER_FLAPPING_TIME_WINDOW_MS, Helix.DEFAULT_FLAPPING_TIME_WINDOW_MS));
  }

  public int getPort() {
    return _port;
  }

  /**
   * Adds an ideal state change handler to handle Helix ideal state change callbacks.
   * <p>NOTE: all change handlers will be run in a single thread, so any slow change handler can block other change
   * handlers from running. For slow change handler, make it asynchronous.
   */
  public void addIdealStateChangeHandler(ClusterChangeHandler idealStateChangeHandler) {
    _idealStateChangeHandlers.add(idealStateChangeHandler);
  }

  /**
   * Adds an external view change handler to handle Helix external view change callbacks.
   * <p>NOTE: all change handlers will be run in a single thread, so any slow change handler can block other change
   * handlers from running. For slow change handler, make it asynchronous.
   */
  public void addExternalViewChangeHandler(ClusterChangeHandler externalViewChangeHandler) {
    _externalViewChangeHandlers.add(externalViewChangeHandler);
  }

  /**
   * Adds an instance config change handler to handle Helix instance config change callbacks.
   * <p>NOTE: all change handlers will be run in a single thread, so any slow change handler can block other change
   * handlers from running. For slow change handler, make it asynchronous.
   */
  public void addInstanceConfigChangeHandler(ClusterChangeHandler instanceConfigChangeHandler) {
    _instanceConfigChangeHandlers.add(instanceConfigChangeHandler);
  }

  /**
   * Adds a cluster config change handler to handle Helix cluster config change callbacks.
   * <p>NOTE: all change handlers will be run in a single thread, so any slow change handler can block other change
   * handlers from running. For slow change handler, make it asynchronous.
   */
  public void addClusterConfigChangeHandler(ClusterChangeHandler clusterConfigChangeHandler) {
    _clusterConfigChangeHandlers.add(clusterConfigChangeHandler);
  }

  /**
   * Adds a live instance change handler to handle Helix live instance change callbacks.
   * <p>NOTE: all change handlers will be run in a single thread, so any slow change handler can block other change
   * handlers from running. For slow change handler, make it asynchronous.
   */
  public void addLiveInstanceChangeHandler(ClusterChangeHandler liveInstanceChangeHandler) {
    _liveInstanceChangeHandlers.add(liveInstanceChangeHandler);
  }

  @Override
  public ServiceRole getServiceRole() {
    return ServiceRole.BROKER;
  }

  @Override
  public String getInstanceId() {
    return _instanceId;
  }

  @Override
  public PinotConfiguration getConfig() {
    return _brokerConf;
  }

  @Override
  public void start()
      throws Exception {
    LOGGER.info("Starting Pinot broker (Version: {})", PinotVersion.VERSION);
    LOGGER.info("Broker configs: {}", new PinotAppConfigs(getConfig()).toJSONString());
    long startTimeMs = System.currentTimeMillis();
    _isStarting = true;
    Utils.logVersions();

    LOGGER.info("Connecting spectator Helix manager");
    _spectatorHelixManager =
        HelixManagerFactory.getZKHelixManager(_clusterName, _instanceId, InstanceType.SPECTATOR, _zkServers);
    _spectatorHelixManager.connect();

    LOGGER.info("Connecting secondary spectator Helix managers");
    initSecondaryClusterSpectatorHelixManagers();

    _helixAdmin = _spectatorHelixManager.getClusterManagmentTool();
    _propertyStore = _spectatorHelixManager.getHelixPropertyStore();
    _helixDataAccessor = _spectatorHelixManager.getHelixDataAccessor();

    LOGGER.info("Setting up broker request handler");
    // Set up metric registry and broker metrics
    _metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry(_brokerConf.subset(Broker.METRICS_CONFIG_PREFIX));
    _brokerMetrics = new BrokerMetrics(
        _brokerConf.getProperty(Broker.CONFIG_OF_METRICS_NAME_PREFIX, Broker.DEFAULT_METRICS_NAME_PREFIX),
        _metricsRegistry,
        _brokerConf.getProperty(Broker.CONFIG_OF_ENABLE_TABLE_LEVEL_METRICS, Broker.DEFAULT_ENABLE_TABLE_LEVEL_METRICS),
        _brokerConf.getProperty(Broker.CONFIG_OF_ALLOWED_TABLES_FOR_EMITTING_METRICS, Collections.emptyList()));
    _brokerMetrics.initializeGlobalMeters();
    _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.VERSION, PinotVersion.VERSION_METRIC_NAME, 1);
    _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.ZK_JUTE_MAX_BUFFER,
        Integer.getInteger(ZkSystemPropertyKeys.JUTE_MAXBUFFER, 0xfffff));
    _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.ADAPTIVE_SERVER_SELECTOR_TYPE,
        _brokerConf.getProperty(Broker.AdaptiveServerSelector.CONFIG_OF_TYPE,
            Broker.AdaptiveServerSelector.DEFAULT_TYPE), 1);
    BrokerMetrics.register(_brokerMetrics);
    // Set up request handling classes
    _serverRoutingStatsManager = new ServerRoutingStatsManager(_brokerConf, _brokerMetrics);
    _serverRoutingStatsManager.init();
    _routingManager = new BrokerRoutingManager(_brokerMetrics, _serverRoutingStatsManager, _brokerConf);
    _routingManager.init(_spectatorHelixManager);

    initSecondaryClusterFederatedRoutingManager();

    final PinotConfiguration factoryConf = _brokerConf.subset(Broker.ACCESS_CONTROL_CONFIG_PREFIX);
    // Adding cluster name to the config so that it can be used by the AccessControlFactory
    factoryConf.setProperty(Helix.CONFIG_OF_CLUSTER_NAME,
        _brokerConf.getProperty(Helix.CONFIG_OF_CLUSTER_NAME));
    _accessControlFactory = AccessControlFactory.loadFactory(factoryConf, _propertyStore);
    _queryQuotaManager = new HelixExternalViewBasedQueryQuotaManager(_brokerMetrics, _instanceId);
    _queryQuotaManager.init(_spectatorHelixManager);
    // Initialize QueryRewriterFactory
    LOGGER.info("Initializing QueryRewriterFactory");
    QueryRewriterFactory.init(_brokerConf.getProperty(Broker.CONFIG_OF_BROKER_QUERY_REWRITER_CLASS_NAMES));
    LOGGER.info("Initializing ResultRewriterFactory");
    ResultRewriterFactory.init(_brokerConf.getProperty(Broker.CONFIG_OF_BROKER_RESULT_REWRITER_CLASS_NAMES));
    // Initialize FunctionRegistry before starting the broker request handler
    FunctionRegistry.init();
    boolean caseInsensitive =
        _brokerConf.getProperty(Helix.ENABLE_CASE_INSENSITIVE_KEY, Helix.DEFAULT_ENABLE_CASE_INSENSITIVE);

    TableCache tableCache = new ZkTableCache(_propertyStore, caseInsensitive);
    initSecondaryClusterFederationProvider(tableCache, caseInsensitive, _routingManager, _federatedRoutingManager);


    LOGGER.info("Initializing Broker Event Listener Factory");
    BrokerQueryEventListenerFactory.init(_brokerConf.subset(Broker.EVENT_LISTENER_CONFIG_PREFIX));

    // Initialize the failure detector that removes servers from the broker routing table if they are not healthy
    _failureDetector = FailureDetectorFactory.getFailureDetector(_brokerConf, _brokerMetrics);
    _failureDetector.registerHealthyServerNotifier(
        instanceId -> _routingManager.includeServerToRouting(instanceId));
    _failureDetector.registerUnhealthyServerNotifier(
        instanceId -> _routingManager.excludeServerFromRouting(instanceId));
    _failureDetector.start();

    // Enable/disable thread CPU time measurement through instance config.
    ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(
        _brokerConf.getProperty(CommonConstants.Broker.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT,
            CommonConstants.Broker.DEFAULT_ENABLE_THREAD_CPU_TIME_MEASUREMENT));
    // Enable/disable thread memory allocation tracking through instance config
    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(
        _brokerConf.getProperty(CommonConstants.Broker.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT,
            CommonConstants.Broker.DEFAULT_THREAD_ALLOCATED_BYTES_MEASUREMENT));
    // Initialize workload budget manager and thread resource usage accountant. Workload budget manager must be
    // initialized first because it might be used by the accountant.
    PinotConfiguration schedulerConfig = _brokerConf.subset(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX);
    WorkloadBudgetManager.set(createWorkloadBudgetManager(schedulerConfig));
    _threadAccountant = ThreadAccountantUtils.createAccountant(schedulerConfig, _instanceId,
        org.apache.pinot.spi.config.instance.InstanceType.BROKER);
    _threadAccountant.startWatcherTask();

    // Create Broker request handler.
    String brokerId = _brokerConf.getProperty(Broker.CONFIG_OF_BROKER_ID, getDefaultBrokerId());
    BrokerRequestIdGenerator requestIdGenerator = new BrokerRequestIdGenerator();
    String brokerRequestHandlerType =
        _brokerConf.getProperty(Broker.BROKER_REQUEST_HANDLER_TYPE, Broker.DEFAULT_BROKER_REQUEST_HANDLER_TYPE);
    BaseSingleStageBrokerRequestHandler singleStageBrokerRequestHandler;
    if (brokerRequestHandlerType.equalsIgnoreCase(Broker.GRPC_BROKER_REQUEST_HANDLER_TYPE)) {
      singleStageBrokerRequestHandler =
          new GrpcBrokerRequestHandler(_brokerConf, brokerId, requestIdGenerator, _routingManager,
              _accessControlFactory, _queryQuotaManager, tableCache, _failureDetector, _threadAccountant,
            _crossClusterFederationProvider);
    } else {
      // Default request handler type, i.e. netty
      NettyConfig nettyDefaults = NettyConfig.extractNettyConfig(_brokerConf, Broker.BROKER_NETTY_PREFIX);
      // Configure TLS for netty connection to server
      TlsConfig tlsDefaults = null;
      if (_brokerConf.getProperty(Broker.BROKER_NETTYTLS_ENABLED, false)) {
        tlsDefaults = TlsUtils.extractTlsConfig(_brokerConf, Broker.BROKER_TLS_PREFIX);
      }
      singleStageBrokerRequestHandler =
          new SingleConnectionBrokerRequestHandler(_brokerConf, brokerId, requestIdGenerator, _routingManager,
              _accessControlFactory, _queryQuotaManager, tableCache, nettyDefaults, tlsDefaults,
              _serverRoutingStatsManager, _failureDetector, _threadAccountant, _crossClusterFederationProvider);
    }
    MultiStageBrokerRequestHandler multiStageBrokerRequestHandler = null;
    QueryDispatcher queryDispatcher = null;
    if (_brokerConf.getProperty(Helix.CONFIG_OF_MULTI_STAGE_ENGINE_ENABLED, Helix.DEFAULT_MULTI_STAGE_ENGINE_ENABLED)) {
      _multiStageQueryThrottler = new MultiStageQueryThrottler(_brokerConf);
      _multiStageQueryThrottler.init(_spectatorHelixManager);
      // multi-stage request handler uses both Netty and GRPC ports.
      // worker requires both the "Netty port" for protocol transport; and "GRPC port" for mailbox transport.
      // TODO: decouple protocol and engine selection.
      queryDispatcher = createQueryDispatcher(_brokerConf);
      multiStageBrokerRequestHandler =
          new MultiStageBrokerRequestHandler(_brokerConf, brokerId, requestIdGenerator, _routingManager,
              _accessControlFactory, _queryQuotaManager, tableCache, _multiStageQueryThrottler, _failureDetector,
              _threadAccountant, _crossClusterFederationProvider);
    }
    TimeSeriesRequestHandler timeSeriesRequestHandler = null;
    if (StringUtils.isNotBlank(_brokerConf.getProperty(PinotTimeSeriesConfiguration.getEnabledLanguagesConfigKey()))) {
      Preconditions.checkNotNull(queryDispatcher, "Multistage Engine should be enabled to use time-series engine");
      timeSeriesRequestHandler =
          new TimeSeriesRequestHandler(_brokerConf, brokerId, requestIdGenerator, _routingManager,
              _accessControlFactory, _queryQuotaManager, tableCache, queryDispatcher, _threadAccountant,
            _crossClusterFederationProvider);
    }

    LOGGER.info("Initializing PinotFSFactory");
    PinotFSFactory.init(_brokerConf.subset(CommonConstants.Broker.PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY));

    LOGGER.info("Initialize ResponseStore");
    PinotConfiguration responseStoreConfiguration =
        _brokerConf.subset(CommonConstants.CursorConfigs.PREFIX_OF_CONFIG_OF_RESPONSE_STORE);

    String expirationTime = _brokerConf.getProperty(CommonConstants.CursorConfigs.RESULTS_EXPIRATION_INTERVAL,
        CommonConstants.CursorConfigs.DEFAULT_RESULTS_EXPIRATION_INTERVAL);

    _responseStore = (AbstractResponseStore) ResponseStoreService.getInstance().getResponseStore(
        responseStoreConfiguration.getProperty(CommonConstants.CursorConfigs.RESPONSE_STORE_TYPE,
            CommonConstants.CursorConfigs.DEFAULT_RESPONSE_STORE_TYPE));
    _responseStore.init(responseStoreConfiguration.subset(_responseStore.getType()), _hostname, _port, brokerId,
        _brokerMetrics, expirationTime);

    _brokerRequestHandler =
        new BrokerRequestHandlerDelegate(singleStageBrokerRequestHandler, multiStageBrokerRequestHandler,
            timeSeriesRequestHandler, _responseStore);
    _brokerRequestHandler.start();

    String controllerUrl = _brokerConf.getProperty(Broker.CONTROLLER_URL);
    if (controllerUrl != null) {
      _sqlQueryExecutor = new SqlQueryExecutor(controllerUrl);
    } else {
      _sqlQueryExecutor = new SqlQueryExecutor(_spectatorHelixManager);
    }

    LOGGER.info("Wiring up cluster config change handler with helix");
    _spectatorHelixManager.addClusterfigChangeListener(_defaultClusterConfigChangeHandler);

    LOGGER.info("Starting broker admin application on: {}", ListenerConfigUtil.toString(_listenerConfigs));
    _brokerAdminApplication = createBrokerAdminApp();
    _brokerAdminApplication.start(_listenerConfigs);

    if (BrokerGrpcServer.isEnabled(_brokerConf)) {
      LOGGER.info("Initializing BrokerGrpcServer");
      _brokerGrpcServer = new BrokerGrpcServer(_brokerConf, brokerId, _brokerMetrics, _brokerRequestHandler);
      _brokerGrpcServer.start();
    } else {
      LOGGER.info("BrokerGrpcServer is not enabled");
    }

    LOGGER.info("Initializing cluster change mediator");
    for (ClusterChangeHandler clusterConfigChangeHandler : _clusterConfigChangeHandlers) {
      clusterConfigChangeHandler.init(_spectatorHelixManager);
    }
    _clusterConfigChangeHandlers.add(_queryQuotaManager);
    if (_multiStageQueryThrottler != null) {
      _clusterConfigChangeHandlers.add(_multiStageQueryThrottler);
    }
    for (ClusterChangeHandler idealStateChangeHandler : _idealStateChangeHandlers) {
      idealStateChangeHandler.init(_spectatorHelixManager);
    }
    _idealStateChangeHandlers.add(_routingManager);
    for (ClusterChangeHandler externalViewChangeHandler : _externalViewChangeHandlers) {
      externalViewChangeHandler.init(_spectatorHelixManager);
    }
    _externalViewChangeHandlers.add(_routingManager);
    _externalViewChangeHandlers.add(_queryQuotaManager);
    if (_multiStageQueryThrottler != null) {
      _externalViewChangeHandlers.add(_multiStageQueryThrottler);
    }
    for (ClusterChangeHandler instanceConfigChangeHandler : _instanceConfigChangeHandlers) {
      instanceConfigChangeHandler.init(_spectatorHelixManager);
    }
    _instanceConfigChangeHandlers.add(_routingManager);
    _instanceConfigChangeHandlers.add(_queryQuotaManager);
    for (ClusterChangeHandler liveInstanceChangeHandler : _liveInstanceChangeHandlers) {
      liveInstanceChangeHandler.init(_spectatorHelixManager);
    }
    Map<ChangeType, List<ClusterChangeHandler>> clusterChangeHandlersMap = new HashMap<>();
    clusterChangeHandlersMap.put(ChangeType.CLUSTER_CONFIG, _clusterConfigChangeHandlers);
    clusterChangeHandlersMap.put(ChangeType.IDEAL_STATE, _idealStateChangeHandlers);
    clusterChangeHandlersMap.put(ChangeType.EXTERNAL_VIEW, _externalViewChangeHandlers);
    clusterChangeHandlersMap.put(ChangeType.INSTANCE_CONFIG, _instanceConfigChangeHandlers);
    if (!_liveInstanceChangeHandlers.isEmpty()) {
      clusterChangeHandlersMap.put(ChangeType.LIVE_INSTANCE, _liveInstanceChangeHandlers);
    }
    _clusterChangeMediator = new ClusterChangeMediator(_instanceId, clusterChangeHandlersMap, _brokerMetrics);
    _clusterChangeMediator.start();
    _spectatorHelixManager.addIdealStateChangeListener(_clusterChangeMediator);
    _spectatorHelixManager.addExternalViewChangeListener(_clusterChangeMediator);
    _spectatorHelixManager.addInstanceConfigChangeListener(_clusterChangeMediator);
    _spectatorHelixManager.addClusterfigChangeListener(_clusterChangeMediator);
    if (!_liveInstanceChangeHandlers.isEmpty()) {
      _spectatorHelixManager.addLiveInstanceChangeListener(_clusterChangeMediator);
    }

    initSecondaryClusterChangeMediator();

    LOGGER.info("Connecting participant Helix manager");
    _participantHelixManager =
        HelixManagerFactory.getZKHelixManager(_clusterName, _instanceId, InstanceType.PARTICIPANT, _zkServers);
    // Register state model factory
    _participantHelixManager.getStateMachineEngine()
        .registerStateModelFactory(BrokerResourceOnlineOfflineStateModelFactory.getStateModelDef(),
            new BrokerResourceOnlineOfflineStateModelFactory(_propertyStore, _helixDataAccessor, _routingManager,
                _queryQuotaManager));
    // Register user-define message handler factory
    _participantHelixManager.getMessagingService()
        .registerMessageHandlerFactory(Message.MessageType.USER_DEFINE_MSG.toString(),
            new BrokerUserDefinedMessageHandlerFactory(_routingManager, _queryQuotaManager));
    _participantHelixManager.connect();

    updateInstanceConfigAndBrokerResourceIfNeeded();
    initSecondaryClusterRouting();
    _brokerMetrics.addCallbackGauge(Helix.INSTANCE_CONNECTED_METRIC_NAME,
        () -> _participantHelixManager.isConnected() ? 1L : 0L);
    _participantHelixManager.addPreConnectCallback(
        () -> _brokerMetrics.addMeteredGlobalValue(BrokerMeter.HELIX_ZOOKEEPER_RECONNECTS, 1L));

    // Initializing Groovy execution security
    GroovyFunctionEvaluator.configureGroovySecurity(
        _brokerConf.getProperty(CommonConstants.Groovy.GROOVY_QUERY_STATIC_ANALYZER_CONFIG,
            _brokerConf.getProperty(CommonConstants.Groovy.GROOVY_ALL_STATIC_ANALYZER_CONFIG)));

    // Register the service status handler
    registerServiceStatusHandler();

    _isStarting = false;
    _brokerMetrics.addTimedValue(BrokerTimer.STARTUP_SUCCESS_DURATION_MS,
        System.currentTimeMillis() - startTimeMs, TimeUnit.MILLISECONDS);

    _defaultClusterConfigChangeHandler.registerClusterConfigChangeListener(ContinuousJfrStarter.INSTANCE);

    LOGGER.info("Finish starting Pinot broker");
  }

  private void initSecondaryClusterRouting() {
    if (_secondaryRoutingManagers == null || _secondaryRoutingManagers.isEmpty()) {
      LOGGER.info("[federation] No secondary routing managers - skipping routing table initialization");
      return;
    }

    LOGGER.info("[federation] Initializing routing tables for {} secondary clusters", _secondaryRoutingManagers.size());
    int initialized = 0;

    for (Map.Entry<String, SecondaryBrokerRoutingManager> entry : _secondaryRoutingManagers.entrySet()) {
      try {
        entry.getValue().determineRoutingChangeForTables();
        initialized++;
      } catch (Exception e) {
        LOGGER.error("[federation] Failed to initialize routing tables for cluster '{}'", entry.getKey(), e);
      }
    }

    LOGGER.info("[federation] Initialized routing tables for {}/{} secondary clusters",
        initialized, _secondaryRoutingManagers.size());
  }

  private void initSecondaryClusterFederationProvider(TableCache tableCache, boolean caseInsensitive,
      RoutingManager primaryRoutingManager, RoutingManager federatedRoutingManager) {
    if (federatedRoutingManager == null) {
      LOGGER.info("[federation] Federation is not enabled - FederationProvider will be null");
      _crossClusterFederationProvider = null;
      return;
    }

    Map<String, TableCache> tableCacheMap = new HashMap<>();
    tableCacheMap.put(_clusterName, tableCache);

    if (_secondarySpectatorHelixManager == null || _secondarySpectatorHelixManager.isEmpty()) {
      LOGGER.info("[federation] No secondary spectator Helix managers - creating provider with primary cluster only");
      _crossClusterFederationProvider = null;
      return;
    }

    LOGGER.info("[federation] Initializing federation provider with {} secondary clusters",
        _secondarySpectatorHelixManager.size());

    for (Map.Entry<String, HelixManager> entry : _secondarySpectatorHelixManager.entrySet()) {
      String clusterName = entry.getKey();
      try {
        TableCache secondaryCache = new ZkTableCache(entry.getValue().getHelixPropertyStore(), caseInsensitive);
        tableCacheMap.put(clusterName, secondaryCache);
      } catch (Exception e) {
        LOGGER.error("[federation] Failed to create table cache for cluster '{}'", clusterName, e);
      }
    }

    _crossClusterFederationProvider = new CrossClusterFederationProvider(tableCacheMap, primaryRoutingManager,
        federatedRoutingManager);
    LOGGER.info("[federation] Created federation provider with {}/{} clusters (1 primary + {} secondary)",
        tableCacheMap.size(), _secondarySpectatorHelixManager.size() + 1, tableCacheMap.size() - 1);
  }

  private void initSecondaryClusterFederatedRoutingManager() {
    if (_secondarySpectatorHelixManager == null || _secondarySpectatorHelixManager.isEmpty()) {
      LOGGER.info("[federation] No secondary spectator Helix managers - skipping federated routing manager init");
      return;
    }

    LOGGER.info("[federation] Initializing federated routing manager for {} clusters",
        _secondarySpectatorHelixManager.size());
    _secondaryRoutingManagers = new HashMap<>();

    for (Map.Entry<String, HelixManager> entry : _secondarySpectatorHelixManager.entrySet()) {
      String clusterName = entry.getKey();
      try {
        SecondaryBrokerRoutingManager routingManager =
            new SecondaryBrokerRoutingManager(_brokerMetrics, _serverRoutingStatsManager, _brokerConf);
        routingManager.init(entry.getValue());
        _secondaryRoutingManagers.put(clusterName, routingManager);
      } catch (Exception e) {
        LOGGER.error("[federation] Failed to initialize routing manager for cluster '{}'", clusterName, e);
      }
    }

    if (_secondaryRoutingManagers.isEmpty()) {
      LOGGER.error("[federation] Failed to initialize any routing managers - federated routing unavailable");
    } else {
      _federatedRoutingManager = new FederatedRoutingManager(_routingManager,
          new ArrayList<>(_secondaryRoutingManagers.values()));
      LOGGER.info("[federation] Created federated routing manager with {}/{} secondary clusters",
          _secondaryRoutingManagers.size(), _secondarySpectatorHelixManager.size());
    }
  }

  private void initSecondaryClusterSpectatorHelixManagers() throws Exception {
    if (_secondaryZkServers == null || _secondaryZkServers.isEmpty()) {
      LOGGER.info("[federation] No secondary ZK servers configured - skipping spectator Helix manager init");
      return;
    }

    LOGGER.info("[federation] Initializing spectator Helix managers for {} secondary clusters",
        _secondaryZkServers.size());
    _secondarySpectatorHelixManager = new HashMap<>();

    for (Map.Entry<String, String> entry : _secondaryZkServers.entrySet()) {
      String clusterName = entry.getKey();
      String zkServers = entry.getValue();
      try {
        HelixManager helixManager = HelixManagerFactory.getZKHelixManager(
            clusterName, _instanceId, InstanceType.SPECTATOR, zkServers);
        helixManager.connect();
        _secondarySpectatorHelixManager.put(clusterName, helixManager);
        LOGGER.info("[federation] Connected to secondary cluster '{}' at ZK: {}", clusterName, zkServers);
      } catch (Exception e) {
        LOGGER.error("[federation] Failed to connect to cluster '{}' at ZK: {}", clusterName, zkServers, e);
      }
    }

    if (_secondarySpectatorHelixManager.isEmpty()) {
      LOGGER.error("[federation] Failed to connect to any secondary clusters - federation will not be functional");
    } else {
      LOGGER.info("[federation] Connected to {}/{} secondary clusters: {}", _secondarySpectatorHelixManager.size(),
          _secondaryZkServers.size(), _secondarySpectatorHelixManager.keySet());
    }
  }

  public void initSecondaryClusterChangeMediator() throws Exception {
    if (_secondarySpectatorHelixManager == null || _secondarySpectatorHelixManager.isEmpty()) {
      LOGGER.info("[federation] No secondary spectator Helix managers - skipping cluster change mediator init");
      return;
    }

    if (_secondaryRoutingManagers == null || _secondaryRoutingManagers.isEmpty()) {
      LOGGER.error("[federation] Secondary routing managers not initialized - cannot create cluster change mediators");
      return;
    }

    LOGGER.info("[federation] Initializing cluster change mediators for {} secondary clusters",
        _secondarySpectatorHelixManager.size());
    _secondaryClusterChangeMediator = new HashMap<>();

    for (String clusterName : _secondarySpectatorHelixManager.keySet()) {
      BrokerRoutingManager routingManager = _secondaryRoutingManagers.get(clusterName);
      if (routingManager == null) {
        LOGGER.error("[federation] Routing manager not found for cluster '{}' - skipping mediator setup", clusterName);
        continue;
      }

      try {
        Map<ChangeType, List<ClusterChangeHandler>> handlers = new HashMap<>();
        handlers.put(ChangeType.CLUSTER_CONFIG, new ArrayList<>());
        handlers.put(ChangeType.IDEAL_STATE, Collections.singletonList(routingManager));
        handlers.put(ChangeType.EXTERNAL_VIEW, Collections.singletonList(routingManager));
        handlers.put(ChangeType.INSTANCE_CONFIG, Collections.singletonList(routingManager));
        handlers.put(ChangeType.RESOURCE_CONFIG, Collections.singletonList(routingManager));

        ClusterChangeMediator mediator = new ClusterChangeMediator(_secondaryInstanceId, handlers, _brokerMetrics);
        mediator.start();
        _secondaryClusterChangeMediator.put(clusterName, mediator);

        HelixManager helixManager = _secondarySpectatorHelixManager.get(clusterName);
        helixManager.addIdealStateChangeListener(mediator);
        helixManager.addExternalViewChangeListener(mediator);
        helixManager.addInstanceConfigChangeListener(mediator);
        helixManager.addClusterfigChangeListener(mediator);
      } catch (Exception e) {
        LOGGER.error("[federation] Failed to initialize cluster change mediator for cluster '{}'", clusterName, e);
      }
    }

    LOGGER.info("[federation] Initialized {}/{} cluster change mediators", _secondaryClusterChangeMediator.size(),
        _secondarySpectatorHelixManager.size());
  }

  /**
   * @deprecated Use {@link #createBrokerAdminApp()} instead.
   * This method is called after initialization of BrokerAdminApiApplication object
   * and before calling start to allow custom broker starters to register additional
   * components.
   * @param brokerAdminApplication is the application
   */
  protected void registerExtraComponents(BrokerAdminApiApplication brokerAdminApplication) {
  }

  /**
   * Can be overridden to create a custom WorkloadBudgetManager.
   */
  protected WorkloadBudgetManager createWorkloadBudgetManager(PinotConfiguration brokerConf) {
    return new WorkloadBudgetManager(brokerConf);
  }

  private QueryDispatcher createQueryDispatcher(PinotConfiguration brokerConf) {
    String hostname = _brokerConf.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME);
    int port =
        Integer.parseInt(_brokerConf.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT));
    return new QueryDispatcher(
        new MailboxService(hostname, port, org.apache.pinot.spi.config.instance.InstanceType.BROKER, _brokerConf),
        _failureDetector);
  }

  private void updateInstanceConfigAndBrokerResourceIfNeeded() {
    InstanceConfig instanceConfig = HelixHelper.getInstanceConfig(_participantHelixManager, _instanceId);
    boolean updated = HelixHelper.updateHostnamePort(instanceConfig, _hostname, _port);

    ZNRecord znRecord = instanceConfig.getRecord();
    Map<String, String> simpleFields = znRecord.getSimpleFields();
    if (_tlsPort > 0) {
      HelixHelper.updateTlsPort(instanceConfig, _tlsPort);
    }
    // Update GRPC query engine port
    if (BrokerGrpcServer.isEnabled(_brokerConf)) {
      int grpcPort = BrokerGrpcServer.getGrpcPort(_brokerConf);
      updated |= updatePortIfNeeded(simpleFields, Helix.Instance.GRPC_PORT_KEY, grpcPort);
    } else {
      updated |= updatePortIfNeeded(simpleFields, Helix.Instance.GRPC_PORT_KEY, -1);
    }

    // Update multi-stage query engine ports
    if (_brokerConf.getProperty(Helix.CONFIG_OF_MULTI_STAGE_ENGINE_ENABLED, Helix.DEFAULT_MULTI_STAGE_ENGINE_ENABLED)) {
      updated |= updatePortIfNeeded(simpleFields, Helix.Instance.MULTI_STAGE_QUERY_ENGINE_MAILBOX_PORT_KEY,
          Integer.parseInt(_brokerConf.getProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT)));
    } else {
      updated |= updatePortIfNeeded(simpleFields, Helix.Instance.MULTI_STAGE_QUERY_ENGINE_MAILBOX_PORT_KEY, -1);
    }
    updated |= HelixHelper.removeDisabledPartitions(instanceConfig);
    boolean shouldUpdateBrokerResource = false;
    List<String> instanceTags = instanceConfig.getTags();
    if (instanceTags.isEmpty()) {
      // This is a new broker (first time joining the cluster). We allow configuring initial broker tags regardless of
      // tenant isolation mode since it defaults to true and is relatively obscure.
      String instanceTagsConfig = _brokerConf.getProperty(Broker.CONFIG_OF_BROKER_INSTANCE_TAGS);
      if (StringUtils.isNotEmpty(instanceTagsConfig)) {
        for (String instanceTag : StringUtils.split(instanceTagsConfig, ',')) {
          Preconditions.checkArgument(TagNameUtils.isBrokerTag(instanceTag), "Illegal broker instance tag: %s",
              instanceTag);
          instanceConfig.addTag(instanceTag);
        }
        shouldUpdateBrokerResource = true;
      } else if (ZKMetadataProvider.getClusterTenantIsolationEnabled(_propertyStore)) {
        instanceConfig.addTag(TagNameUtils.getBrokerTagForTenant(null));
        shouldUpdateBrokerResource = true;
      } else {
        instanceConfig.addTag(Helix.UNTAGGED_BROKER_INSTANCE);
      }
      instanceTags = instanceConfig.getTags();
      updated = true;
    }
    updated |= HelixHelper.updatePinotVersion(instanceConfig);
    if (updated) {
      HelixHelper.updateInstanceConfig(_participantHelixManager, instanceConfig);
    }

    if (shouldUpdateBrokerResource) {
      // Update broker resource to include the new broker
      long startTimeMs = System.currentTimeMillis();
      List<String> tablesAdded = new ArrayList<>();
      HelixHelper.updateBrokerResource(_participantHelixManager, _instanceId, instanceTags, tablesAdded, null);
      LOGGER.info("Updated broker resource for new joining broker: {} with instance tags: {} in {}ms, tables added: {}",
          _instanceId, instanceTags, System.currentTimeMillis() - startTimeMs, tablesAdded);
    }
  }

  /**
   * Fetches the resources to monitor and registers the
   * {@link org.apache.pinot.common.utils.ServiceStatus.ServiceStatusCallback}s
   */
  private void registerServiceStatusHandler() {
    List<String> resourcesToMonitor = new ArrayList<>(1);
    IdealState brokerResourceIdealState =
        _helixAdmin.getResourceIdealState(_clusterName, Helix.BROKER_RESOURCE_INSTANCE);
    if (brokerResourceIdealState != null && brokerResourceIdealState.isEnabled()) {
      for (String partitionName : brokerResourceIdealState.getPartitionSet()) {
        if (brokerResourceIdealState.getInstanceSet(partitionName).contains(_instanceId)) {
          resourcesToMonitor.add(Helix.BROKER_RESOURCE_INSTANCE);
          break;
        }
      }
    }

    double minResourcePercentForStartup =
        _brokerConf.getProperty(Broker.CONFIG_OF_BROKER_MIN_RESOURCE_PERCENT_FOR_START,
            Broker.DEFAULT_BROKER_MIN_RESOURCE_PERCENT_FOR_START);

    LOGGER.info("Registering service status handler");
    ServiceStatus.setServiceStatusCallback(_instanceId, new ServiceStatus.MultipleCallbackServiceStatusCallback(
        List.of(
            new ServiceStatus.LifecycleServiceStatusCallback(this::isStarting, this::isShuttingDown),
            new ServiceStatus.IdealStateAndCurrentStateMatchServiceStatusCallback(_participantHelixManager,
                _clusterName, _instanceId, resourcesToMonitor, minResourcePercentForStartup),
            new ServiceStatus.IdealStateAndExternalViewMatchServiceStatusCallback(_participantHelixManager,
                _clusterName, _instanceId, resourcesToMonitor, minResourcePercentForStartup))));
  }

  private String getDefaultBrokerId() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      LOGGER.error("Caught exception while getting default broker Id", e);
      return "";
    }
  }

  private boolean updatePortIfNeeded(Map<String, String> instanceConfigSimpleFields, String key, int port) {
    String existingPortStr = instanceConfigSimpleFields.get(key);
    if (port > 0) {
      String portStr = Integer.toString(port);
      if (!portStr.equals(existingPortStr)) {
        LOGGER.info("Updating '{}' for instance: {} to: {}", key, _instanceId, port);
        instanceConfigSimpleFields.put(key, portStr);
        return true;
      }
    } else {
      if (existingPortStr != null) {
        LOGGER.info("Removing '{}' from instance: {}", key, _instanceId);
        instanceConfigSimpleFields.remove(key);
        return true;
      }
    }
    return false;
  }

  @Override
  public void stop() {
    LOGGER.info("Shutting down Pinot broker");
    _isShuttingDown = true;

    LOGGER.info("Disconnecting participant Helix manager");
    _participantHelixManager.disconnect();

    LOGGER.info("Stopping cluster change mediator");
    _clusterChangeMediator.stop();
    if (_secondaryClusterChangeMediator != null) {
      _secondaryClusterChangeMediator.values().forEach(ClusterChangeMediator::stop);
    }
    if (_secondaryRoutingManagers != null) {
      _secondaryRoutingManagers.values().forEach(SecondaryBrokerRoutingManager::shutdown);
    }

    _failureDetector.stop();

    // Delay shutdown of request handler so that the pending queries can be finished. The participant Helix manager has
    // been disconnected, so instance should disappear from ExternalView soon and stop getting new queries.
    long delayShutdownTimeMs =
        _brokerConf.getProperty(Broker.CONFIG_OF_DELAY_SHUTDOWN_TIME_MS, Broker.DEFAULT_DELAY_SHUTDOWN_TIME_MS);
    LOGGER.info("Wait for {}ms before shutting down request handler to finish the pending queries",
        delayShutdownTimeMs);
    try {
      Thread.sleep(delayShutdownTimeMs);
    } catch (Exception e) {
      LOGGER.error("Caught exception while waiting for shutdown delay of {}ms", delayShutdownTimeMs, e);
    }

    if (_brokerGrpcServer != null) {
      LOGGER.info("Stopping broker grpc server");
      _brokerGrpcServer.shutdown();
    }

    LOGGER.info("Shutting down request handler and broker admin application");
    _brokerRequestHandler.shutDown();
    _threadAccountant.stopWatcherTask();
    _brokerAdminApplication.stop();

    LOGGER.info("Stopping the broker routing manager");
    _routingManager.stop();

    LOGGER.info("Close PinotFs");
    try {
      PinotFSFactory.shutdown();
    } catch (IOException e) {
      LOGGER.error("Caught exception when shutting down PinotFsFactory", e);
    }

    LOGGER.info("Disconnecting spectator Helix manager");
    _spectatorHelixManager.disconnect();
    if (_secondarySpectatorHelixManager != null) {
      _secondarySpectatorHelixManager.values().forEach(HelixManager::disconnect);
    }

    LOGGER.info("Deregistering service status handler");
    ServiceStatus.removeServiceStatusCallback(_instanceId);
    LOGGER.info("Shutdown Broker Metrics Registry");
    _metricsRegistry.shutdown();
    LOGGER.info("Finish shutting down Pinot broker for {}", _instanceId);
  }

  public boolean isStarting() {
    return _isStarting;
  }

  public boolean isShuttingDown() {
    return _isShuttingDown;
  }

  public HelixManager getSpectatorHelixManager() {
    return _spectatorHelixManager;
  }

  public PinotMetricsRegistry getMetricsRegistry() {
    return _metricsRegistry;
  }

  public BrokerMetrics getBrokerMetrics() {
    return _brokerMetrics;
  }

  public BrokerRoutingManager getRoutingManager() {
    return _routingManager;
  }

  public AccessControlFactory getAccessControlFactory() {
    return _accessControlFactory;
  }

  public BrokerRequestHandler getBrokerRequestHandler() {
    return _brokerRequestHandler;
  }

  protected BrokerAdminApiApplication createBrokerAdminApp() {
    BrokerAdminApiApplication brokerAdminApiApplication =
        new BrokerAdminApiApplication(_routingManager, _brokerRequestHandler, _brokerMetrics, _brokerConf,
            _sqlQueryExecutor, _serverRoutingStatsManager, _accessControlFactory, _spectatorHelixManager,
            _queryQuotaManager, _threadAccountant, _responseStore);
    brokerAdminApiApplication.register(
        new AuditServiceBinder(_defaultClusterConfigChangeHandler, getServiceRole(), _brokerMetrics));
    registerExtraComponents(brokerAdminApiApplication);
    return brokerAdminApiApplication;
  }
}
