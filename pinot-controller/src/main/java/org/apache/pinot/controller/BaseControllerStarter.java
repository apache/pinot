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
package org.apache.pinot.controller;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.util.Timeout;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.api.listeners.ControllerChangeListener;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.Message;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.zookeeper.constant.ZkSystemPropertyKeys;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.http.PoolingHttpClientConnectionManagerHelper;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ControllerTimer;
import org.apache.pinot.common.metrics.ValidationMetrics;
import org.apache.pinot.common.minion.InMemoryTaskManagerStatusCache;
import org.apache.pinot.common.minion.TaskGeneratorMostRecentRunInfo;
import org.apache.pinot.common.minion.TaskManagerStatusCache;
import org.apache.pinot.common.utils.PinotAppConfigs;
import org.apache.pinot.common.utils.ServiceStartableUtils;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.helix.IdealStateGroupCommit;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.common.utils.log.DummyLogFileServer;
import org.apache.pinot.common.utils.log.LocalLogFileServer;
import org.apache.pinot.common.utils.log.LogFileServer;
import org.apache.pinot.common.utils.tls.PinotInsecureMode;
import org.apache.pinot.common.utils.tls.TlsUtils;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.controller.api.ControllerAdminApiApplication;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.events.MetadataEventNotifierFactory;
import org.apache.pinot.controller.api.resources.ControllerFilePathProvider;
import org.apache.pinot.controller.api.resources.InvalidControllerConfigException;
import org.apache.pinot.controller.cursors.ResponseStoreCleaner;
import org.apache.pinot.controller.helix.RealtimeConsumerMonitor;
import org.apache.pinot.controller.helix.SegmentStatusChecker;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.cleanup.StaleInstancesCleanupTask;
import org.apache.pinot.controller.helix.core.controllerjob.ControllerJobTypes;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.helix.core.minion.TaskMetricsEmitter;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.controller.helix.core.realtime.SegmentCompletionConfig;
import org.apache.pinot.controller.helix.core.realtime.SegmentCompletionManager;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceChecker;
import org.apache.pinot.controller.helix.core.rebalance.RebalancePreChecker;
import org.apache.pinot.controller.helix.core.rebalance.RebalancePreCheckerFactory;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalanceManager;
import org.apache.pinot.controller.helix.core.rebalance.tenant.DefaultTenantRebalancer;
import org.apache.pinot.controller.helix.core.rebalance.tenant.TenantRebalancer;
import org.apache.pinot.controller.helix.core.relocation.SegmentRelocator;
import org.apache.pinot.controller.helix.core.retention.RetentionManager;
import org.apache.pinot.controller.helix.core.statemodel.LeadControllerResourceMasterSlaveStateModelFactory;
import org.apache.pinot.controller.helix.core.util.HelixSetupUtils;
import org.apache.pinot.controller.helix.starter.HelixConfig;
import org.apache.pinot.controller.tuner.TableConfigTunerRegistry;
import org.apache.pinot.controller.util.BrokerServiceHelper;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.controller.validation.BrokerResourceValidationManager;
import org.apache.pinot.controller.validation.DiskUtilizationChecker;
import org.apache.pinot.controller.validation.OfflineSegmentIntervalChecker;
import org.apache.pinot.controller.validation.RealtimeSegmentValidationManager;
import org.apache.pinot.controller.validation.ResourceUtilizationChecker;
import org.apache.pinot.controller.validation.ResourceUtilizationManager;
import org.apache.pinot.controller.validation.StorageQuotaChecker;
import org.apache.pinot.core.periodictask.PeriodicTask;
import org.apache.pinot.core.periodictask.PeriodicTaskScheduler;
import org.apache.pinot.core.query.executor.sql.SqlQueryExecutor;
import org.apache.pinot.core.segment.processing.lifecycle.PinotSegmentLifecycleEventListenerManager;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.core.util.ListenerConfigUtil;
import org.apache.pinot.core.util.trace.ContinuousJfrStarter;
import org.apache.pinot.segment.local.function.GroovyFunctionEvaluator;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.crypt.PinotCrypterFactory;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.spi.services.ServiceStartable;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.sql.parsers.rewriter.QueryRewriterFactory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base class for controller startables
 */
public abstract class BaseControllerStarter implements ServiceStartable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseControllerStarter.class);

  public static final String CONTROLLER_INSTANCE_ID = "controllerInstanceId";
  private static final String METRICS_REGISTRY_NAME = "pinot.controller.metrics";
  private static final Long DATA_DIRECTORY_MISSING_VALUE = 1000000L;
  private static final Long DATA_DIRECTORY_EXCEPTION_VALUE = 1100000L;
  private static final String METADATA_EVENT_NOTIFIER_PREFIX = "metadata.event.notifier";
  private static final String MAX_STATE_TRANSITIONS_PER_INSTANCE = "MaxStateTransitionsPerInstance";

  protected ControllerConf _config;
  protected List<ListenerConfig> _listenerConfigs;
  protected ControllerAdminApiApplication _adminApp;
  // TODO: rename this variable once it's full separated with Helix controller.
  protected PinotHelixResourceManager _helixResourceManager;
  protected ExecutorService _executorService;
  protected String _helixZkURL;
  protected String _helixClusterName;
  protected String _hostname;
  protected int _port;
  protected int _tlsPort;
  protected String _helixControllerInstanceId;
  protected String _helixParticipantInstanceId;
  protected boolean _isUpdateStateModel;
  protected boolean _enableBatchMessageMode;
  protected ControllerConf.ControllerMode _controllerMode;
  protected HelixManager _helixControllerManager;
  protected HelixManager _helixParticipantManager;
  protected PinotMetricsRegistry _metricsRegistry;
  protected ControllerMetrics _controllerMetrics;
  protected ValidationMetrics _validationMetrics;
  protected SqlQueryExecutor _sqlQueryExecutor;
  // Can only be constructed after resource manager getting started
  protected OfflineSegmentIntervalChecker _offlineSegmentIntervalChecker;
  protected RealtimeSegmentValidationManager _realtimeSegmentValidationManager;
  protected BrokerResourceValidationManager _brokerResourceValidationManager;
  protected SegmentRelocator _segmentRelocator;
  protected RetentionManager _retentionManager;
  protected SegmentStatusChecker _segmentStatusChecker;
  protected RebalanceChecker _rebalanceChecker;
  protected RealtimeConsumerMonitor _realtimeConsumerMonitor;
  protected PinotTaskManager _taskManager;
  protected TaskManagerStatusCache<TaskGeneratorMostRecentRunInfo> _taskManagerStatusCache;
  protected PeriodicTaskScheduler _periodicTaskScheduler;
  protected PinotHelixTaskResourceManager _helixTaskResourceManager;
  protected PinotLLCRealtimeSegmentManager _pinotLLCRealtimeSegmentManager;
  protected SegmentCompletionManager _segmentCompletionManager;
  protected LeadControllerManager _leadControllerManager;
  protected List<ServiceStatus.ServiceStatusCallback> _serviceStatusCallbackList;
  protected StaleInstancesCleanupTask _staleInstancesCleanupTask;
  protected TaskMetricsEmitter _taskMetricsEmitter;
  protected PoolingHttpClientConnectionManager _connectionManager;
  protected TenantRebalancer _tenantRebalancer;
  // This executor should be used by all code paths for user initiated rebalances, so that the controller config
  // CONTROLLER_EXECUTOR_REBALANCE_NUM_THREADS is honored.
  protected ExecutorService _rebalancerExecutorService;
  protected TableSizeReader _tableSizeReader;
  protected StorageQuotaChecker _storageQuotaChecker;
  protected DiskUtilizationChecker _diskUtilizationChecker;
  protected ResourceUtilizationManager _resourceUtilizationManager;
  protected RebalancePreChecker _rebalancePreChecker;
  protected TableRebalanceManager _tableRebalanceManager;

  @Override
  public void init(PinotConfiguration pinotConfiguration)
      throws Exception {
    _config = new ControllerConf(pinotConfiguration.toMap());
    _helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(_config.getZkStr());
    _helixClusterName = _config.getHelixClusterName();
    _controllerMode = _config.getControllerMode();
    if (_controllerMode == ControllerConf.ControllerMode.DUAL
        || _controllerMode == ControllerConf.ControllerMode.HELIX_ONLY) {
      HelixSetupUtils.setupHelixClusterWithDefaultConfigs(_helixZkURL, _helixClusterName, getDefaultClusterConfigs());
    }
    ServiceStartableUtils.applyClusterConfig(_config, _helixZkURL, _helixClusterName, ServiceRole.CONTROLLER);
    applyCustomConfigs(_config);

    PinotInsecureMode.setPinotInInsecureMode(_config.getProperty(CommonConstants.CONFIG_OF_PINOT_INSECURE_MODE, false));

    setupHelixSystemProperties();
    IdealStateGroupCommit.setMinNumCharsInISToTurnOnCompression(_config.getMinNumCharsInISToTurnOnCompression());
    _listenerConfigs = ListenerConfigUtil.buildControllerConfigs(_config);
    inferHostnameIfNeeded(_config);
    _hostname = _config.getControllerHost();
    _port = _listenerConfigs.get(0).getPort();
    _tlsPort = ListenerConfigUtil.findLastTlsPort(_listenerConfigs, 0);
    // NOTE: Use <hostname>_<port> as Helix controller instance id because ControllerLeaderLocator relies on this format
    //       to parse the leader controller's hostname and port
    // TODO: Use the same instance id for controller and participant when leadControllerResource is always enabled after
    //       releasing 0.8.0
    _helixControllerInstanceId = _hostname + "_" + _port;
    _helixParticipantInstanceId = _config.getInstanceId();
    if (_helixParticipantInstanceId != null) {
      // NOTE: Force all instances to have the same prefix in order to derive the instance type based on the instance id
      Preconditions.checkState(InstanceTypeUtils.isController(_helixParticipantInstanceId),
          "Instance id must have prefix '%s', got '%s'", Helix.PREFIX_OF_CONTROLLER_INSTANCE,
          _helixParticipantInstanceId);
    } else {
      _helixParticipantInstanceId = LeadControllerUtils.generateParticipantInstanceId(_hostname, _port);
    }
    _isUpdateStateModel = _config.isUpdateSegmentStateModel();
    _enableBatchMessageMode = _config.getEnableBatchMessageMode();

    _serviceStatusCallbackList = new ArrayList<>();
    if (_controllerMode == ControllerConf.ControllerMode.HELIX_ONLY) {
      _adminApp = null;
      _helixResourceManager = null;
      _executorService = null;
    } else {
      // Initialize FunctionRegistry before starting the admin application (PinotQueryResource requires it to compile
      // queries)
      FunctionRegistry.init();
      _adminApp = createControllerAdminApp();
      // This executor service is used to do async tasks from multiget util or table rebalancing.
      _executorService = createExecutorService(_config.getControllerExecutorNumThreads(), "async-task-thread-%d");
      // Do not use this before the invocation of {@link PinotHelixResourceManager::start()}, which happens in {@link
      // ControllerStarter::start()}
      _helixResourceManager = createHelixResourceManager();
    }

    // Initialize the table config tuner registry.
    TableConfigTunerRegistry.init(_config.getTableConfigTunerPackages());

    TableConfigUtils.setDisableGroovy(_config.isDisableIngestionGroovy());
    TableConfigUtils.setEnforcePoolBasedAssignment(_config.isEnforcePoolBasedAssignmentEnabled());

    ContinuousJfrStarter.init(_config);
    ControllerJobTypes.init(_config);
  }

  /// Returns the default cluster configs to be stored in ZK as Helix cluster config. These configs will then be
  /// propagated to all the instance configs to control the default behavior for each component.
  /// Can be overridden to add more configs.
  protected Map<String, String> getDefaultClusterConfigs() {
    Map<String, String> configs = new HashMap<>();
    configs.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, "true");
    configs.put(Helix.ENABLE_CASE_INSENSITIVE_KEY, Boolean.toString(Helix.DEFAULT_ENABLE_CASE_INSENSITIVE));
    configs.put(Helix.DEFAULT_HYPERLOGLOG_LOG2M_KEY, Integer.toString(Helix.DEFAULT_HYPERLOGLOG_LOG2M));
    configs.put(CommonConstants.Broker.CONFIG_OF_ENABLE_QUERY_LIMIT_OVERRIDE, "true");
    return configs;
  }

  /// Can be overridden to apply custom configs to the controller conf.
  protected void applyCustomConfigs(ControllerConf controllerConf) {
  }

  // If thread pool size is not configured executor will use cached thread pool
  private ExecutorService createExecutorService(int numThreadPool, String threadNameFormat) {
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build();
    return (numThreadPool <= 0) ? Executors.newCachedThreadPool(threadFactory)
        : Executors.newFixedThreadPool(numThreadPool, threadFactory);
  }

  private void inferHostnameIfNeeded(ControllerConf config) {
    if (config.getControllerHost() == null) {
      if (config.getProperty(Helix.SET_INSTANCE_ID_TO_HOSTNAME_KEY, false)) {
        final String inferredHostname = NetUtils.getHostnameOrAddress();
        if (inferredHostname != null) {
          config.setControllerHost(inferredHostname);
        } else {
          throw new RuntimeException(
              "Failed to infer controller hostname, please set controller instanceId explicitly in config file.");
        }
      }
    }
  }

  private void setupHelixSystemProperties() {
    // NOTE: Helix will disconnect the manager and disable the instance if it detects flapping (too frequent disconnect
    // from ZooKeeper). Setting flapping time window to a small value can avoid this from happening. Helix ignores the
    // non-positive value, so set the default value as 1.
    System.setProperty(SystemPropertyKeys.FLAPPING_TIME_WINDOW,
        _config.getProperty(Helix.CONFIG_OF_CONTROLLER_FLAPPING_TIME_WINDOW_MS, Helix.DEFAULT_FLAPPING_TIME_WINDOW_MS));
  }

  private void setupHelixClusterConstraints() {
    String maxStateTransitions = _config.getProperty(Helix.CONFIG_OF_HELIX_INSTANCE_MAX_STATE_TRANSITIONS,
        Helix.DEFAULT_HELIX_INSTANCE_MAX_STATE_TRANSITIONS);
    Map<ClusterConstraints.ConstraintAttribute, String> constraintAttributes = new HashMap<>();
    constraintAttributes.put(ClusterConstraints.ConstraintAttribute.INSTANCE, ".*");
    constraintAttributes.put(ClusterConstraints.ConstraintAttribute.MESSAGE_TYPE,
        Message.MessageType.STATE_TRANSITION.name());
    ConstraintItem constraintItem = new ConstraintItem(constraintAttributes, maxStateTransitions);

    _helixControllerManager.getClusterManagmentTool()
        .setConstraint(_helixClusterName, ClusterConstraints.ConstraintType.MESSAGE_CONSTRAINT,
            MAX_STATE_TRANSITIONS_PER_INSTANCE, constraintItem);
  }

  /**
   * Creates an instance of PinotHelixResourceManager.
   * <p>
   * This method can be overridden by subclasses to instantiate the object
   * with subclasses of PinotHelixResourceManager.
   * By default, it returns a new PinotHelixResourceManager using the current configuration.
   *
   * @return A new instance of PinotHelixResourceManager.
   */
  protected PinotHelixResourceManager createHelixResourceManager() {
    return new PinotHelixResourceManager(_config);
  }

  public PinotHelixResourceManager getHelixResourceManager() {
    return _helixResourceManager;
  }

  /**
   * Gets the Helix Manager connected as Helix controller.
   */
  public HelixManager getHelixControllerManager() {
    return _helixControllerManager;
  }

  public LeadControllerManager getLeadControllerManager() {
    return _leadControllerManager;
  }

  public OfflineSegmentIntervalChecker getOfflineSegmentIntervalChecker() {
    return _offlineSegmentIntervalChecker;
  }

  public RealtimeSegmentValidationManager getRealtimeSegmentValidationManager() {
    return _realtimeSegmentValidationManager;
  }

  public BrokerResourceValidationManager getBrokerResourceValidationManager() {
    return _brokerResourceValidationManager;
  }

  public PinotHelixTaskResourceManager getHelixTaskResourceManager() {
    return _helixTaskResourceManager;
  }

  public PinotTaskManager getTaskManager() {
    return _taskManager;
  }

  public StaleInstancesCleanupTask getStaleInstancesCleanupTask() {
    return _staleInstancesCleanupTask;
  }

  public TableRebalanceManager getTableRebalanceManager() {
    return _tableRebalanceManager;
  }

  public TableSizeReader getTableSizeReader() {
    return _tableSizeReader;
  }

  @Override
  public ServiceRole getServiceRole() {
    return ServiceRole.CONTROLLER;
  }

  @Override
  public String getInstanceId() {
    return _helixParticipantInstanceId;
  }

  @Override
  public ControllerConf getConfig() {
    return _config;
  }

  @Override
  public void start()
      throws Exception {
    LOGGER.info("Starting Pinot controller in mode: {}. (Version: {})", _controllerMode.name(), PinotVersion.VERSION);
    LOGGER.info("Controller configs: {}", new PinotAppConfigs(getConfig()).toJSONString());
    long startTimeMs = System.currentTimeMillis();
    Utils.logVersions();

    // Set up controller metrics
    initControllerMetrics();

    switch (_controllerMode) {
      case DUAL:
        setUpHelixController();
        setUpPinotController();
        break;
      case PINOT_ONLY:
        setUpPinotController();
        break;
      case HELIX_ONLY:
        setUpHelixController();
        break;
      default:
        LOGGER.error("Invalid mode: {}", _controllerMode);
        break;
    }

    // Initializing Groovy execution security
    GroovyFunctionEvaluator.configureGroovySecurity(
        _config.getProperty(CommonConstants.Groovy.GROOVY_INGESTION_STATIC_ANALYZER_CONFIG,
            _config.getProperty(CommonConstants.Groovy.GROOVY_ALL_STATIC_ANALYZER_CONFIG)));

    ServiceStatus.setServiceStatusCallback(_helixParticipantInstanceId,
        new ServiceStatus.MultipleCallbackServiceStatusCallback(_serviceStatusCallbackList));
    _controllerMetrics.addTimedValue(ControllerTimer.STARTUP_SUCCESS_DURATION_MS,
        System.currentTimeMillis() - startTimeMs, TimeUnit.MILLISECONDS);
  }

  private void setUpHelixController() {
    // Register and connect instance as Helix controller.
    LOGGER.info("Starting Helix controller");
    _helixControllerManager =
        HelixSetupUtils.setupHelixController(_helixClusterName, _helixZkURL, _helixControllerInstanceId);

    // Emit helix controller metrics
    _controllerMetrics.addCallbackGauge(Helix.INSTANCE_CONNECTED_METRIC_NAME,
        () -> _helixControllerManager.isConnected() ? 1L : 0L);
    // Deprecated, since getting the leadership of Helix does not mean Helix has been ready for pinot.
    _controllerMetrics.addCallbackGauge("helix.leader", () -> _helixControllerManager.isLeader() ? 1L : 0L);
    _helixControllerManager.addPreConnectCallback(
        () -> _controllerMetrics.addMeteredGlobalValue(ControllerMeter.HELIX_ZOOKEEPER_RECONNECTS, 1L));

    _serviceStatusCallbackList.add(generateServiceStatusCallback(_helixControllerManager));

    // setup up constraint
    setupHelixClusterConstraints();
  }

  private void setUpPinotController() {
    // install default SSL context if necessary (even if not force-enabled everywhere)
    TlsConfig tlsDefaults = TlsUtils.extractTlsConfig(_config, ControllerConf.CONTROLLER_TLS_PREFIX);
    if (StringUtils.isNotBlank(tlsDefaults.getKeyStorePath()) || StringUtils.isNotBlank(
        tlsDefaults.getTrustStorePath())) {
      LOGGER.info("Installing default SSL context for any client requests");
      TlsUtils.installDefaultSSLSocketFactory(tlsDefaults);
    }

    // Set up Pinot cluster in Helix if needed
    HelixSetupUtils.setupPinotCluster(_helixClusterName, _helixZkURL, _isUpdateStateModel, _enableBatchMessageMode,
        _config);

    // Start all components
    initPinotFSFactory();
    initControllerFilePathProvider();
    initSegmentFetcherFactory();
    initPinotCrypterFactory();

    LOGGER.info("Initializing QueryRewriterFactory");
    QueryRewriterFactory.init(
        _config.getProperty(CommonConstants.Controller.CONFIG_OF_CONTROLLER_QUERY_REWRITER_CLASS_NAMES));

    LOGGER.info("Initializing Helix participant manager");
    _helixParticipantManager =
        HelixManagerFactory.getZKHelixManager(_helixClusterName, _helixParticipantInstanceId, InstanceType.PARTICIPANT,
            _helixZkURL);

    // LeadControllerManager needs to be initialized before registering as Helix participant.
    LOGGER.info("Initializing lead controller manager");
    _leadControllerManager =
        new LeadControllerManager(_helixControllerInstanceId, _helixParticipantManager, _controllerMetrics);

    LOGGER.info("Registering and connecting Helix participant manager as Helix Participant role");
    registerAndConnectAsHelixParticipant();

    // LeadControllerManager needs to be started after the connection
    // as it can check Helix leadership and resource config only after connecting to Helix cluster.
    LOGGER.info("Starting lead controller manager");
    _leadControllerManager.start();

    LOGGER.info("Starting Pinot Helix resource manager and connecting to Zookeeper");
    _helixResourceManager.start(_helixParticipantManager, _controllerMetrics);

    // Initialize segment lifecycle event listeners
    PinotSegmentLifecycleEventListenerManager.getInstance().init(_helixParticipantManager);

    LOGGER.info("Starting task resource manager");
    _helixTaskResourceManager =
        new PinotHelixTaskResourceManager(_helixResourceManager, new TaskDriver(_helixParticipantManager),
            _config.getPinotTaskExpireTimeInMs());

    // Helix resource manager must be started in order to create PinotLLCRealtimeSegmentManager
    LOGGER.info("Starting realtime segment manager");
    _pinotLLCRealtimeSegmentManager = createPinotLLCRealtimeSegmentManager();
    // TODO: Need to put this inside HelixResourceManager when HelixControllerLeadershipManager is removed.
    _helixResourceManager.registerPinotLLCRealtimeSegmentManager(_pinotLLCRealtimeSegmentManager);

    SegmentCompletionConfig segmentCompletionConfig = new SegmentCompletionConfig(_config);

    _segmentCompletionManager =
        new SegmentCompletionManager(_helixParticipantManager, _pinotLLCRealtimeSegmentManager, _controllerMetrics,
            _leadControllerManager, _config.getSegmentCommitTimeoutSeconds(), segmentCompletionConfig);

    _sqlQueryExecutor = new SqlQueryExecutor(_config.generateVipUrl());

    _connectionManager = PoolingHttpClientConnectionManagerHelper.createWithSocketFactory();
    _connectionManager.setDefaultSocketConfig(
        SocketConfig.custom()
            .setSoTimeout(Timeout.of(_config.getServerAdminRequestTimeoutSeconds() * 1000, TimeUnit.MILLISECONDS))
            .build());
    _tableSizeReader =
        new TableSizeReader(_executorService, _connectionManager, _controllerMetrics, _helixResourceManager,
            _leadControllerManager);
    _storageQuotaChecker = new StorageQuotaChecker(_tableSizeReader, _controllerMetrics, _leadControllerManager,
        _helixResourceManager, _config);

    _diskUtilizationChecker = new DiskUtilizationChecker(_helixResourceManager, _config);
    _resourceUtilizationManager = new ResourceUtilizationManager(_config, _diskUtilizationChecker);
    _rebalancePreChecker = RebalancePreCheckerFactory.create(_config.getRebalancePreCheckerClass());
    _rebalancePreChecker.init(_helixResourceManager, _executorService, _config.getDiskUtilizationThreshold());
    _rebalancerExecutorService = createExecutorService(_config.getControllerExecutorRebalanceNumThreads(),
        "rebalance-thread-%d");
    _tableRebalanceManager =
        new TableRebalanceManager(_helixResourceManager, _controllerMetrics, _rebalancePreChecker, _tableSizeReader,
            _rebalancerExecutorService);
    _tenantRebalancer =
        new DefaultTenantRebalancer(_tableRebalanceManager, _helixResourceManager, _rebalancerExecutorService);

    // Setting up periodic tasks
    List<PeriodicTask> controllerPeriodicTasks = setupControllerPeriodicTasks();
    LOGGER.info("Init controller periodic tasks scheduler");
    _periodicTaskScheduler = new PeriodicTaskScheduler();
    _periodicTaskScheduler.init(controllerPeriodicTasks);
    _periodicTaskScheduler.start();

    // Register message handler for incoming user-defined helix messages.
    _helixParticipantManager.getMessagingService()
        .registerMessageHandlerFactory(Message.MessageType.USER_DEFINE_MSG.toString(),
            new ControllerUserDefinedMessageHandlerFactory(_periodicTaskScheduler));

    String accessControlFactoryClass = _config.getAccessControlFactoryClass();
    LOGGER.info("Use class: {} as the AccessControlFactory", accessControlFactoryClass);
    final AccessControlFactory accessControlFactory;
    try {
      accessControlFactory = (AccessControlFactory) Class.forName(accessControlFactoryClass).newInstance();
      accessControlFactory.init(_config, _helixResourceManager);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while creating new AccessControlFactory instance", e);
    }

    final MetadataEventNotifierFactory metadataEventNotifierFactory =
        MetadataEventNotifierFactory.loadFactory(_config.subset(METADATA_EVENT_NOTIFIER_PREFIX), _helixResourceManager);

    LOGGER.info("Controller download url base: {}", _config.generateVipUrl());
    LOGGER.info("Injecting configuration and resource managers to the API context");
    // register all the controller objects for injection to jersey resources
    Instant controllerStartTime = Instant.now();
    _adminApp.registerBinder(new AbstractBinder() {
      @Override
      protected void configure() {
        bind(_config).to(ControllerConf.class);
        bind(_helixParticipantInstanceId).named(CONTROLLER_INSTANCE_ID);
        bind(_helixResourceManager).to(PinotHelixResourceManager.class);
        bind(_helixTaskResourceManager).to(PinotHelixTaskResourceManager.class);
        bind(_tableRebalanceManager).to(TableRebalanceManager.class);
        bind(_segmentCompletionManager).to(SegmentCompletionManager.class);
        bind(_taskManager).to(PinotTaskManager.class);
        bind(_taskManagerStatusCache).to(TaskManagerStatusCache.class);
        bind(_connectionManager).to(HttpClientConnectionManager.class);
        bind(_executorService).to(Executor.class);
        bind(_controllerMetrics).to(ControllerMetrics.class);
        bind(accessControlFactory).to(AccessControlFactory.class);
        bind(metadataEventNotifierFactory).to(MetadataEventNotifierFactory.class);
        bind(_leadControllerManager).to(LeadControllerManager.class);
        bind(_periodicTaskScheduler).to(PeriodicTaskScheduler.class);
        bind(_sqlQueryExecutor).to(SqlQueryExecutor.class);
        bind(_pinotLLCRealtimeSegmentManager).to(PinotLLCRealtimeSegmentManager.class);
        bind(_tenantRebalancer).to(TenantRebalancer.class);
        bind(_tableSizeReader).to(TableSizeReader.class);
        bind(_storageQuotaChecker).to(StorageQuotaChecker.class);
        bind(_diskUtilizationChecker).to(DiskUtilizationChecker.class);
        bind(_resourceUtilizationManager).to(ResourceUtilizationManager.class);
        bind(controllerStartTime).named(ControllerAdminApiApplication.START_TIME);
        String loggerRootDir = _config.getProperty(CommonConstants.Controller.CONFIG_OF_LOGGER_ROOT_DIR);
        if (loggerRootDir != null) {
          bind(new LocalLogFileServer(loggerRootDir)).to(LogFileServer.class);
        } else {
          bind(new DummyLogFileServer()).to(LogFileServer.class);
        }
      }
    });

    LOGGER.info("Starting controller admin application on: {}", ListenerConfigUtil.toString(_listenerConfigs));
    _adminApp.start(_listenerConfigs, _controllerMetrics);

    enforceTableConfigAndSchema();

    _controllerMetrics.addCallbackGauge("dataDir.exists", () -> new File(_config.getDataDir()).exists() ? 1L : 0L);
    _controllerMetrics.addCallbackGauge("dataDir.fileOpLatencyMs", () -> {
      File dataDir = new File(_config.getDataDir());
      if (dataDir.exists()) {
        try {
          long startTime = System.currentTimeMillis();
          File testFile = new File(dataDir, _config.getControllerHost());
          try (OutputStream outputStream = new FileOutputStream(testFile, false)) {
            outputStream.write(Longs.toByteArray(System.currentTimeMillis()));
          }
          FileUtils.deleteQuietly(testFile);
          return System.currentTimeMillis() - startTime;
        } catch (IOException e) {
          LOGGER.warn("Caught exception while checking the data directory operation latency", e);
          return DATA_DIRECTORY_EXCEPTION_VALUE;
        }
      } else {
        return DATA_DIRECTORY_MISSING_VALUE;
      }
    });

    _serviceStatusCallbackList.add(generateServiceStatusCallback(_helixParticipantManager));
  }

  protected PinotLLCRealtimeSegmentManager createPinotLLCRealtimeSegmentManager() {
    return new PinotLLCRealtimeSegmentManager(_helixResourceManager, _config, _controllerMetrics);
  }

  /**
   * Scan all table resources in the cluster and ensure table config and schema exist for each table.
   * TODO: Cleanup orphan table config and schema
   */
  private void enforceTableConfigAndSchema() {
    ZkHelixPropertyStore<ZNRecord> propertyStore = _helixResourceManager.getPropertyStore();
    List<String> tablesWithoutTableConfig = new ArrayList<>();
    List<String> tablesWithoutSchema = new ArrayList<>();
    for (String tableNameWithType : _helixResourceManager.getAllTables()) {
      TableConfig tableConfig = ZKMetadataProvider.getTableConfig(propertyStore, tableNameWithType);
      if (tableConfig == null) {
        tablesWithoutTableConfig.add(tableNameWithType);
        continue;
      }
      Schema schema = ZKMetadataProvider.getTableSchema(propertyStore, tableNameWithType);
      if (schema == null) {
        tablesWithoutSchema.add(tableNameWithType);
      }
    }
    if (!tablesWithoutTableConfig.isEmpty()) {
      LOGGER.error("[CRITICAL!!!] Failed to find table config for tables: {}", tablesWithoutTableConfig);
      if (_config.isExitOnTableConfigCheckFailure()) {
        throw new IllegalStateException("Failed to find table config for tables: " + tablesWithoutTableConfig
            + ", exiting! Please set controller.startup.exitOnTableConfigCheckFailure=false to not exit and fix these "
            + "tables.");
      } else {
        _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.TABLE_WITHOUT_TABLE_CONFIG_COUNT,
            tablesWithoutTableConfig.size());
      }
    }
    if (!tablesWithoutSchema.isEmpty()) {
      LOGGER.error("[CRITICAL!!!] Failed to find schema for tables: {}", tablesWithoutSchema);
      if (_config.isExitOnSchemaCheckFailure()) {
        throw new IllegalStateException("Failed to find schema for tables: " + tablesWithoutSchema
            + ", exiting! Please set controller.startup.exitOnSchemaCheckFailure=false to not exit and fix these "
            + "tables.");
      } else {
        _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.TABLE_WITHOUT_SCHEMA_COUNT,
            tablesWithoutSchema.size());
      }
    }
  }

  private ServiceStatus.ServiceStatusCallback generateServiceStatusCallback(HelixManager helixManager) {
    return new ServiceStatus.ServiceStatusCallback() {
      private volatile boolean _isStarted = false;
      private volatile String _statusDescription = "Helix ZK Not connected as " + helixManager.getInstanceType();

      @Override
      public ServiceStatus.Status getServiceStatus() {
        if (_isStarted) {
          // If we've connected to Helix at some point, the instance status depends on being connected to ZK
          if (helixManager.isConnected()) {
            return ServiceStatus.Status.GOOD;
          } else {
            return ServiceStatus.Status.BAD;
          }
        }

        // Return starting until zk is connected
        if (!helixManager.isConnected()) {
          return ServiceStatus.Status.STARTING;
        } else {
          _isStarted = true;
          _statusDescription = ServiceStatus.STATUS_DESCRIPTION_NONE;
          return ServiceStatus.Status.GOOD;
        }
      }

      @Override
      public String getStatusDescription() {
        return _statusDescription;
      }
    };
  }

  private void initControllerMetrics() {
    _metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry(_config.subset(METRICS_REGISTRY_NAME));
    _controllerMetrics = new ControllerMetrics(_config.getMetricsPrefix(), _metricsRegistry);
    _controllerMetrics.initializeGlobalMeters();
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.VERSION, PinotVersion.VERSION_METRIC_NAME, 1);
    // log zookeeper's JUTE_MAX_BUFFER value, default is 0xfffff bytes (just under 1MB)
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.ZK_JUTE_MAX_BUFFER,
        Integer.getInteger(ZkSystemPropertyKeys.JUTE_MAXBUFFER, 0xfffff));
    ControllerMetrics.register(_controllerMetrics);
    _validationMetrics = new ValidationMetrics(_metricsRegistry);
  }

  private void initPinotFSFactory() {
    LOGGER.info("Initializing PinotFSFactory");

    PinotFSFactory.init(_config.subset(CommonConstants.Controller.PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY));
  }

  private void initControllerFilePathProvider() {
    LOGGER.info("Initializing ControllerFilePathProvider");
    try {
      ControllerFilePathProvider.init(_config);
    } catch (InvalidControllerConfigException e) {
      throw new RuntimeException("Caught exception while initializing ControllerFilePathProvider", e);
    }
  }

  private void initSegmentFetcherFactory() {
    PinotConfiguration segmentFetcherFactoryConfig =
        _config.subset(CommonConstants.Controller.PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY);
    LOGGER.info("Initializing SegmentFetcherFactory");
    try {
      SegmentFetcherFactory.init(segmentFetcherFactoryConfig);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while initializing SegmentFetcherFactory", e);
    }
  }

  private void initPinotCrypterFactory() {
    PinotConfiguration pinotCrypterConfig =
        _config.subset(CommonConstants.Controller.PREFIX_OF_CONFIG_OF_PINOT_CRYPTER);
    LOGGER.info("Initializing PinotCrypterFactory");
    try {
      PinotCrypterFactory.init(pinotCrypterConfig);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while initializing PinotCrypterFactory", e);
    }
  }

  /**
   * Registers, connects to Helix cluster as PARTICIPANT role, and adds listeners.
   */
  private void registerAndConnectAsHelixParticipant() {
    // Registers customized Master-Slave state model to state machine engine, which is for calculating participant
    // assignment in lead controller resource.
    _helixParticipantManager.getStateMachineEngine().registerStateModelFactory(MasterSlaveSMD.name,
        new LeadControllerResourceMasterSlaveStateModelFactory(_leadControllerManager));

    // Connects to cluster.
    try {
      _helixParticipantManager.connect();
    } catch (Exception e) {
      String errorMsg = String.format("Exception when connecting the instance %s as Participant role to Helix.",
          _helixParticipantInstanceId);
      LOGGER.error(errorMsg, e);
      throw new RuntimeException(errorMsg);
    }
    updateInstanceConfigIfNeeded();

    LOGGER.info("Registering helix controller listener");
    // This registration is not needed when the leadControllerResource is enabled.
    // However, the resource can be disabled sometime while the cluster is in operation, so we keep it here. Plus, it
    // does not add much overhead.
    // At some point in future when we stop supporting the disabled resource, we will remove this line altogether and
    // the logic that goes with it.
    _helixParticipantManager.addControllerListener(
        (ControllerChangeListener) changeContext -> _leadControllerManager.onHelixControllerChange());

    LOGGER.info("Registering resource config listener");
    try {
      _helixParticipantManager.addResourceConfigChangeListener(
          (resourceConfigList, changeContext) -> _leadControllerManager.onResourceConfigChange());
    } catch (Exception e) {
      throw new RuntimeException(
          "Error registering resource config listener for " + Helix.LEAD_CONTROLLER_RESOURCE_NAME, e);
    }
  }

  private void updateInstanceConfigIfNeeded() {
    InstanceConfig instanceConfig =
        HelixHelper.getInstanceConfig(_helixParticipantManager, _helixParticipantInstanceId);
    boolean updated = HelixHelper.updateHostnamePort(instanceConfig, _hostname, _port);
    if (_tlsPort > 0) {
      updated |= HelixHelper.updateTlsPort(instanceConfig, _tlsPort);
    }
    updated |= HelixHelper.addDefaultTags(instanceConfig, () -> Collections.singletonList(Helix.CONTROLLER_INSTANCE));
    updated |= HelixHelper.removeDisabledPartitions(instanceConfig);
    updated |= HelixHelper.updatePinotVersion(instanceConfig);

    if (updated) {
      HelixHelper.updateInstanceConfig(_helixParticipantManager, instanceConfig);
    }
  }

  public ControllerConf.ControllerMode getControllerMode() {
    return _controllerMode;
  }

  protected TaskManagerStatusCache<TaskGeneratorMostRecentRunInfo> getTaskManagerStatusCache() {
    return new InMemoryTaskManagerStatusCache();
  }

  @VisibleForTesting
  protected List<PeriodicTask> setupControllerPeriodicTasks() {
    LOGGER.info("Setting up periodic tasks");
    List<PeriodicTask> periodicTasks = new ArrayList<>();
    _taskManagerStatusCache = getTaskManagerStatusCache();
    // Create and add task manager
    _taskManager = createTaskManager();
    _taskManager.init();
    periodicTasks.add(_taskManager);
    BrokerServiceHelper brokerServiceHelper =
        new BrokerServiceHelper(_helixResourceManager, _config, _executorService, _connectionManager);
    _retentionManager = new RetentionManager(_helixResourceManager, _leadControllerManager, _config, _controllerMetrics,
        brokerServiceHelper);
    periodicTasks.add(_retentionManager);
    _offlineSegmentIntervalChecker =
        new OfflineSegmentIntervalChecker(_config, _helixResourceManager, _leadControllerManager,
            new ValidationMetrics(_metricsRegistry), _controllerMetrics);
    periodicTasks.add(_offlineSegmentIntervalChecker);
    _realtimeSegmentValidationManager =
        new RealtimeSegmentValidationManager(_config, _helixResourceManager, _leadControllerManager,
            _pinotLLCRealtimeSegmentManager, _validationMetrics, _controllerMetrics, _storageQuotaChecker,
            _resourceUtilizationManager);
    periodicTasks.add(_realtimeSegmentValidationManager);
    _brokerResourceValidationManager =
        new BrokerResourceValidationManager(_config, _helixResourceManager, _leadControllerManager, _controllerMetrics);
    periodicTasks.add(_brokerResourceValidationManager);
    _segmentStatusChecker =
        new SegmentStatusChecker(_helixResourceManager, _leadControllerManager, _config, _controllerMetrics,
            _tableSizeReader);
    periodicTasks.add(_segmentStatusChecker);
    _rebalanceChecker =
        new RebalanceChecker(_tableRebalanceManager, _helixResourceManager, _leadControllerManager, _config,
            _controllerMetrics);
    periodicTasks.add(_rebalanceChecker);
    _realtimeConsumerMonitor =
        new RealtimeConsumerMonitor(_config, _helixResourceManager, _leadControllerManager, _controllerMetrics,
            _executorService);
    periodicTasks.add(_realtimeConsumerMonitor);
    _segmentRelocator =
        new SegmentRelocator(_tableRebalanceManager, _helixResourceManager, _leadControllerManager, _config,
            _controllerMetrics, _executorService, _connectionManager);
    periodicTasks.add(_segmentRelocator);
    _staleInstancesCleanupTask =
        new StaleInstancesCleanupTask(_helixResourceManager, _leadControllerManager, _config, _controllerMetrics);
    periodicTasks.add(_staleInstancesCleanupTask);
    _taskMetricsEmitter =
        new TaskMetricsEmitter(_helixResourceManager, _helixTaskResourceManager, _leadControllerManager, _config,
            _controllerMetrics);
    periodicTasks.add(_taskMetricsEmitter);
    PeriodicTask responseStoreCleaner = new ResponseStoreCleaner(_config, _helixResourceManager, _leadControllerManager,
        _controllerMetrics, _executorService, _connectionManager);
    periodicTasks.add(responseStoreCleaner);
    PeriodicTask resourceUtilizationChecker = new ResourceUtilizationChecker(_config, _connectionManager,
        _controllerMetrics, _diskUtilizationChecker, _executorService, _helixResourceManager);
    periodicTasks.add(resourceUtilizationChecker);

    return periodicTasks;
  }

  /**
   * Creates a TaskManager instance  as specified in the configuration.
   */
  private PinotTaskManager createTaskManager() {
    String taskManagerClass = _config.getProperty(CommonConstants.Controller.CONFIG_OF_TASK_MANAGER_CLASS,
        CommonConstants.Controller.DEFAULT_TASK_MANAGER_CLASS);
    LOGGER.info("Creating TaskManager with class: {}", taskManagerClass);
    try {
      return PluginManager.get().createInstance(taskManagerClass,
          new Class[]{PinotHelixTaskResourceManager.class, PinotHelixResourceManager.class, LeadControllerManager.class,
              ControllerConf.class, ControllerMetrics.class, TaskManagerStatusCache.class,
              Executor.class, PoolingHttpClientConnectionManager.class, ResourceUtilizationManager.class},
          new Object[]{_helixTaskResourceManager, _helixResourceManager, _leadControllerManager,
              _config, _controllerMetrics, _taskManagerStatusCache, _executorService,
              _connectionManager, _resourceUtilizationManager});
    } catch (Exception e) {
      LOGGER.error("Failed to create task manager with class: {}", taskManagerClass, e);
      throw new RuntimeException("Failed to create task manager with class: " + taskManagerClass, e);
    }
  }

  @Override
  public void stop() {
    switch (_controllerMode) {
      case DUAL:
        stopPinotController();
        stopHelixController();
        break;
      case PINOT_ONLY:
        stopPinotController();
        break;
      case HELIX_ONLY:
        stopHelixController();
        break;
      default:
        break;
    }
    LOGGER.info("Deregistering service status handler");
    ServiceStatus.removeServiceStatusCallback(_helixParticipantInstanceId);
    LOGGER.info("Shutdown Controller Metrics Registry");
    _metricsRegistry.shutdown();
    LOGGER.info("Finish shutting down Pinot controller for {}", _helixParticipantInstanceId);
  }

  private void stopHelixController() {
    LOGGER.info("Disconnecting helix controller zk manager");
    _helixControllerManager.disconnect();
  }

  private void stopPinotController() {
    try {
      // Stopping periodic tasks has to be done before stopping HelixResourceManager.
      // Stop controller periodic task.
      LOGGER.info("Stopping controller periodic tasks");
      _periodicTaskScheduler.stop();

      LOGGER.info("Stopping lead controller manager");
      _leadControllerManager.stop();

      // Stop PinotLLCSegmentManager before stopping Jersey API. It is possible that stopping Jersey API
      // may interrupt the handlers waiting on an I/O.
      _pinotLLCRealtimeSegmentManager.stop();

      LOGGER.info("Closing PinotFS classes");
      PinotFSFactory.shutdown();

      LOGGER.info("Stopping Jersey admin API");
      _adminApp.stop();

      LOGGER.info("Stopping resource manager");
      _helixResourceManager.stop();

      LOGGER.info("Disconnecting helix participant zk manager");
      _helixParticipantManager.disconnect();

      LOGGER.info("Shutting down http connection manager");
      _connectionManager.close();

      LOGGER.info("Shutting down executor service");
      _executorService.shutdownNow();
      _executorService.awaitTermination(10L, TimeUnit.SECONDS);
      _rebalancerExecutorService.shutdownNow();
      _rebalancerExecutorService.awaitTermination(10L, TimeUnit.SECONDS);
    } catch (final Exception e) {
      LOGGER.error("Caught exception while shutting down", e);
    }
  }

  public PinotMetricsRegistry getMetricsRegistry() {
    return _metricsRegistry;
  }

  @VisibleForTesting
  public ControllerMetrics getControllerMetrics() {
    return _controllerMetrics;
  }

  protected ControllerAdminApiApplication createControllerAdminApp() {
    return new ControllerAdminApiApplication(_config);
  }

  /**
   * Return the PeriodicTaskScheduler instance so that the periodic tasks can be tested.
   * @return PeriodicTaskScheduler.
   */
  @VisibleForTesting
  public PeriodicTaskScheduler getPeriodicTaskScheduler() {
    return _periodicTaskScheduler;
  }
}
