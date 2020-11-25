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
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.api.listeners.ControllerChangeListener;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.Message;
import org.apache.helix.task.TaskDriver;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.MetricsHelper;
import org.apache.pinot.common.metrics.ValidationMetrics;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.NetUtil;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.controller.api.ControllerAdminApiApplication;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.events.MetadataEventNotifierFactory;
import org.apache.pinot.controller.api.listeners.ListenerConfig;
import org.apache.pinot.controller.api.resources.ControllerFilePathProvider;
import org.apache.pinot.controller.api.resources.InvalidControllerConfigException;
import org.apache.pinot.controller.helix.SegmentStatusChecker;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.controller.helix.core.realtime.PinotRealtimeSegmentManager;
import org.apache.pinot.controller.helix.core.realtime.SegmentCompletionManager;
import org.apache.pinot.controller.helix.core.relocation.SegmentRelocator;
import org.apache.pinot.controller.helix.core.retention.RetentionManager;
import org.apache.pinot.controller.helix.core.statemodel.LeadControllerResourceMasterSlaveStateModelFactory;
import org.apache.pinot.controller.helix.core.util.HelixSetupUtils;
import org.apache.pinot.controller.helix.starter.HelixConfig;
import org.apache.pinot.controller.util.ListenerConfigUtil;
import org.apache.pinot.controller.validation.BrokerResourceValidationManager;
import org.apache.pinot.controller.validation.OfflineSegmentIntervalChecker;
import org.apache.pinot.controller.validation.RealtimeSegmentValidationManager;
import org.apache.pinot.core.periodictask.PeriodicTask;
import org.apache.pinot.core.periodictask.PeriodicTaskScheduler;
import org.apache.pinot.spi.crypt.PinotCrypterFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.spi.services.ServiceStartable;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ControllerStarter implements ServiceStartable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerStarter.class);

  private static final String METRICS_REGISTRY_NAME = "pinot.controller.metrics";
  private static final Long DATA_DIRECTORY_MISSING_VALUE = 1000000L;
  private static final Long DATA_DIRECTORY_EXCEPTION_VALUE = 1100000L;
  private static final String METADATA_EVENT_NOTIFIER_PREFIX = "metadata.event.notifier";
  private static final String MAX_STATE_TRANSITIONS_PER_INSTANCE =  "MaxStateTransitionsPerInstance";

  private final ControllerConf _config;
  private final List<ListenerConfig> _listenerConfigs;
  private final ControllerAdminApiApplication _adminApp;
  // TODO: rename this variable once it's full separated with Helix controller.
  private final PinotHelixResourceManager _helixResourceManager;
  private final MetricsRegistry _metricsRegistry;
  private final ControllerMetrics _controllerMetrics;
  private final ExecutorService _executorService;

  private final String _helixZkURL;
  private final String _helixClusterName;
  private final String _helixControllerInstanceId;
  private final String _helixParticipantInstanceId;
  private final boolean _isUpdateStateModel;
  private final boolean _enableBatchMessageMode;
  private final ControllerConf.ControllerMode _controllerMode;

  private HelixManager _helixControllerManager;
  private HelixManager _helixParticipantManager;

  // Can only be constructed after resource manager getting started
  private OfflineSegmentIntervalChecker _offlineSegmentIntervalChecker;
  private RealtimeSegmentValidationManager _realtimeSegmentValidationManager;
  private BrokerResourceValidationManager _brokerResourceValidationManager;
  private SegmentRelocator _segmentRelocator;
  private RetentionManager _retentionManager;
  private SegmentStatusChecker _segmentStatusChecker;
  private PinotTaskManager _taskManager;
  private PeriodicTaskScheduler _periodicTaskScheduler;
  private PinotHelixTaskResourceManager _helixTaskResourceManager;
  private PinotRealtimeSegmentManager _realtimeSegmentsManager;
  private PinotLLCRealtimeSegmentManager _pinotLLCRealtimeSegmentManager;
  private SegmentCompletionManager _segmentCompletionManager;
  private LeadControllerManager _leadControllerManager;
  private List<ServiceStatus.ServiceStatusCallback> _serviceStatusCallbackList;

  public ControllerStarter(ControllerConf conf) {
    _config = conf;
    inferHostnameIfNeeded(_config);
    setupHelixSystemProperties();

    _controllerMode = conf.getControllerMode();
    // Helix related settings.
    _helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(_config.getZkStr());
    _helixClusterName = _config.getHelixClusterName();
    _listenerConfigs = ListenerConfigUtil.buildListenerConfigs(_config);
    
    String host = conf.getControllerHost();
    int port = inferPort();
    
    _helixControllerInstanceId = host + "_" + port;
    _helixParticipantInstanceId = LeadControllerUtils.generateParticipantInstanceId(host, port);
    _isUpdateStateModel = _config.isUpdateSegmentStateModel();
    _enableBatchMessageMode = _config.getEnableBatchMessageMode();

    _metricsRegistry = new MetricsRegistry();
    _controllerMetrics = new ControllerMetrics(conf.getMetricsPrefix(), _metricsRegistry);
    _serviceStatusCallbackList = new ArrayList<>();
    if (_controllerMode == ControllerConf.ControllerMode.HELIX_ONLY) {
      _adminApp = null;
      _helixResourceManager = null;
      _executorService = null;
    } else {
      // Initialize FunctionRegistry before starting the admin application (PinotQueryResource requires it to compile
      // queries)
      FunctionRegistry.init();
      _adminApp = new ControllerAdminApiApplication();
      // Do not use this before the invocation of {@link PinotHelixResourceManager::start()}, which happens in {@link ControllerStarter::start()}
      _helixResourceManager = new PinotHelixResourceManager(_config);
      _executorService =
          Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("restapi-multiget-thread-%d").build());
    }
  }

  private void inferHostnameIfNeeded(ControllerConf config) {
    if (config.getControllerHost() == null) {
      if (config.getProperty(CommonConstants.Helix.SET_INSTANCE_ID_TO_HOSTNAME_KEY, false)) {
        final String inferredHostname = NetUtil.getHostnameOrAddress();
        if (inferredHostname != null) {
          config.setControllerHost(inferredHostname);
        } else {
          throw new RuntimeException(
              "Failed to infer controller hostname, please set controller instanceId explicitly in config file.");
        }
      }
    }
  }

  private int inferPort() {
    return Optional.ofNullable(_config.getControllerPort()).map(Integer::parseInt)

        // Fall back to protocol listeners if legacy controller.port is undefined. 
        .orElseGet(() -> _listenerConfigs.stream().findFirst().map(ListenerConfig::getPort).get());
  }

  private void setupHelixSystemProperties() {
    // NOTE: Helix will disconnect the manager and disable the instance if it detects flapping (too frequent disconnect
    // from ZooKeeper). Setting flapping time window to a small value can avoid this from happening. Helix ignores the
    // non-positive value, so set the default value as 1.
    System.setProperty(SystemPropertyKeys.FLAPPING_TIME_WINDOW, _config
        .getProperty(CommonConstants.Helix.CONFIG_OF_CONTROLLER_FLAPPING_TIME_WINDOW_MS,
            CommonConstants.Helix.DEFAULT_FLAPPING_TIME_WINDOW_MS));
  }

  private void setupHelixClusterConstraints() {
    String maxStateTransitions = _config
        .getProperty(CommonConstants.Helix.CONFIG_OF_HELIX_INSTANCE_MAX_STATE_TRANSITIONS,
            CommonConstants.Helix.DEFAULT_HELIX_INSTANCE_MAX_STATE_TRANSITIONS);
    Map<ClusterConstraints.ConstraintAttribute, String> constraintAttributes = new HashMap<>();
    constraintAttributes.put(ClusterConstraints.ConstraintAttribute.INSTANCE, ".*");
    constraintAttributes
        .put(ClusterConstraints.ConstraintAttribute.MESSAGE_TYPE, Message.MessageType.STATE_TRANSITION.name());
    ConstraintItem constraintItem = new ConstraintItem(constraintAttributes, maxStateTransitions);

    _helixControllerManager.getClusterManagmentTool()
        .setConstraint(_helixClusterName, ClusterConstraints.ConstraintType.MESSAGE_CONSTRAINT,
            MAX_STATE_TRANSITIONS_PER_INSTANCE, constraintItem);
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

  @Override
  public ServiceRole getServiceRole() {
    return ServiceRole.CONTROLLER;
  }

  @Override
  public String getInstanceId() {
    return _helixParticipantInstanceId;
  }

  @Override
  public PinotConfiguration getConfig() {
    return _config;
  }

  @Override
  public void start() {
    LOGGER.info("Starting Pinot controller in mode: {}.", _controllerMode.name());
    Utils.logVersions();

    // Set up controller metrics
    MetricsHelper.initializeMetrics(_config.subset(METRICS_REGISTRY_NAME));
    MetricsHelper.registerMetricsRegistry(_metricsRegistry);
    _controllerMetrics.initializeGlobalMeters();

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
        LOGGER.error("Invalid mode: " + _controllerMode);
    }

    ServiceStatus
        .setServiceStatusCallback(_helixParticipantInstanceId, new ServiceStatus.MultipleCallbackServiceStatusCallback(_serviceStatusCallbackList));
  }

  private void setUpHelixController() {
    // Register and connect instance as Helix controller.
    LOGGER.info("Starting Helix controller");
    _helixControllerManager =
        HelixSetupUtils.setupHelixController(_helixClusterName, _helixZkURL, _helixControllerInstanceId);

    // Emit helix controller metrics
    _controllerMetrics.addCallbackGauge(CommonConstants.Helix.INSTANCE_CONNECTED_METRIC_NAME,
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
    // Set up Pinot cluster in Helix if needed
    HelixSetupUtils.setupPinotCluster(_helixClusterName, _helixZkURL, _isUpdateStateModel, _enableBatchMessageMode,
        _config.getLeadControllerResourceRebalanceStrategy());

    // Start all components
    initPinotFSFactory();
    initControllerFilePathProvider();
    initSegmentFetcherFactory();
    initPinotCrypterFactory();

    LOGGER.info("Initializing Helix participant manager");
    _helixParticipantManager = HelixManagerFactory
        .getZKHelixManager(_helixClusterName, _helixParticipantInstanceId, InstanceType.PARTICIPANT, _helixZkURL);

    // LeadControllerManager needs to be initialized before registering as Helix participant.
    LOGGER.info("Initializing lead controller manager");
    _leadControllerManager = new LeadControllerManager(_helixParticipantManager, _controllerMetrics);

    LOGGER.info("Registering and connecting Helix participant manager as Helix Participant role");
    registerAndConnectAsHelixParticipant();

    // LeadControllerManager needs to be started after the connection
    // as it can check Helix leadership and resource config only after connecting to Helix cluster.
    LOGGER.info("Starting lead controller manager");
    _leadControllerManager.start();

    LOGGER.info("Starting Pinot Helix resource manager and connecting to Zookeeper");
    _helixResourceManager.start(_helixParticipantManager);

    LOGGER.info("Starting task resource manager");
    _helixTaskResourceManager = new PinotHelixTaskResourceManager(new TaskDriver(_helixParticipantManager));

    // Helix resource manager must be started in order to create PinotLLCRealtimeSegmentManager
    LOGGER.info("Starting realtime segment manager");
    _pinotLLCRealtimeSegmentManager =
        new PinotLLCRealtimeSegmentManager(_helixResourceManager, _config, _controllerMetrics);
    // TODO: Need to put this inside HelixResourceManager when HelixControllerLeadershipManager is removed.
    _helixResourceManager.registerPinotLLCRealtimeSegmentManager(_pinotLLCRealtimeSegmentManager);
    _segmentCompletionManager =
        new SegmentCompletionManager(_helixParticipantManager, _pinotLLCRealtimeSegmentManager, _controllerMetrics,
            _leadControllerManager, _config.getSegmentCommitTimeoutSeconds());

    if (_config.getHLCTablesAllowed()) {
      LOGGER.info("Realtime tables with High Level consumers will be supported");
      _realtimeSegmentsManager = new PinotRealtimeSegmentManager(_helixResourceManager, _leadControllerManager);
      _realtimeSegmentsManager.start(_controllerMetrics);
    } else {
      LOGGER.info("Realtime tables with High Level consumers will NOT be supported");
      _realtimeSegmentsManager = null;
    }

    // Setting up periodic tasks
    List<PeriodicTask> controllerPeriodicTasks = setupControllerPeriodicTasks();
    LOGGER.info("Init controller periodic tasks scheduler");
    _periodicTaskScheduler = new PeriodicTaskScheduler();
    _periodicTaskScheduler.init(controllerPeriodicTasks);
    _periodicTaskScheduler.start();

    String accessControlFactoryClass = _config.getAccessControlFactoryClass();
    LOGGER.info("Use class: {} as the AccessControlFactory", accessControlFactoryClass);
    final AccessControlFactory accessControlFactory;
    try {
      accessControlFactory = (AccessControlFactory) Class.forName(accessControlFactoryClass).newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while creating new AccessControlFactory instance", e);
    }

    final MetadataEventNotifierFactory metadataEventNotifierFactory =
        MetadataEventNotifierFactory.loadFactory(_config.subset(METADATA_EVENT_NOTIFIER_PREFIX));

    LOGGER.info("Controller download url base: {}", _config.generateVipUrl());
    LOGGER.info("Injecting configuration and resource managers to the API context");
    final MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
    connectionManager.getParams().setConnectionTimeout(_config.getServerAdminRequestTimeoutSeconds() * 1000);
    // register all the controller objects for injection to jersey resources
    _adminApp.registerBinder(new AbstractBinder() {
      @Override
      protected void configure() {
        bind(_config).to(ControllerConf.class);
        bind(_helixResourceManager).to(PinotHelixResourceManager.class);
        bind(_helixTaskResourceManager).to(PinotHelixTaskResourceManager.class);
        bind(_segmentCompletionManager).to(SegmentCompletionManager.class);
        bind(_taskManager).to(PinotTaskManager.class);
        bind(connectionManager).to(HttpConnectionManager.class);
        bind(_executorService).to(Executor.class);
        bind(_controllerMetrics).to(ControllerMetrics.class);
        bind(accessControlFactory).to(AccessControlFactory.class);
        bind(metadataEventNotifierFactory).to(MetadataEventNotifierFactory.class);
        bind(_leadControllerManager).to(LeadControllerManager.class);
      }
    });

    _adminApp.start(_listenerConfigs);

    _listenerConfigs.stream().forEach(listenerConfig -> LOGGER.info("Controller services available at {}://{}:{}/",
        listenerConfig.getProtocol(), listenerConfig.getHost(), listenerConfig.getPort()));

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

  private ServiceStatus.ServiceStatusCallback generateServiceStatusCallback(HelixManager helixManager) {
    return new ServiceStatus.ServiceStatusCallback() {
      private boolean _isStarted = false;
      private String _statusDescription = "Helix ZK Not connected as " + helixManager.getInstanceType();

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
    PinotConfiguration pinotCrypterConfig = _config.subset(CommonConstants.Controller.PREFIX_OF_CONFIG_OF_PINOT_CRYPTER);
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
    // Registers customized Master-Slave state model to state machine engine, which is for calculating participant assignment in lead controller resource.
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

    LOGGER.info("Registering helix controller listener");
    // This registration is not needed when the leadControllerResource is enabled.
    // However, the resource can be disabled sometime while the cluster is in operation, so we keep it here. Plus, it does not add much overhead.
    // At some point in future when we stop supporting the disabled resource, we will remove this line altogether and the logic that goes with it.
    _helixParticipantManager.addControllerListener(
        (ControllerChangeListener) changeContext -> _leadControllerManager.onHelixControllerChange());

    LOGGER.info("Registering resource config listener");
    try {
      _helixParticipantManager.addResourceConfigChangeListener(
          (resourceConfigList, changeContext) -> _leadControllerManager.onResourceConfigChange());
    } catch (Exception e) {
      throw new RuntimeException(
          "Error registering resource config listener for " + CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME, e);
    }
  }

  public ControllerConf.ControllerMode getControllerMode() {
    return _controllerMode;
  }

  @VisibleForTesting
  protected List<PeriodicTask> setupControllerPeriodicTasks() {
    LOGGER.info("Setting up periodic tasks");
    List<PeriodicTask> periodicTasks = new ArrayList<>();
    _taskManager =
        new PinotTaskManager(_helixTaskResourceManager, _helixResourceManager, _leadControllerManager, _config,
            _controllerMetrics);
    periodicTasks.add(_taskManager);
    _retentionManager =
        new RetentionManager(_helixResourceManager, _leadControllerManager, _config, _controllerMetrics);
    periodicTasks.add(_retentionManager);
    _offlineSegmentIntervalChecker =
        new OfflineSegmentIntervalChecker(_config, _helixResourceManager, _leadControllerManager,
            new ValidationMetrics(_metricsRegistry), _controllerMetrics);
    periodicTasks.add(_offlineSegmentIntervalChecker);
    _realtimeSegmentValidationManager =
        new RealtimeSegmentValidationManager(_config, _helixResourceManager, _leadControllerManager,
            _pinotLLCRealtimeSegmentManager, new ValidationMetrics(_metricsRegistry), _controllerMetrics);
    periodicTasks.add(_realtimeSegmentValidationManager);
    _brokerResourceValidationManager =
        new BrokerResourceValidationManager(_config, _helixResourceManager, _leadControllerManager, _controllerMetrics);
    periodicTasks.add(_brokerResourceValidationManager);
    _segmentStatusChecker =
        new SegmentStatusChecker(_helixResourceManager, _leadControllerManager, _config, _controllerMetrics);
    periodicTasks.add(_segmentStatusChecker);
    _segmentRelocator = new SegmentRelocator(_helixResourceManager, _leadControllerManager, _config, _controllerMetrics,
        _executorService);
    periodicTasks.add(_segmentRelocator);

    return periodicTasks;
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

      if (_realtimeSegmentsManager != null) {
        LOGGER.info("Stopping realtime segment manager");
        _realtimeSegmentsManager.stop();
      }

      LOGGER.info("Stopping resource manager");
      _helixResourceManager.stop();

      LOGGER.info("Disconnecting helix participant zk manager");
      _helixParticipantManager.disconnect();

      LOGGER.info("Shutting down executor service");
      _executorService.shutdownNow();
      _executorService.awaitTermination(10L, TimeUnit.SECONDS);
    } catch (final Exception e) {
      LOGGER.error("Caught exception while shutting down", e);
    }
  }

  public MetricsRegistry getMetricsRegistry() {
    return _metricsRegistry;
  }

  @VisibleForTesting
  public ControllerMetrics getControllerMetrics() {
    return _controllerMetrics;
  }

  public static ControllerStarter startDefault() {
    return startDefault(null);
  }

  public static ControllerStarter startDefault(File webappPath) {
    final ControllerConf conf = new ControllerConf();
    conf.setControllerHost("localhost");
    conf.setControllerPort("9000");
    conf.setDataDir("/tmp/PinotController");
    conf.setZkStr("localhost:2122");
    conf.setHelixClusterName("quickstart");
    if (webappPath == null) {
      String path = ControllerStarter.class.getClassLoader().getResource("webapp").getFile();
      if (!path.startsWith("file://")) {
        path = "file://" + path;
      }
      conf.setQueryConsolePath(path);
    } else {
      conf.setQueryConsolePath("file://" + webappPath.getAbsolutePath());
    }

    conf.setControllerVipHost("localhost");
    conf.setControllerVipProtocol(CommonConstants.HTTP_PROTOCOL);
    conf.setRetentionControllerFrequencyInSeconds(3600 * 6);
    conf.setOfflineSegmentIntervalCheckerFrequencyInSeconds(3600);
    conf.setRealtimeSegmentValidationFrequencyInSeconds(3600);
    conf.setBrokerResourceValidationFrequencyInSeconds(3600);
    conf.setStatusCheckerFrequencyInSeconds(5 * 60);
    conf.setSegmentRelocatorFrequencyInSeconds(3600);
    conf.setStatusCheckerWaitForPushTimeInSeconds(10 * 60);
    conf.setTenantIsolationEnabled(true);

    final ControllerStarter starter = new ControllerStarter(conf);

    starter.start();
    return starter;
  }

  public static void main(String[] args) {
    startDefault();
  }
}
