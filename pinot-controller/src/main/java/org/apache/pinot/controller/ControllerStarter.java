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
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.task.TaskDriver;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.MetricsHelper;
import org.apache.pinot.common.metrics.ValidationMetrics;
import org.apache.pinot.common.segment.fetcher.SegmentFetcherFactory;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.controller.api.ControllerAdminApiApplication;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.events.MetadataEventNotifierFactory;
import org.apache.pinot.controller.helix.SegmentStatusChecker;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTaskScheduler;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.controller.helix.core.realtime.PinotRealtimeSegmentManager;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceSegmentStrategyFactory;
import org.apache.pinot.controller.helix.core.relocation.RealtimeSegmentRelocator;
import org.apache.pinot.controller.helix.core.retention.RetentionManager;
import org.apache.pinot.controller.validation.BrokerResourceValidationManager;
import org.apache.pinot.controller.validation.OfflineSegmentIntervalChecker;
import org.apache.pinot.controller.validation.RealtimeSegmentValidationManager;
import org.apache.pinot.core.crypt.PinotCrypterFactory;
import org.apache.pinot.core.periodictask.PeriodicTask;
import org.apache.pinot.filesystem.PinotFSFactory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ControllerStarter {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerStarter.class);

  private static final String METRICS_REGISTRY_NAME = "pinot.controller.metrics";
  private static final Long DATA_DIRECTORY_MISSING_VALUE = 1000000L;
  private static final Long DATA_DIRECTORY_EXCEPTION_VALUE = 1100000L;
  private static final String METADATA_EVENT_NOTIFIER_PREFIX = "metadata.event.notifier";

  private final ControllerConf _config;
  private final ControllerAdminApiApplication _adminApp;
  private final PinotHelixResourceManager _helixResourceManager;
  private final MetricsRegistry _metricsRegistry;
  private final ControllerMetrics _controllerMetrics;
  private final ExecutorService _executorService;

  // Can only be constructed after resource manager getting started
  private OfflineSegmentIntervalChecker _offlineSegmentIntervalChecker;
  private RealtimeSegmentValidationManager _realtimeSegmentValidationManager;
  private BrokerResourceValidationManager _brokerResourceValidationManager;
  private RealtimeSegmentRelocator _realtimeSegmentRelocator;
  private RetentionManager _retentionManager;
  private SegmentStatusChecker _segmentStatusChecker;
  private PinotTaskManager _taskManager;
  private ControllerPeriodicTaskScheduler _controllerPeriodicTaskScheduler;
  private PinotHelixTaskResourceManager _helixTaskResourceManager;
  private PinotRealtimeSegmentManager _realtimeSegmentsManager;

  public ControllerStarter(ControllerConf conf) {
    _config = conf;
    _adminApp = new ControllerAdminApiApplication(_config.getQueryConsoleWebappPath(), _config.getQueryConsoleUseHttps());
    // Do not use this before the invocation of {@link PinotHelixResourceManager::start()}, which happens in {@link ControllerStarter::start()}
    _helixResourceManager = new PinotHelixResourceManager(_config);
    _metricsRegistry = new MetricsRegistry();
    _controllerMetrics = new ControllerMetrics(_metricsRegistry);
    _executorService =
        Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("restapi-multiget-thread-%d").build());
  }

  public PinotHelixResourceManager getHelixResourceManager() {
    return _helixResourceManager;
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



  public void start() {
    LOGGER.info("Starting Pinot controller");

    Utils.logVersions();

    // Set up controller metrics
    MetricsHelper.initializeMetrics(_config.subset(METRICS_REGISTRY_NAME));
    MetricsHelper.registerMetricsRegistry(_metricsRegistry);

    Configuration pinotFSConfig = _config.subset(CommonConstants.Controller.PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY);
    Configuration segmentFetcherFactoryConfig =
        _config.subset(CommonConstants.Controller.PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY);
    Configuration pinotCrypterConfig = _config.subset(CommonConstants.Controller.PREFIX_OF_CONFIG_OF_PINOT_CRYPTER);

    // Start all components
    LOGGER.info("Initializing PinotFSFactory");
    try {
      PinotFSFactory.init(pinotFSConfig);
    } catch (Exception e) {
      Utils.rethrowException(e);
    }

    LOGGER.info("Initializing SegmentFetcherFactory");
    try {
      SegmentFetcherFactory.getInstance()
          .init(segmentFetcherFactoryConfig);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while initializing SegmentFetcherFactory", e);
    }

    LOGGER.info("Initializing PinotCrypterFactory");
    try {
      PinotCrypterFactory.init(pinotCrypterConfig);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while initializing PinotCrypterFactory", e);
    }

    LOGGER.info("Starting Pinot Helix resource manager and connecting to Zookeeper");
    _helixResourceManager.start();
    final HelixManager helixManager = _helixResourceManager.getHelixZkManager();

    LOGGER.info("Init controller leadership manager");
    ControllerLeadershipManager.init(helixManager);

    LOGGER.info("Starting task resource manager");
    _helixTaskResourceManager = new PinotHelixTaskResourceManager(new TaskDriver(helixManager));

    // Helix resource manager must be started in order to create PinotLLCRealtimeSegmentManager
    LOGGER.info("Starting realtime segment manager");
    PinotLLCRealtimeSegmentManager.create(_helixResourceManager, _config, _controllerMetrics);
    _realtimeSegmentsManager = new PinotRealtimeSegmentManager(_helixResourceManager);
    _realtimeSegmentsManager.start(_controllerMetrics);

    // Setting up periodic tasks
    List<PeriodicTask> controllerPeriodicTasks = setupControllerPeriodicTasks();
    LOGGER.info("Init controller periodic tasks scheduler");
    _controllerPeriodicTaskScheduler = new ControllerPeriodicTaskScheduler();
    _controllerPeriodicTaskScheduler.init(controllerPeriodicTasks);

    LOGGER.info("Creating rebalance segments factory");
    RebalanceSegmentStrategyFactory.createInstance(helixManager);

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

    int jerseyPort = Integer.parseInt(_config.getControllerPort());

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
        bind(_taskManager).to(PinotTaskManager.class);
        bind(connectionManager).to(HttpConnectionManager.class);
        bind(_executorService).to(Executor.class);
        bind(_controllerMetrics).to(ControllerMetrics.class);
        bind(accessControlFactory).to(AccessControlFactory.class);
        bind(metadataEventNotifierFactory).to(MetadataEventNotifierFactory.class);
      }
    });

    _adminApp.start(jerseyPort);
    LOGGER.info("Started Jersey API on port {}", jerseyPort);
    LOGGER.info("Pinot controller ready and listening on port {} for API requests", _config.getControllerPort());
    LOGGER.info("Controller services available at http://{}:{}/", _config.getControllerHost(),
        _config.getControllerPort());

    _controllerMetrics.addCallbackGauge("helix.connected", () -> helixManager.isConnected() ? 1L : 0L);
    _controllerMetrics.addCallbackGauge("helix.leader", () -> helixManager.isLeader() ? 1L : 0L);
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

    ServiceStatus.setServiceStatusCallback(new ServiceStatus.ServiceStatusCallback() {
      private boolean _isStarted = false;
      private String _statusDescription = "Helix ZK Not connected";

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
    });

    helixManager.addPreConnectCallback(
        () -> _controllerMetrics.addMeteredGlobalValue(ControllerMeter.HELIX_ZOOKEEPER_RECONNECTS, 1L));
    _controllerMetrics.initializeGlobalMeters();
  }

  @VisibleForTesting
  protected List<PeriodicTask> setupControllerPeriodicTasks() {
    LOGGER.info("Setting up periodic tasks");
    List<PeriodicTask> periodicTasks = new ArrayList<>();
    _taskManager = new PinotTaskManager(_helixTaskResourceManager, _helixResourceManager, _config, _controllerMetrics);
    periodicTasks.add(_taskManager);
    _retentionManager = new RetentionManager(_helixResourceManager, _config);
    periodicTasks.add(_retentionManager);
    _offlineSegmentIntervalChecker =
        new OfflineSegmentIntervalChecker(_config, _helixResourceManager, new ValidationMetrics(_metricsRegistry));
    periodicTasks.add(_offlineSegmentIntervalChecker);
    _realtimeSegmentValidationManager =
        new RealtimeSegmentValidationManager(_config, _helixResourceManager, PinotLLCRealtimeSegmentManager.getInstance(),
            new ValidationMetrics(_metricsRegistry));
    periodicTasks.add(_realtimeSegmentValidationManager);
    _brokerResourceValidationManager =
        new BrokerResourceValidationManager(_config, _helixResourceManager);
    periodicTasks.add(_brokerResourceValidationManager);
    _segmentStatusChecker = new SegmentStatusChecker(_helixResourceManager, _config, _controllerMetrics);
    periodicTasks.add(_segmentStatusChecker);
    _realtimeSegmentRelocator = new RealtimeSegmentRelocator(_helixResourceManager, _config);
    periodicTasks.add(_realtimeSegmentRelocator);

    return periodicTasks;
  }

  public void stop() {
    try {
      LOGGER.info("Stopping controller leadership manager");
      ControllerLeadershipManager.getInstance().stop();

      // Stop PinotLLCSegmentManager before stopping Jersey API. It is possible that stopping Jersey API
      // may interrupt the handlers waiting on an I/O.
      PinotLLCRealtimeSegmentManager.getInstance().stop();

      LOGGER.info("Closing PinotFS classes");
      PinotFSFactory.shutdown();

      LOGGER.info("Stopping Jersey admin API");
      _adminApp.stop();

      LOGGER.info("Stopping realtime segment manager");
      _realtimeSegmentsManager.stop();

      LOGGER.info("Stopping resource manager");
      _helixResourceManager.stop();

      _executorService.shutdownNow();

    } catch (final Exception e) {
      LOGGER.error("Caught exception while shutting down", e);
    }
  }

  public MetricsRegistry getMetricsRegistry() {
    return _metricsRegistry;
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
    conf.setControllerVipProtocol("http");
    conf.setRetentionControllerFrequencyInSeconds(3600 * 6);
    conf.setOfflineSegmentIntervalCheckerFrequencyInSeconds(3600);
    conf.setRealtimeSegmentValidationFrequencyInSeconds(3600);
    conf.setBrokerResourceValidationFrequencyInSeconds(3600);
    conf.setStatusCheckerFrequencyInSeconds(5 * 60);
    conf.setRealtimeSegmentRelocatorFrequency("1h");
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
