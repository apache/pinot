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
package com.linkedin.pinot.controller;

import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.metrics.MetricsHelper;
import com.linkedin.pinot.common.metrics.ValidationMetrics;
import com.linkedin.pinot.common.segment.fetcher.SegmentFetcherFactory;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ServiceStatus;
import com.linkedin.pinot.controller.api.ControllerAdminApiApplication;
import com.linkedin.pinot.controller.api.access.AccessControlFactory;
import com.linkedin.pinot.controller.api.events.MetadataEventNotifierFactory;
import com.linkedin.pinot.controller.helix.SegmentStatusChecker;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.PinotTaskManager;
import com.linkedin.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import com.linkedin.pinot.controller.helix.core.realtime.PinotRealtimeSegmentManager;
import com.linkedin.pinot.controller.helix.core.rebalance.RebalanceSegmentStrategyFactory;
import com.linkedin.pinot.controller.helix.core.retention.RetentionManager;
import com.linkedin.pinot.controller.validation.ValidationManager;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.io.FileUtils;
import org.apache.helix.PreConnectCallback;
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
  private final RetentionManager _retentionManager;
  private final MetricsRegistry _metricsRegistry;
  private final ControllerMetrics _controllerMetrics;
  private final PinotRealtimeSegmentManager _realtimeSegmentsManager;
  private final SegmentStatusChecker _segmentStatusChecker;
  private final ExecutorService _executorService;

  // Can only be constructed after resource manager getting started
  private ValidationManager _validationManager;
  private PinotHelixTaskResourceManager _helixTaskResourceManager;
  private PinotTaskManager _taskManager;

  public ControllerStarter(ControllerConf conf) {
    _config = conf;
    _adminApp = new ControllerAdminApiApplication(_config.getQueryConsole());
    _helixResourceManager = new PinotHelixResourceManager(_config);
    _retentionManager = new RetentionManager(_helixResourceManager, _config.getRetentionControllerFrequencyInSeconds(),
        _config.getDeletedSegmentsRetentionInDays());
    _metricsRegistry = new MetricsRegistry();
    _controllerMetrics = new ControllerMetrics(_metricsRegistry);
    _realtimeSegmentsManager = new PinotRealtimeSegmentManager(_helixResourceManager);
    _executorService = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat("restapi-multiget-thread-%d").build());
    _segmentStatusChecker = new SegmentStatusChecker(_helixResourceManager, _config, _controllerMetrics);
  }

  public PinotHelixResourceManager getHelixResourceManager() {
    return _helixResourceManager;
  }

  public ValidationManager getValidationManager() {
    return _validationManager;
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

    // Start all components
    try {
      LOGGER.info("initializing segment fetchers for all protocols");
      SegmentFetcherFactory.getInstance()
          .init(_config.subset(CommonConstants.Controller.PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY));

      LOGGER.info("Starting Pinot Helix resource manager and connecting to Zookeeper");
      _helixResourceManager.start();

      LOGGER.info("Starting task resource manager");
      _helixTaskResourceManager = new PinotHelixTaskResourceManager(_helixResourceManager.getHelixZkManager());

      LOGGER.info("Starting task manager");
      _taskManager = new PinotTaskManager(_helixTaskResourceManager, _helixResourceManager, _config, _controllerMetrics);
      int taskManagerFrequencyInSeconds = _config.getTaskManagerFrequencyInSeconds();
      if (taskManagerFrequencyInSeconds > 0) {
        LOGGER.info("Starting task manager with running frequency of {} seconds", taskManagerFrequencyInSeconds);
        _taskManager.startScheduler(taskManagerFrequencyInSeconds);
      }

      LOGGER.info("Starting retention manager");
      _retentionManager.start();

      LOGGER.info("Starting validation manager");
      // Helix resource manager must be started in order to create PinotLLCRealtimeSegmentManager
      PinotLLCRealtimeSegmentManager.create(_helixResourceManager, _config, _controllerMetrics);
      ValidationMetrics validationMetrics = new ValidationMetrics(_metricsRegistry);
      _validationManager = new ValidationManager(validationMetrics, _helixResourceManager, _config,
          PinotLLCRealtimeSegmentManager.getInstance());
      _validationManager.start();

      LOGGER.info("Starting realtime segment manager");
      _realtimeSegmentsManager.start(_controllerMetrics);
      PinotLLCRealtimeSegmentManager.getInstance().start();

      LOGGER.info("Starting segment status manager");
      _segmentStatusChecker.start();

      LOGGER.info("Creating rebalance segments factory");
      RebalanceSegmentStrategyFactory.createInstance(_helixResourceManager.getHelixZkManager());

      String accessControlFactoryClass = _config.getAccessControlFactoryClass();
      LOGGER.info("Use class: {} as the access control factory", accessControlFactoryClass);
      final AccessControlFactory accessControlFactory =
          (AccessControlFactory) Class.forName(accessControlFactoryClass).newInstance();

      final MetadataEventNotifierFactory metadataEventNotifierFactory = MetadataEventNotifierFactory.loadFactory(
          _config.subset(METADATA_EVENT_NOTIFIER_PREFIX));

      int jerseyPort = Integer.parseInt(_config.getControllerPort());

      LOGGER.info("Controller download url base: {}", _config.generateVipUrl());
      LOGGER.info("Injecting configuration and resource managers to the API context");
      final MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
      connectionManager.getParams().setConnectionTimeout(_config.getServerAdminRequestTimeoutSeconds());
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
    } catch (final Exception e) {
      LOGGER.error("Caught exception while starting controller", e);
      Utils.rethrowException(e);
      throw new AssertionError("Should not reach this");
    }

    _controllerMetrics.addCallbackGauge(
            "helix.connected",
            new Callable<Long>() {
              @Override
              public Long call() throws Exception {
                return _helixResourceManager.getHelixZkManager().isConnected() ? 1L : 0L;
              }
            });

    _controllerMetrics.addCallbackGauge(
        "helix.leader", new Callable<Long>() {
              @Override
              public Long call() throws Exception {
                return _helixResourceManager.getHelixZkManager().isLeader() ? 1L : 0L;
              }
            });

    _controllerMetrics.addCallbackGauge("dataDir.exists", new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        return new File(_config.getDataDir()).exists() ? 1L : 0L;
      }
    });

    _controllerMetrics.addCallbackGauge("dataDir.fileOpLatencyMs", new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        File dataDir = new File(_config.getDataDir());

        if (dataDir.exists()) {
          try {
            long startTime = System.currentTimeMillis();
            final File testFile = new File(dataDir, _config.getControllerHost());
            FileOutputStream outputStream = new FileOutputStream(testFile, false);
            outputStream.write(Longs.toByteArray(System.currentTimeMillis()));
            outputStream.flush();
            outputStream.close();
            FileUtils.deleteQuietly(testFile);
            long endTime = System.currentTimeMillis();

            return endTime - startTime;
          } catch (IOException e) {
            LOGGER.warn("Caught exception while checking the data directory operation latency", e);
            return DATA_DIRECTORY_EXCEPTION_VALUE;
          }
        } else {
          return DATA_DIRECTORY_MISSING_VALUE;
        }
      }
    });

    ServiceStatus.setServiceStatusCallback(new ServiceStatus.ServiceStatusCallback() {
      private boolean _isStarted = false;
      private String _statusDescription = "Helix ZK Not connected";
      @Override
      public ServiceStatus.Status getServiceStatus() {
        if(_isStarted) {
          // If we've connected to Helix at some point, the instance status depends on being connected to ZK
          if (_helixResourceManager.getHelixZkManager().isConnected()) {
            return ServiceStatus.Status.GOOD;
          } else {
            return ServiceStatus.Status.BAD;
          }
        }

        // Return starting until zk is connected
        if (!_helixResourceManager.getHelixZkManager().isConnected()) {
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

    _helixResourceManager.getHelixZkManager().addPreConnectCallback(new PreConnectCallback() {
      @Override
      public void onPreConnect() {
        _controllerMetrics.addMeteredGlobalValue(ControllerMeter.HELIX_ZOOKEEPER_RECONNECTS, 1L);
      }
    });
    _controllerMetrics.initializeGlobalMeters();
  }

  public void stop() {
    try {
      LOGGER.info("Stopping validation manager");
      _validationManager.stop();

      LOGGER.info("Stopping retention manager");
      _retentionManager.stop();

      LOGGER.info("Stopping Jersey admin API");
      _adminApp.stop();

      LOGGER.info("Stopping realtime segment manager");
      _realtimeSegmentsManager.stop();

      LOGGER.info("Stopping resource manager");
      _helixResourceManager.stop();

      LOGGER.info("Stopping segment status manager");
      _segmentStatusChecker.stop();

      LOGGER.info("Stopping task manager");
      _taskManager.stopScheduler();

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
    conf.setValidationControllerFrequencyInSeconds(3600);
    conf.setStatusCheckerFrequencyInSeconds(5*60);
    conf.setStatusCheckerWaitForPushTimeInSeconds(10*60);
    conf.setTenantIsolationEnabled(true);
    final ControllerStarter starter = new ControllerStarter(conf);

    starter.start();
    return starter;
  }

  public static void main(String[] args) throws InterruptedException {
    startDefault();
  }
}
