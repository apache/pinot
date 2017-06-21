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
import com.linkedin.pinot.common.utils.ServiceStatus;
import com.linkedin.pinot.controller.api.ControllerRestApplication;
import com.linkedin.pinot.controller.helix.SegmentStatusChecker;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.PinotTaskManager;
import com.linkedin.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import com.linkedin.pinot.controller.helix.core.realtime.PinotRealtimeSegmentManager;
import com.linkedin.pinot.controller.helix.core.retention.RetentionManager;
import com.linkedin.pinot.controller.validation.ValidationManager;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.io.FileUtils;
import org.apache.helix.PreConnectCallback;
import org.apache.helix.task.TaskDriver;
import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.data.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControllerStarter {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerStarter.class);

  private static final String METRICS_REGISTRY_NAME = "pinot.controller.metrics";
  private static final Long DATA_DIRECTORY_MISSING_VALUE = 1000000L;
  private static final Long DATA_DIRECTORY_EXCEPTION_VALUE = 1100000L;

  private final ControllerConf config;
  private final Component component;
  private final Application controllerRestApp;
  private final PinotHelixResourceManager helixResourceManager;
  private final RetentionManager retentionManager;
  private final MetricsRegistry _metricsRegistry;
  private final PinotRealtimeSegmentManager realtimeSegmentsManager;
  private final SegmentStatusChecker segmentStatusChecker;
  private final ExecutorService executorService;

  // Can only be constructed after resource manager getting started
  private ValidationManager _validationManager;
  private PinotHelixTaskResourceManager _helixTaskResourceManager;
  private PinotTaskManager _taskManager;

  public ControllerStarter(ControllerConf conf) {
    config = conf;
    component = new Component();
    controllerRestApp = new ControllerRestApplication(config.getQueryConsole());
    helixResourceManager = new PinotHelixResourceManager(config);
    retentionManager = new RetentionManager(helixResourceManager, config.getRetentionControllerFrequencyInSeconds(),
        config.getDeletedSegmentsRetentionInDays());
    _metricsRegistry = new MetricsRegistry();
    realtimeSegmentsManager = new PinotRealtimeSegmentManager(helixResourceManager);
    segmentStatusChecker = new SegmentStatusChecker(helixResourceManager, config);
    executorService = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat("restlet-multiget-thread-%d").build());
  }

  public PinotHelixResourceManager getHelixResourceManager() {
    return helixResourceManager;
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
    MetricsHelper.initializeMetrics(config.subset(METRICS_REGISTRY_NAME));
    MetricsHelper.registerMetricsRegistry(_metricsRegistry);
    final ControllerMetrics controllerMetrics = new ControllerMetrics(_metricsRegistry);

    // Start all components
    try {
      LOGGER.info("Starting Pinot Helix resource manager and connecting to Zookeeper");
      helixResourceManager.start();

      LOGGER.info("Starting task resource manager");
      // Helix resource manager must be started in order to get TaskDriver
      TaskDriver taskDriver = new TaskDriver(helixResourceManager.getHelixZkManager());
      _helixTaskResourceManager = new PinotHelixTaskResourceManager(taskDriver);

      LOGGER.info("Starting task manager");
      _taskManager = new PinotTaskManager(taskDriver, helixResourceManager, _helixTaskResourceManager);
      _taskManager.ensureTaskQueuesExist();
      int taskManagerFrequencyInSeconds = config.getTaskManagerFrequencyInSeconds();
      if (taskManagerFrequencyInSeconds > 0) {
        LOGGER.info("Starting task manager with running frequency of {} seconds", taskManagerFrequencyInSeconds);
        _taskManager.startScheduler(taskManagerFrequencyInSeconds);
      }

      LOGGER.info("Starting retention manager");
      retentionManager.start();

      LOGGER.info("Starting validation manager");
      // Helix resource manager must be started in order to create PinotLLCRealtimeSegmentManager
      PinotLLCRealtimeSegmentManager.create(helixResourceManager, config, controllerMetrics);
      ValidationMetrics validationMetrics = new ValidationMetrics(_metricsRegistry);
      _validationManager = new ValidationManager(validationMetrics, helixResourceManager, config,
          PinotLLCRealtimeSegmentManager.getInstance());
      _validationManager.start();

      LOGGER.info("Starting realtime segment manager");
      realtimeSegmentsManager.start(controllerMetrics);
      PinotLLCRealtimeSegmentManager.getInstance().start();

      LOGGER.info("Starting segment status manager");
      segmentStatusChecker.start(controllerMetrics);

      LOGGER.info("Starting Pinot REST API component");
      component.getServers().add(Protocol.HTTP, Integer.parseInt(config.getControllerPort()));
      component.getClients().add(Protocol.FILE);
      component.getClients().add(Protocol.JAR);
      Context applicationContext = component.getContext().createChildContext();
      LOGGER.info("Controller download url base: {}", config.generateVipUrl());
      LOGGER.info("Injecting configuration and resource managers to the API context");
      ConcurrentMap<String, Object> attributes = applicationContext.getAttributes();
      attributes.put(ControllerConf.class.toString(), config);
      attributes.put(PinotHelixResourceManager.class.toString(), helixResourceManager);
      attributes.put(PinotHelixTaskResourceManager.class.toString(), _helixTaskResourceManager);
      attributes.put(PinotTaskManager.class.toString(), _taskManager);
      MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
      connectionManager.getParams().setConnectionTimeout(config.getServerAdminRequestTimeoutSeconds());
      attributes.put(HttpConnectionManager.class.toString(), connectionManager);
      attributes.put(Executor.class.toString(), executorService);
      controllerRestApp.setContext(applicationContext);
      component.getDefaultHost().attach(controllerRestApp);
      component.start();

      LOGGER.info("Pinot controller ready and listening on port {} for API requests", config.getControllerPort());
      LOGGER.info("Controller services available at http://{}:{}/", config.getControllerHost(),
          config.getControllerPort());
    } catch (final Exception e) {
      LOGGER.error("Caught exception while starting controller", e);
      Utils.rethrowException(e);
      throw new AssertionError("Should not reach this");
    }

    controllerMetrics.addCallbackGauge(
            "helix.connected",
            new Callable<Long>() {
              @Override
              public Long call() throws Exception {
                return helixResourceManager.getHelixZkManager().isConnected() ? 1L : 0L;
              }
            });

    controllerMetrics.addCallbackGauge(
        "helix.leader", new Callable<Long>() {
              @Override
              public Long call() throws Exception {
                return helixResourceManager.getHelixZkManager().isLeader() ? 1L : 0L;
              }
            });

    controllerMetrics.addCallbackGauge("dataDir.exists", new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        return new File(config.getDataDir()).exists() ? 1L : 0L;
      }
    });

    controllerMetrics.addCallbackGauge("dataDir.fileOpLatencyMs", new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        File dataDir = new File(config.getDataDir());

        if (dataDir.exists()) {
          try {
            long startTime = System.currentTimeMillis();
            final File testFile = new File(dataDir, config.getControllerHost());
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
      @Override
      public ServiceStatus.Status getServiceStatus() {
        if(_isStarted) {
          // If we've connected to Helix at some point, the instance status depends on being connected to ZK
          if (helixResourceManager.getHelixZkManager().isConnected()) {
            return ServiceStatus.Status.GOOD;
          } else {
            return ServiceStatus.Status.BAD;
          }
        }

        // Return starting until zk is connected
        if (!helixResourceManager.getHelixZkManager().isConnected()) {
          return ServiceStatus.Status.STARTING;
        } else {
          _isStarted = true;
          return ServiceStatus.Status.GOOD;
        }
      }
    });

    helixResourceManager.getHelixZkManager().addPreConnectCallback(new PreConnectCallback() {
      @Override
      public void onPreConnect() {
        controllerMetrics.addMeteredGlobalValue(ControllerMeter.HELIX_ZOOKEEPER_RECONNECTS, 1L);
      }
    });
    controllerMetrics.initializeGlobalMeters();
    ControllerRestApplication.setControllerMetrics(controllerMetrics);
  }

  public void stop() {
    try {
      LOGGER.info("Stopping validation manager");
      _validationManager.stop();

      LOGGER.info("Stopping retention manager");
      retentionManager.stop();

      LOGGER.info("Stopping API component");
      component.stop();

      LOGGER.info("Stopping realtime segment manager");
      realtimeSegmentsManager.stop();

      LOGGER.info("Stopping resource manager");
      helixResourceManager.stop();

      LOGGER.info("Stopping segment status manager");
      segmentStatusChecker.stop();

      LOGGER.info("Stopping task manager");
      _taskManager.stopScheduler();

      executorService.shutdownNow();
    } catch (final Exception e) {
      LOGGER.error("Caught exception while shutting down", e);
    }
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
    conf.setSplitCommit(false);
    final ControllerStarter starter = new ControllerStarter(conf);

    starter.start();
    return starter;
  }

  public static void main(String[] args) throws InterruptedException {
    startDefault();
  }
}
