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
package org.apache.pinot.minion;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.net.ssl.SSLContext;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.metrics.MinionGauge;
import org.apache.pinot.common.metrics.MinionMeter;
import org.apache.pinot.common.metrics.MinionMetrics;
import org.apache.pinot.common.utils.ClientSSLContextGenerator;
import org.apache.pinot.common.utils.PinotAppConfigs;
import org.apache.pinot.common.utils.ServiceStartableUtils;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.common.utils.TlsUtils;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.core.util.ListenerConfigUtil;
import org.apache.pinot.minion.event.EventObserverFactoryRegistry;
import org.apache.pinot.minion.event.MinionEventObserverFactory;
import org.apache.pinot.minion.event.MinionEventObservers;
import org.apache.pinot.minion.executor.MinionTaskZkMetadataManager;
import org.apache.pinot.minion.executor.PinotTaskExecutorFactory;
import org.apache.pinot.minion.executor.TaskExecutorFactoryRegistry;
import org.apache.pinot.minion.taskfactory.TaskFactoryRegistry;
import org.apache.pinot.spi.crypt.PinotCrypterFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.spi.services.ServiceStartable;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.apache.pinot.sql.parsers.rewriter.QueryRewriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base class for minion starter
 */
public abstract class BaseMinionStarter implements ServiceStartable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseMinionStarter.class);

  private static final String HTTPS_ENABLED = "enabled";

  protected MinionConf _config;
  protected String _hostname;
  protected int _port;
  protected String _instanceId;
  protected HelixManager _helixManager;
  protected TaskExecutorFactoryRegistry _taskExecutorFactoryRegistry;
  protected EventObserverFactoryRegistry _eventObserverFactoryRegistry;
  protected MinionAdminApiApplication _minionAdminApplication;
  protected List<ListenerConfig> _listenerConfigs;
  protected ExecutorService _executorService;

  @Override
  public void init(PinotConfiguration config)
      throws Exception {
    _config = new MinionConf(config.toMap());
    String zkAddress = _config.getZkAddress();
    String helixClusterName = _config.getHelixClusterName();
    ServiceStartableUtils.applyClusterConfig(_config, zkAddress, helixClusterName, ServiceRole.MINION);

    setupHelixSystemProperties();
    _hostname = _config.getHostName();
    _port = _config.getPort();
    _instanceId = _config.getInstanceId();
    if (_instanceId != null) {
      // NOTE: Force all instances to have the same prefix in order to derive the instance type based on the instance id
      Preconditions.checkState(InstanceTypeUtils.isMinion(_instanceId), "Instance id must have prefix '%s', got '%s'",
          CommonConstants.Helix.PREFIX_OF_MINION_INSTANCE, _instanceId);
    } else {
      _instanceId = CommonConstants.Helix.PREFIX_OF_MINION_INSTANCE + _hostname + "_" + _port;
    }
    _listenerConfigs = ListenerConfigUtil.buildMinionConfigs(_config);
    _helixManager = new ZKHelixManager(helixClusterName, _instanceId, InstanceType.PARTICIPANT, zkAddress);
    MinionTaskZkMetadataManager minionTaskZkMetadataManager = new MinionTaskZkMetadataManager(_helixManager);
    _taskExecutorFactoryRegistry = new TaskExecutorFactoryRegistry(minionTaskZkMetadataManager, _config);
    _eventObserverFactoryRegistry = new EventObserverFactoryRegistry(minionTaskZkMetadataManager);
    _executorService =
        Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("async-task-thread-%d").build());
    MinionEventObservers.init(_config, _executorService);
  }

  private void setupHelixSystemProperties() {
    // NOTE: Helix will disconnect the manager and disable the instance if it detects flapping (too frequent disconnect
    // from ZooKeeper). Setting flapping time window to a small value can avoid this from happening. Helix ignores the
    // non-positive value, so set the default value as 1.
    System.setProperty(SystemPropertyKeys.FLAPPING_TIME_WINDOW,
        _config.getProperty(CommonConstants.Helix.CONFIG_OF_MINION_FLAPPING_TIME_WINDOW_MS,
            CommonConstants.Helix.DEFAULT_FLAPPING_TIME_WINDOW_MS));
  }

  /**
   * Registers a task executor factory.
   * <p>This is for pluggable task executor factories.
   */
  public void registerTaskExecutorFactory(PinotTaskExecutorFactory taskExecutorFactory) {
    _taskExecutorFactoryRegistry.registerTaskExecutorFactory(taskExecutorFactory);
  }

  /**
   * Registers an event observer factory.
   * <p>This is for pluggable event observer factories.
   */
  public void registerEventObserverFactory(MinionEventObserverFactory eventObserverFactory) {
    _eventObserverFactoryRegistry.registerEventObserverFactory(eventObserverFactory);
  }

  @Override
  public ServiceRole getServiceRole() {
    return ServiceRole.MINION;
  }

  @Override
  public String getInstanceId() {
    return _instanceId;
  }

  @Override
  public PinotConfiguration getConfig() {
    return _config;
  }

  /**
   * Starts the Pinot Minion instance.
   * <p>Should be called after all classes of task executor get registered.
   */
  @Override
  public void start()
      throws Exception {
    LOGGER.info("Starting Pinot minion: {} (Version: {})", _instanceId, PinotVersion.VERSION);
    LOGGER.info("Minion configs: {}", new PinotAppConfigs(getConfig()).toJSONString());
    Utils.logVersions();
    MinionContext minionContext = MinionContext.getInstance();

    // Initialize data directory
    LOGGER.info("Initializing data directory");
    File dataDir = new File(_config.getProperty(CommonConstants.Helix.Instance.DATA_DIR_KEY,
        CommonConstants.Minion.DEFAULT_INSTANCE_DATA_DIR));
    if (dataDir.exists()) {
      FileUtils.cleanDirectory(dataDir);
    } else {
      FileUtils.forceMkdir(dataDir);
    }
    minionContext.setDataDir(dataDir);

    // Initialize metrics
    LOGGER.info("Initializing metrics");
    // TODO: put all the metrics related configs down to "pinot.server.metrics"
    PinotMetricsRegistry metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry(_config.getMetricsConfig());

    MinionMetrics minionMetrics = new MinionMetrics(_config.getMetricsPrefix(), metricsRegistry);
    minionMetrics.initializeGlobalMeters();
    minionMetrics.setValueOfGlobalGauge(MinionGauge.VERSION, PinotVersion.VERSION_METRIC_NAME, 1);
    MinionMetrics.register(minionMetrics);
    minionContext.setMinionMetrics(minionMetrics);

    // Install default SSL context if necessary (even if not force-enabled everywhere)
    TlsConfig tlsDefaults = TlsUtils.extractTlsConfig(_config, CommonConstants.Minion.MINION_TLS_PREFIX);
    if (StringUtils.isNotBlank(tlsDefaults.getKeyStorePath()) || StringUtils.isNotBlank(
        tlsDefaults.getTrustStorePath())) {
      LOGGER.info("Installing default SSL context for any client requests");
      TlsUtils.installDefaultSSLSocketFactory(tlsDefaults);
    }

    // initialize authentication
    minionContext.setTaskAuthProvider(
        AuthProviderUtils.extractAuthProvider(_config, CommonConstants.Minion.CONFIG_TASK_AUTH_NAMESPACE));

    // Start all components
    LOGGER.info("Initializing PinotFSFactory");
    PinotConfiguration pinotFSConfig = _config.subset(CommonConstants.Minion.PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY);
    if (pinotFSConfig.isEmpty()) {
      pinotFSConfig = _config.subset(CommonConstants.Minion.DEPRECATED_PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY);
    }
    PinotFSFactory.init(pinotFSConfig);

    LOGGER.info("Initializing QueryRewriterFactory");
    QueryRewriterFactory.init(_config.getProperty(CommonConstants.Minion.CONFIG_OF_MINION_QUERY_REWRITER_CLASS_NAMES));

    LOGGER.info("Initializing segment fetchers for all protocols");
    PinotConfiguration segmentFetcherFactoryConfig =
        _config.subset(CommonConstants.Minion.PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY);
    if (segmentFetcherFactoryConfig.isEmpty()) {
      segmentFetcherFactoryConfig =
          _config.subset(CommonConstants.Minion.DEPRECATED_PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY);
    }
    SegmentFetcherFactory.init(segmentFetcherFactoryConfig);

    LOGGER.info("Initializing pinot crypter");
    PinotConfiguration pinotCrypterConfig = _config.subset(CommonConstants.Minion.PREFIX_OF_CONFIG_OF_PINOT_CRYPTER);
    if (pinotCrypterConfig.isEmpty()) {
      pinotCrypterConfig = _config.subset(CommonConstants.Minion.DEPRECATED_PREFIX_OF_CONFIG_OF_PINOT_CRYPTER);
    }
    PinotCrypterFactory.init(pinotCrypterConfig);

    // Need to do this before we start receiving state transitions.
    LOGGER.info("Initializing ssl context for segment uploader");
    PinotConfiguration segmentUploaderConfig =
        _config.subset(CommonConstants.Minion.PREFIX_OF_CONFIG_OF_SEGMENT_UPLOADER);
    if (segmentUploaderConfig.isEmpty()) {
      segmentUploaderConfig = _config.subset(CommonConstants.Minion.DEPRECATED_PREFIX_OF_CONFIG_OF_SEGMENT_UPLOADER);
    }
    PinotConfiguration httpsConfig = segmentUploaderConfig.subset(CommonConstants.HTTPS_PROTOCOL);
    if (httpsConfig.getProperty(HTTPS_ENABLED, false)) {
      SSLContext sslContext =
          new ClientSSLContextGenerator(httpsConfig.subset(CommonConstants.PREFIX_OF_SSL_SUBSET)).generate();
      minionContext.setSSLContext(sslContext);
    }

    // Join the Helix cluster
    LOGGER.info("Joining the Helix cluster");
    _helixManager.getStateMachineEngine().registerStateModelFactory("Task", new TaskStateModelFactory(_helixManager,
        new TaskFactoryRegistry(_taskExecutorFactoryRegistry, _eventObserverFactoryRegistry).getTaskFactoryRegistry()));
    _helixManager.connect();
    updateInstanceConfigIfNeeded();
    minionMetrics.setOrUpdateGauge(CommonConstants.Helix.INSTANCE_CONNECTED_METRIC_NAME,
            () -> _helixManager.isConnected() ? 1L : 0L);
    minionContext.setHelixPropertyStore(_helixManager.getHelixPropertyStore());
    minionContext.setHelixManager(_helixManager);
    LOGGER.info("Starting minion admin application on: {}", ListenerConfigUtil.toString(_listenerConfigs));
    _minionAdminApplication = new MinionAdminApiApplication(_instanceId, _config);
    _minionAdminApplication.start(_listenerConfigs);

    // Initialize health check callback
    LOGGER.info("Initializing health check callback");
    ServiceStatus.setServiceStatusCallback(_instanceId, new ServiceStatus.ServiceStatusCallback() {
      private volatile boolean _isStarted = false;
      private volatile String _statusDescription = "Helix ZK Not connected as " + _helixManager.getInstanceType();

      @Override
      public ServiceStatus.Status getServiceStatus() {
        // TODO: add health check here
        minionMetrics.addMeteredGlobalValue(MinionMeter.HEALTH_CHECK_GOOD_CALLS, 1L);
        if (_isStarted) {
          if (_helixManager.isConnected()) {
            return ServiceStatus.Status.GOOD;
          } else {
            return ServiceStatus.Status.BAD;
          }
        }

        if (!_helixManager.isConnected()) {
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

    LOGGER.info("Pinot minion started");
  }

  private void updateInstanceConfigIfNeeded() {
    InstanceConfig instanceConfig = HelixHelper.getInstanceConfig(_helixManager, _instanceId);
    boolean updated = HelixHelper.updateHostnamePort(instanceConfig, _hostname, _port);
    updated |= HelixHelper.addDefaultTags(instanceConfig,
        () -> Collections.singletonList(CommonConstants.Helix.UNTAGGED_MINION_INSTANCE));
    updated |= HelixHelper.removeDisabledPartitions(instanceConfig);
    if (updated) {
      HelixHelper.updateInstanceConfig(_helixManager, instanceConfig);
    }
  }

  /**
   * Stops the Pinot Minion instance.
   */
  @Override
  public void stop() {
    try {
      LOGGER.info("Closing PinotFS classes");
      PinotFSFactory.shutdown();
    } catch (IOException e) {
      LOGGER.warn("Caught exception closing PinotFS classes", e);
    }
    LOGGER.info("Shutting down admin application");
    _minionAdminApplication.stop();

    LOGGER.info("Stopping Pinot minion: " + _instanceId);
    _helixManager.disconnect();
    LOGGER.info("Deregistering service status handler");
    ServiceStatus.removeServiceStatusCallback(_instanceId);
    LOGGER.info("Shutting down executor service");
    _executorService.shutdownNow();
    LOGGER.info("Clean up Minion data directory");
    try {
      FileUtils.cleanDirectory(MinionContext.getInstance().getDataDir());
    } catch (IOException e) {
      LOGGER.warn("Failed to clean up Minion data directory: {}", MinionContext.getInstance().getDataDir(), e);
    }
    LOGGER.info("Pinot minion stopped");
  }
}
