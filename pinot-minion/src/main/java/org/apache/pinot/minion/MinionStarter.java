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
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.metrics.MetricsHelper;
import org.apache.pinot.common.segment.fetcher.SegmentFetcherFactory;
import org.apache.pinot.common.utils.ClientSSLContextGenerator;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.NetUtil;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.core.crypt.PinotCrypterFactory;
import org.apache.pinot.filesystem.PinotFSFactory;
import org.apache.pinot.minion.events.EventObserverFactoryRegistry;
import org.apache.pinot.minion.events.MinionEventObserverFactory;
import org.apache.pinot.minion.executor.PinotTaskExecutorFactory;
import org.apache.pinot.minion.executor.TaskExecutorFactoryRegistry;
import org.apache.pinot.minion.metrics.MinionMeter;
import org.apache.pinot.minion.metrics.MinionMetrics;
import org.apache.pinot.minion.taskfactory.TaskFactoryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class <code>MinionStarter</code> provides methods to start and stop the Pinot Minion.
 * <p>Pinot Minion will automatically join the given Helix cluster as a participant.
 */
public class MinionStarter {
  private static final Logger LOGGER = LoggerFactory.getLogger(MinionStarter.class);

  private static final String HTTPS_PROTOCOL = "https";
  private static final String HTTPS_ENABLED = "enabled";

  private final String _helixClusterName;
  private final Configuration _config;
  private final String _instanceId;
  private final HelixManager _helixManager;
  private final TaskExecutorFactoryRegistry _taskExecutorFactoryRegistry;
  private final EventObserverFactoryRegistry _eventObserverFactoryRegistry;

  private HelixAdmin _helixAdmin;

  public MinionStarter(String zkAddress, String helixClusterName, Configuration config)
      throws Exception {
    _helixClusterName = helixClusterName;
    _config = config;
    _instanceId = config.getString(CommonConstants.Helix.Instance.INSTANCE_ID_KEY,
        CommonConstants.Helix.PREFIX_OF_MINION_INSTANCE + NetUtil.getHostAddress() + "_"
            + CommonConstants.Minion.DEFAULT_HELIX_PORT);
    setupHelixSystemProperties();
    _helixManager = new ZKHelixManager(_helixClusterName, _instanceId, InstanceType.PARTICIPANT, zkAddress);
    _taskExecutorFactoryRegistry = new TaskExecutorFactoryRegistry();
    _eventObserverFactoryRegistry = new EventObserverFactoryRegistry();
  }

  private void setupHelixSystemProperties() {
    // NOTE: Helix will disconnect the manager and disable the instance if it detects flapping (too frequent disconnect
    // from ZooKeeper). Setting flapping time window to a small value can avoid this from happening. Helix ignores the
    // non-positive value, so set the default value as 1.
    System.setProperty(SystemPropertyKeys.FLAPPING_TIME_WINDOW, _config
        .getString(CommonConstants.Helix.CONFIG_OF_MINION_FLAPPING_TIME_WINDOW_MS,
            CommonConstants.Helix.DEFAULT_FLAPPING_TIME_WINDOW_MS));
  }

  /**
   * Registers a task executor factory.
   * <p>This is for pluggable task executor factories.
   *
   * @param taskType Task type
   * @param taskExecutorFactory Task executor factory associated with the task type
   */
  public void registerTaskExecutorFactory(@Nonnull String taskType,
      @Nonnull PinotTaskExecutorFactory taskExecutorFactory) {
    _taskExecutorFactoryRegistry.registerTaskExecutorFactory(taskType, taskExecutorFactory);
  }

  /**
   * Registers an event observer factory.
   * <p>This is for pluggable event observer factories.
   *
   * @param taskType Task type
   * @param eventObserverFactory Event observer factory associated with the task type
   */
  public void registerEventObserverFactory(@Nonnull String taskType,
      @Nonnull MinionEventObserverFactory eventObserverFactory) {
    _eventObserverFactoryRegistry.registerEventObserverFactory(taskType, eventObserverFactory);
  }

  /**
   * Start the Pinot Minion instance.
   * <p>Should be called after all classes of task executor get registered.
   */
  public void start()
      throws Exception {
    LOGGER.info("Starting Pinot minion: {}", _instanceId);
    Utils.logVersions();
    MinionContext minionContext = MinionContext.getInstance();

    // Initialize data directory
    LOGGER.info("Initializing data directory");
    File dataDir = new File(_config
        .getString(CommonConstants.Helix.Instance.DATA_DIR_KEY, CommonConstants.Minion.DEFAULT_INSTANCE_DATA_DIR));
    if (!dataDir.exists()) {
      Preconditions.checkState(dataDir.mkdirs());
    }
    minionContext.setDataDir(dataDir);

    // Initialize metrics
    LOGGER.info("Initializing metrics");
    MetricsHelper.initializeMetrics(_config);
    MetricsRegistry metricsRegistry = new MetricsRegistry();
    MetricsHelper.registerMetricsRegistry(metricsRegistry);
    final MinionMetrics minionMetrics = new MinionMetrics(_config
        .getString(CommonConstants.Minion.CONFIG_OF_METRICS_PREFIX_KEY,
            CommonConstants.Minion.CONFIG_OF_METRICS_PREFIX), metricsRegistry);
    minionMetrics.initializeGlobalMeters();
    minionContext.setMinionMetrics(minionMetrics);

    // TODO: set the correct minion version
    minionContext.setMinionVersion("1.0");

    // Start all components
    LOGGER.info("Initializing PinotFSFactory");
    Configuration pinotFSConfig = _config.subset(CommonConstants.Minion.PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY);
    PinotFSFactory.init(pinotFSConfig);

    LOGGER.info("Initializing segment fetchers for all protocols");
    Configuration segmentFetcherFactoryConfig =
        _config.subset(CommonConstants.Minion.PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY);
    SegmentFetcherFactory.getInstance().init(segmentFetcherFactoryConfig);

    LOGGER.info("Initializing pinot crypter");
    Configuration pinotCrypterConfig = _config.subset(CommonConstants.Minion.PREFIX_OF_CONFIG_OF_PINOT_CRYPTER);
    PinotCrypterFactory.init(pinotCrypterConfig);

    // Need to do this before we start receiving state transitions.
    LOGGER.info("Initializing ssl context for segment uploader");
    Configuration httpsConfig =
        _config.subset(CommonConstants.Minion.PREFIX_OF_CONFIG_OF_SEGMENT_UPLOADER).subset(HTTPS_PROTOCOL);
    if (httpsConfig.getBoolean(HTTPS_ENABLED, false)) {
      SSLContext sslContext =
          new ClientSSLContextGenerator(httpsConfig.subset(CommonConstants.PREFIX_OF_SSL_SUBSET)).generate();
      minionContext.setSSLContext(sslContext);
    }

    // Join the Helix cluster
    LOGGER.info("Joining the Helix cluster");
    _helixManager.getStateMachineEngine().registerStateModelFactory("Task", new TaskStateModelFactory(_helixManager,
        new TaskFactoryRegistry(_taskExecutorFactoryRegistry, _eventObserverFactoryRegistry).getTaskFactoryRegistry()));
    _helixManager.connect();
    _helixAdmin = _helixManager.getClusterManagmentTool();
    addInstanceTagIfNeeded();

    // Initialize health check callback
    LOGGER.info("Initializing health check callback");
    ServiceStatus.setServiceStatusCallback(new ServiceStatus.ServiceStatusCallback() {
      @Override
      public ServiceStatus.Status getServiceStatus() {
        // TODO: add health check here
        minionMetrics.addMeteredGlobalValue(MinionMeter.HEALTH_CHECK_GOOD_CALLS, 1L);
        return ServiceStatus.Status.GOOD;
      }

      @Override
      public String getStatusDescription() {
        return ServiceStatus.STATUS_DESCRIPTION_NONE;
      }
    });

    LOGGER.info("Pinot minion started");
  }

  /**
   * Stop the Pinot Minion instance.
   */
  public void stop() {
    try {
      LOGGER.info("Closing PinotFS classes");
      PinotFSFactory.shutdown();
    } catch (IOException e) {
      LOGGER.warn("Caught exception closing PinotFS classes", e);
    }
    LOGGER.info("Stopping Pinot minion: " + _instanceId);
    _helixManager.disconnect();
    LOGGER.info("Pinot minion stopped");
  }

  /**
   * Tag Pinot Minion instance if needed.
   */
  private void addInstanceTagIfNeeded() {
    InstanceConfig instanceConfig = _helixAdmin.getInstanceConfig(_helixClusterName, _instanceId);
    if (instanceConfig.getTags().isEmpty()) {
      LOGGER.info("Adding default Helix tag: {} to Pinot minion", CommonConstants.Helix.UNTAGGED_MINION_INSTANCE);
      _helixAdmin.addInstanceTag(_helixClusterName, _instanceId, CommonConstants.Helix.UNTAGGED_MINION_INSTANCE);
    }
  }
}
