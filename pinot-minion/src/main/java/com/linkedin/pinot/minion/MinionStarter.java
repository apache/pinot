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
package com.linkedin.pinot.minion;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.metrics.MetricsHelper;
import com.linkedin.pinot.common.segment.fetcher.SegmentFetcherFactory;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.common.utils.ServiceStatus;
import com.linkedin.pinot.minion.executor.PinotTaskExecutor;
import com.linkedin.pinot.minion.executor.TaskExecutorRegistry;
import com.linkedin.pinot.minion.metrics.MinionMeter;
import com.linkedin.pinot.minion.metrics.MinionMetrics;
import com.linkedin.pinot.minion.taskfactory.TaskFactoryRegistry;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import javax.annotation.Nonnull;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.task.TaskStateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class <code>MinionStarter</code> provides methods to start and stop the Pinot Minion.
 * <p>Pinot Minion will automatically join the given Helix cluster as a participant.
 */
public class MinionStarter {
  private static final Logger LOGGER = LoggerFactory.getLogger(MinionStarter.class);

  private final String _helixClusterName;
  private final Configuration _config;
  private final String _instanceId;
  private final HelixManager _helixManager;
  private final TaskExecutorRegistry _taskExecutorRegistry;

  private HelixAdmin _helixAdmin;

  public MinionStarter(String zkAddress, String helixClusterName, Configuration config) throws Exception {
    _helixClusterName = helixClusterName;
    _config = config;
    _instanceId = config.getString(CommonConstants.Helix.Instance.INSTANCE_ID_KEY,
        CommonConstants.Minion.INSTANCE_PREFIX + NetUtil.getHostAddress() + "_"
            + CommonConstants.Minion.DEFAULT_HELIX_PORT);
    _helixManager = new ZKHelixManager(_helixClusterName, _instanceId, InstanceType.PARTICIPANT, zkAddress);
    _taskExecutorRegistry = new TaskExecutorRegistry();
  }

  /**
   * Register a class of task executor.
   * <p>This is for pluggable task executors.
   *
   * @param taskType Task type
   * @param taskExecutorClass Class of task executor to be registered
   */
  public void registerTaskExecutorClass(@Nonnull String taskType,
      @Nonnull Class<? extends PinotTaskExecutor> taskExecutorClass) {
    _taskExecutorRegistry.registerTaskExecutorClass(taskType, taskExecutorClass);
  }

  /**
   * Start the Pinot Minion instance.
   * <p>Should be called after all classes of task executor get registered.
   */
  public void start() throws Exception {
    LOGGER.info("Starting Pinot minion: {}", _instanceId);
    Utils.logVersions();
    MinionContext minionContext = MinionContext.getInstance();

    // Initialize data directory
    LOGGER.info("Initializing data directory");
    File dataDir = new File(_config.getString(CommonConstants.Helix.Instance.DATA_DIR_KEY,
        CommonConstants.Minion.DEFAULT_INSTANCE_DATA_DIR));
    if (!dataDir.exists()) {
      Preconditions.checkState(dataDir.mkdirs());
    }
    minionContext.setDataDir(dataDir);

    // Initialize metrics
    LOGGER.info("Initializing metrics");
    MetricsHelper.initializeMetrics(_config);
    MetricsRegistry metricsRegistry = new MetricsRegistry();
    MetricsHelper.registerMetricsRegistry(metricsRegistry);
    final MinionMetrics minionMetrics = new MinionMetrics(metricsRegistry);
    minionMetrics.initializeGlobalMeters();
    minionContext.setMinionMetrics(minionMetrics);

    // TODO: set the correct minion version
    minionContext.setMinionVersion("1.0");

    LOGGER.info("initializing segment fetchers for all protocols");
    SegmentFetcherFactory.getInstance()
        .init(_config.subset(CommonConstants.Minion.PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY));

    // Join the Helix cluster
    LOGGER.info("Joining the Helix cluster");
    _helixManager.getStateMachineEngine()
        .registerStateModelFactory("Task", new TaskStateModelFactory(_helixManager,
            new TaskFactoryRegistry(_taskExecutorRegistry).getTaskFactoryRegistry()));
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
      LOGGER.info("Adding default Helix tag: {} to Pinot minion", CommonConstants.Minion.UNTAGGED_INSTANCE);
      _helixAdmin.addInstanceTag(_helixClusterName, _instanceId, CommonConstants.Minion.UNTAGGED_INSTANCE);
    }
  }
}
