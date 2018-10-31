/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.server.starter.helix;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.config.TagNameUtils;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metrics.ServerMeter;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.common.utils.ServiceStatus;
import com.linkedin.pinot.common.utils.ServiceStatus.Status;
import com.linkedin.pinot.core.data.manager.InstanceDataManager;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.filesystem.PinotFSFactory;
import com.linkedin.pinot.server.conf.ServerConf;
import com.linkedin.pinot.server.realtime.ControllerLeaderLocator;
import com.linkedin.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import com.linkedin.pinot.server.starter.ServerInstance;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Single server helix starter. Will start automatically with an untagged box.
 * Will auto join current cluster as a participant.
 *
 *
 *
 */
public class HelixServerStarter {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixServerStarter.class);

  private final String _helixClusterName;
  private final Configuration _helixServerConfig;
  private final String _instanceId;
  private final long _maxQueryTimeMs;
  private final long _maxShutdownWaitTimeMs;
  private final long _checkIntervalTimeMs;
  private final HelixManager _helixManager;
  private final HelixAdmin _helixAdmin;
  private final ServerInstance _serverInstance;
  private final AdminApiApplication _adminApiApplication;
  private final String _zkServers;

  public HelixServerStarter(String helixClusterName, String zkServer, Configuration helixServerConfig)
      throws Exception {
    LOGGER.info("Starting Pinot server");
    _helixClusterName = helixClusterName;

    // Make a clone so that changes to the config won't propagate to the caller
    _helixServerConfig = ConfigurationUtils.cloneConfiguration(helixServerConfig);

    if (_helixServerConfig.containsKey(CommonConstants.Server.CONFIG_OF_INSTANCE_ID)) {
      _instanceId = _helixServerConfig.getString(CommonConstants.Server.CONFIG_OF_INSTANCE_ID);
    } else {
      String host =
          _helixServerConfig.getString(CommonConstants.Helix.KEY_OF_SERVER_NETTY_HOST, NetUtil.getHostAddress());
      int port = _helixServerConfig.getInt(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT,
          CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT);
      _instanceId = CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE + host + "_" + port;
      _helixServerConfig.addProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_ID, _instanceId);
    }

    _maxQueryTimeMs = _helixServerConfig.getLong(CommonConstants.Server.CONFIG_OF_QUERY_EXECUTOR_TIMEOUT,
        CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    _maxShutdownWaitTimeMs =
        _helixServerConfig.getLong(CommonConstants.Server.CONFIG_OF_INSTANCE_MAX_SHUTDOWN_WAIT_TIME,
            CommonConstants.Server.DEFAULT_MAX_SHUTDOWN_WAIT_TIME_MS);
    long checkIntervalTimeMs = _helixServerConfig.getLong(CommonConstants.Server.CONFIG_OF_INSTANCE_CHECK_INTERVAL_TIME,
        CommonConstants.Server.DEFAULT_CHECK_INTERVAL_TIME_MS);
    if (checkIntervalTimeMs <= 0L) {
      _checkIntervalTimeMs = CommonConstants.Server.DEFAULT_CHECK_INTERVAL_TIME_MS;
      LOGGER.warn("Cannot set check interval time to non-positive value. Using the default setting: {}ms",
          _checkIntervalTimeMs);
    } else {
      _checkIntervalTimeMs = checkIntervalTimeMs;
    }

    LOGGER.info("Connecting Helix components");
    setupHelixSystemProperties(_helixServerConfig);
    // Replace all white-spaces from list of zkServers.
    _zkServers = zkServer.replaceAll("\\s+", "");
    _helixManager =
        HelixManagerFactory.getZKHelixManager(helixClusterName, _instanceId, InstanceType.PARTICIPANT, _zkServers);
    final StateMachineEngine stateMachineEngine = _helixManager.getStateMachineEngine();
    _helixManager.connect();
    _helixAdmin = _helixManager.getClusterManagmentTool();
    addInstanceTagIfNeeded(helixClusterName, _instanceId);
    ZkHelixPropertyStore<ZNRecord> propertyStore = _helixManager.getHelixPropertyStore();

    LOGGER.info("Starting server instance");
    Utils.logVersions();
    ServerConf serverInstanceConfig = DefaultHelixStarterServerConfig.getDefaultHelixServerConfig(_helixServerConfig);
    // Need to do this before we start receiving state transitions.
    ServerSegmentCompletionProtocolHandler.init(
        _helixServerConfig.subset(CommonConstants.Server.PREFIX_OF_CONFIG_OF_SEGMENT_UPLOADER));
    _serverInstance = new ServerInstance();
    _serverInstance.init(serverInstanceConfig, propertyStore);
    _serverInstance.start();

    // Register state model factory
    SegmentFetcherAndLoader fetcherAndLoader =
        new SegmentFetcherAndLoader(_helixServerConfig, _serverInstance.getInstanceDataManager(), propertyStore);
    StateModelFactory<?> stateModelFactory =
        new SegmentOnlineOfflineStateModelFactory(_instanceId, _serverInstance.getInstanceDataManager(),
            fetcherAndLoader, propertyStore);
    stateMachineEngine.registerStateModelFactory(SegmentOnlineOfflineStateModelFactory.getStateModelName(),
        stateModelFactory);

    // Start restlet server for admin API endpoint
    int adminApiPort = _helixServerConfig.getInt(CommonConstants.Server.CONFIG_OF_ADMIN_API_PORT,
        CommonConstants.Server.DEFAULT_ADMIN_API_PORT);
    _adminApiApplication = new AdminApiApplication(_serverInstance);
    _adminApiApplication.start(adminApiPort);
    setAdminApiPort(adminApiPort);

    // Register message handler factory
    SegmentMessageHandlerFactory messageHandlerFactory =
        new SegmentMessageHandlerFactory(fetcherAndLoader, _serverInstance.getInstanceDataManager());
    _helixManager.getMessagingService()
        .registerMessageHandlerFactory(Message.MessageType.USER_DEFINE_MSG.toString(), messageHandlerFactory);

    final ServerMetrics serverMetrics = _serverInstance.getServerMetrics();
    serverMetrics.addCallbackGauge("helix.connected", () -> _helixManager.isConnected() ? 1L : 0L);
    _helixManager.addPreConnectCallback(
        () -> serverMetrics.addMeteredGlobalValue(ServerMeter.HELIX_ZOOKEEPER_RECONNECTS, 1L));

    // Register the service status handler
    ServiceStatus.setServiceStatusCallback(new ServiceStatus.MultipleCallbackServiceStatusCallback(ImmutableList.of(
        new ServiceStatus.IdealStateAndCurrentStateMatchServiceStatusCallback(_helixManager, _helixClusterName,
            _instanceId),
        new ServiceStatus.IdealStateAndExternalViewMatchServiceStatusCallback(_helixManager, _helixClusterName,
            _instanceId))));

    ControllerLeaderLocator.create(_helixManager);

    waitForAllSegmentsLoaded();
    setShuttingDownStatus(false);
    LOGGER.info("Pinot server ready");

    // Create metrics for mmap stuff
    serverMetrics.addCallbackGauge("memory.directBufferCount", PinotDataBuffer::getDirectBufferCount);
    serverMetrics.addCallbackGauge("memory.directBufferUsage", PinotDataBuffer::getDirectBufferUsage);
    serverMetrics.addCallbackGauge("memory.mmapBufferCount", PinotDataBuffer::getMmapBufferCount);
    serverMetrics.addCallbackGauge("memory.mmapBufferUsage", PinotDataBuffer::getMmapBufferUsage);
    serverMetrics.addCallbackGauge("memory.allocationFailureCount", PinotDataBuffer::getAllocationFailureCount);
  }

  private void waitForAllSegmentsLoaded() {
    if (_helixServerConfig.getBoolean(CommonConstants.Server.CONFIG_OF_STARTER_ENABLE_SEGMENTS_LOADING_CHECK, CommonConstants.Server.DEFAULT_STARTER_ENABLE_SEGMENTS_LOADING_CHECK)) {
      long startTime = System.currentTimeMillis();
      int serverStarterTimeout = _helixServerConfig.getInt(CommonConstants.Server.CONFIG_OF_STARTER_TIMEOUT_IN_SECONDS, CommonConstants.Server.DEFAULT_STARTER_TIMEOUT_IN_SECONDS);
      long endTime = startTime + TimeUnit.SECONDS.toMillis(serverStarterTimeout);
      boolean allSegmentsLoaded = false;
      while (System.currentTimeMillis() < endTime) {
        long timeToSleep = Math.min(TimeUnit.MILLISECONDS.toSeconds(endTime - System.currentTimeMillis()), 10 /* Sleep 10 seconds as default*/);
        if (ServiceStatus.getServiceStatus() == Status.GOOD) {
          LOGGER.info("All the segments are fully loaded into Pinot server, time taken: {} seconds", TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime));
          allSegmentsLoaded = true;
          break;
        }
        try {
          int numSegmentsLoaded = getNumSegmentLoaded();
          int numSegmentsToLoad = getNumSegmentsToLoad();
          LOGGER.warn("Waiting for all segments to be loaded, current progress: [ {} / {} ], sleep {} seconds...", numSegmentsLoaded, numSegmentsToLoad, timeToSleep);
          Thread.sleep(TimeUnit.SECONDS.toMillis(timeToSleep));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        } catch (Exception e) {
          LOGGER.warn("Caught exception during waiting for segments loading...", e);
        }
      }
      if (!allSegmentsLoaded) {
        LOGGER.info("Segments are not fully loaded within {} seconds...", serverStarterTimeout);
        logSegmentsLoadingInfo();
      }
    }
  }

  private int getNumSegmentLoaded() {
    InstanceDataManager instanceDataManager = _serverInstance.getInstanceDataManager();
    if (instanceDataManager == null) {
      return -1;
    }

    List<String> tableNames = _helixAdmin.getResourcesInCluster(_helixClusterName);
    int numSegmentsLoaded = 0;
    for (String tableName: tableNames) {
      numSegmentsLoaded += instanceDataManager.getAllSegmentsMetadata(tableName).size();
    }
    return numSegmentsLoaded;
  }

  private int getNumSegmentsToLoad() {
    InstanceDataManager instanceDataManager = _serverInstance.getInstanceDataManager();
    if (instanceDataManager == null) {
      return -1;
    }

    HelixDataAccessor helixDataAccessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = helixDataAccessor.keyBuilder();
    int numSegmentsToLoad = 0;
    List<String> tableNames = _helixAdmin.getResourcesInCluster(_helixClusterName);
    for (String tableName: tableNames) {
      LiveInstance liveInstance = helixDataAccessor.getProperty(keyBuilder.liveInstance(_instanceId));
      String sessionId = liveInstance.getSessionId();
      PropertyKey currentStateKey = keyBuilder.currentState(_instanceId, sessionId, tableName);
      CurrentState currentState = helixDataAccessor.getProperty(currentStateKey);
      if (currentState != null && currentState.isValid()) {
        numSegmentsToLoad += currentState.getPartitionStateMap().size();
      }
    }
    return numSegmentsToLoad;
  }

  private void logSegmentsLoadingInfo() {
    InstanceDataManager instanceDataManager = _serverInstance.getInstanceDataManager();
    if (instanceDataManager == null) {
      return;
    }
    HelixDataAccessor helixDataAccessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = helixDataAccessor.keyBuilder();
    LiveInstance liveInstance = helixDataAccessor.getProperty(keyBuilder.liveInstance(_instanceId));
    String sessionId = liveInstance.getSessionId();
    List<String> tableNames = _helixAdmin.getResourcesInCluster(_helixClusterName);
    for (String tableName: tableNames) {
      PropertyKey currentStateKey = keyBuilder.currentState(_instanceId, sessionId, tableName);
      CurrentState currentState = helixDataAccessor.getProperty(currentStateKey);
      int numSegmentsLoaded = instanceDataManager.getAllSegmentsMetadata(tableName).size();
      if (currentState != null && currentState.isValid()) {
        int numSegmentsToLoad = currentState.getPartitionStateMap().size();
        LOGGER.info("Segments are not fully loaded during server bootstrap, current progress: table: {}, segments loading progress [ {} / {} ]", tableName, numSegmentsLoaded, numSegmentsToLoad);
      }
    }
  }

  private void setAdminApiPort(int adminApiPort) {
    Map<String, String> propToUpdate = new HashMap<>();
    propToUpdate.put(CommonConstants.Helix.Instance.ADMIN_PORT_KEY, String.valueOf(adminApiPort));
    updateInstanceConfigInHelix(propToUpdate);
  }

  private void setShuttingDownStatus(boolean shuttingDownStatus) {
    Map<String, String> propToUpdate = new HashMap<>();
    propToUpdate.put(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, String.valueOf(shuttingDownStatus));
    updateInstanceConfigInHelix(propToUpdate);
  }

  private void updateInstanceConfigInHelix(Map<String, String> props) {
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.PARTICIPANT, _helixClusterName).forParticipant(_instanceId)
            .build();
    _helixAdmin.setConfig(scope, props);
  }

  private void addInstanceTagIfNeeded(String clusterName, String instanceName) {
    InstanceConfig instanceConfig = _helixAdmin.getInstanceConfig(clusterName, instanceName);
    List<String> instanceTags = instanceConfig.getTags();
    if (instanceTags == null || instanceTags.size() == 0) {
      if (ZKMetadataProvider.getClusterTenantIsolationEnabled(_helixManager.getHelixPropertyStore())) {
        _helixAdmin.addInstanceTag(clusterName, instanceName,
            TableNameBuilder.OFFLINE.tableNameWithType(TagNameUtils.DEFAULT_TENANT_NAME));
        _helixAdmin.addInstanceTag(clusterName, instanceName,
            TableNameBuilder.REALTIME.tableNameWithType(TagNameUtils.DEFAULT_TENANT_NAME));
      } else {
        _helixAdmin.addInstanceTag(clusterName, instanceName, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
      }
    }
  }

  private void setupHelixSystemProperties(Configuration conf) {
    // [PINOT-2435] [PINOT-3927] Disable helix detection of flapping connection
    // Helix will shutdown and effectively remove the instance from cluster if
    // it detects flapping while the process continues to run
    // Helix ignores the value if it is <= 0. Hence, setting time window to small value
    // and number of connection failures within that window to high value
    System.setProperty(CommonConstants.Helix.HELIX_MANAGER_FLAPPING_TIME_WINDOW_KEY,
        conf.getString(CommonConstants.Helix.CONFIG_OF_HELIX_FLAPPING_TIMEWINDOW_MS,
            CommonConstants.Helix.DEFAULT_HELIX_FLAPPING_TIMEWINDOW_MS));

    System.setProperty(CommonConstants.Helix.HELIX_MANAGER_MAX_DISCONNECT_THRESHOLD_KEY,
        conf.getString(CommonConstants.Helix.CONFIG_OF_HELIX_MAX_DISCONNECT_THRESHOLD,
            CommonConstants.Helix.DEFAULT_HELIX_FLAPPING_MAX_DISCONNECT_THRESHOLD));
  }

  public void stop() {
    try {
      LOGGER.info("Closing PinotFS classes");
      PinotFSFactory.shutdown();
    } catch (IOException e) {
      LOGGER.warn("Caught exception closing PinotFS classes", e);
    }
    _adminApiApplication.stop();
    setShuttingDownStatus(true);

    // Total waiting time should include max query time.
    final long endTime = _maxShutdownWaitTimeMs + System.currentTimeMillis();
    if (_helixServerConfig.getBoolean(CommonConstants.Server.CONFIG_OF_ENABLE_SHUTDOWN_DELAY, true)) {
      Uninterruptibles.sleepUninterruptibly(_maxQueryTimeMs, TimeUnit.MILLISECONDS);
    }
    waitUntilNoIncomingQueries(System.currentTimeMillis(), endTime);
    _helixManager.disconnect();
    _serverInstance.shutDown();
    waitUntilNoOnlineResources(System.currentTimeMillis(), endTime);
  }

  private void waitUntilNoIncomingQueries(long startTime, final long endTime) {
    if (startTime >= endTime) {
      LOGGER.warn("Skip waiting until no incoming queries.");
      return;
    }
    LOGGER.info("Waiting upto {}ms until Pinot server doesn't receive any incoming queries...", (endTime - startTime));
    long currentTime = startTime;

    while (currentTime < endTime) {
      if (noIncomingQueries(currentTime)) {
        LOGGER.info("No incoming query within {}ms. Total waiting Time: {}ms", _checkIntervalTimeMs,
            (currentTime - startTime));
        return;
      }

      try {
        Thread.sleep(Math.min(_maxQueryTimeMs, (endTime - currentTime)));
      } catch (InterruptedException e) {
        LOGGER.error("Interrupted when waiting for Pinot server not to receive any queries.", e);
        Thread.currentThread().interrupt();
        return;
      }
      currentTime = System.currentTimeMillis();
    }
    LOGGER.error("Reach timeout when waiting for no incoming queries! Max waiting time: {}ms", _maxShutdownWaitTimeMs);
  }

  /**
   * Init a helix spectator to watch the external view updates.
   */
  private void waitUntilNoOnlineResources(long startTime, final long endTime) {
    if (startTime >= endTime) {
      LOGGER.warn("Skip waiting until no online resources.");
      return;
    }
    LOGGER.info("Waiting upto {}ms until no online resources...", (endTime - startTime));

    // Initialize a helix spectator.
    HelixManager spectatorManager =
        HelixManagerFactory.getZKHelixManager(_helixClusterName, _instanceId, InstanceType.SPECTATOR, _zkServers);
    try {
      spectatorManager.connect();

      Set<String> resources = fetchLatestTableResources(spectatorManager.getClusterManagmentTool());

      long currentTime = startTime;
      while (currentTime < endTime) {
        if (noOnlineResources(spectatorManager, resources)) {
          LOGGER.info("No online resource within {}ms. Total waiting Time: {}ms", _checkIntervalTimeMs,
              (currentTime - startTime));
          return;
        }

        try {
          Thread.sleep(Math.min(_checkIntervalTimeMs, (endTime - currentTime)));
        } catch (InterruptedException e) {
          LOGGER.error("Interrupted when waiting for no online resources.", e);
          Thread.currentThread().interrupt();
          return;
        }
        currentTime = System.currentTimeMillis();
      }
      LOGGER.error(
          "Reach timeout waiting for no online resources! Forcing Pinot server to shutdown. Max waiting time: {}ms",
          _maxShutdownWaitTimeMs);
    } catch (Exception e) {
      LOGGER.error("Exception waiting until no online resources. Skip checking external view.", e);
    } finally {
      spectatorManager.disconnect();
    }
  }

  private boolean noIncomingQueries(long currentTime) {
    return currentTime > _serverInstance.getLatestQueryTime() + _checkIntervalTimeMs;
  }

  private boolean noOnlineResources(HelixManager spectatorManager, Set<String> resources) {
    Iterator<String> iterator = resources.iterator();
    while (iterator.hasNext()) {
      String resourceName = iterator.next();
      ExternalView externalView =
          spectatorManager.getClusterManagmentTool().getResourceExternalView(_helixClusterName, resourceName);
      if (externalView == null) {
        iterator.remove();
        continue;
      }
      for (String partition : externalView.getPartitionSet()) {
        Map<String, String> instanceStateMap = externalView.getStateMap(partition);
        if (instanceStateMap.containsKey(_instanceId)) {
          if ("ONLINE".equals(instanceStateMap.get(_instanceId))) {
            return false;
          }
        }
      }
      iterator.remove();
    }
    return true;
  }

  private Set<String> fetchLatestTableResources(HelixAdmin helixAdmin) {
    Set<String> resourcesToMonitor = new HashSet<>();
    for (String resourceName : helixAdmin.getResourcesInCluster(_helixClusterName)) {
      // Only monitor table resources
      if (!TableNameBuilder.isTableResource(resourceName)) {
        continue;
      }
      // Only monitor enabled resources
      IdealState idealState = helixAdmin.getResourceIdealState(_helixClusterName, resourceName);
      if (idealState.isEnabled()) {
        for (String partitionName : idealState.getPartitionSet()) {
          if (idealState.getInstanceSet(partitionName).contains(_instanceId)) {
            resourcesToMonitor.add(resourceName);
            break;
          }
        }
      }
    }
    return resourcesToMonitor;
  }

  /**
   * This method is for reference purpose only.
   */
  public static HelixServerStarter startDefault() throws Exception {
    Configuration configuration = new PropertiesConfiguration();
    int port = 8003;
    configuration.addProperty(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT, port);
    configuration.addProperty("pinot.server.instance.dataDir", "/tmp/PinotServer/test" + port + "/index");
    configuration.addProperty("pinot.server.instance.segmentTarDir", "/tmp/PinotServer/test" + port + "/segmentTar");
    return new HelixServerStarter("quickstart", "localhost:2191", configuration);
  }

  public static void main(String[] args) throws Exception {
    /*
    // Another way to start a server via IDE
    if (args.length < 1) {
      throw new RuntimeException("Usage: cmd <port>");
    }
    for (int i = 0; i < args.length; i++) {
      final int port = Integer.valueOf(args[i]);
      final String serverFQDN = "localhost";
      final String server = "Server_" + serverFQDN + "_" + port;
      final Configuration configuration = new PropertiesConfiguration();
      configuration.addProperty("pinot.server.instance.dataDir", "/tmp/PinotServer/test" + port + "/index");
      configuration.addProperty("pinot.server.instance.segmentTarDir", "/tmp/PinotServer/test" + port + "/segmentTar");
      configuration.addProperty("instanceId", server);
      final HelixServerStarter pinotHelixStarter = new HelixServerStarter("PinotPerfTestCluster", "localhost:2191", configuration);
    }
    */
    startDefault();
  }
}
