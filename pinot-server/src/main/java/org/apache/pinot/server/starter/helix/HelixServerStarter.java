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
package org.apache.pinot.server.starter.helix;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.config.TagNameUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.NetUtil;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.common.utils.ServiceStatus.Status;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.filesystem.PinotFSFactory;
import org.apache.pinot.server.conf.ServerConf;
import org.apache.pinot.server.realtime.ControllerLeaderLocator;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.apache.pinot.server.starter.ServerInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.common.utils.CommonConstants.Helix.*;
import static org.apache.pinot.common.utils.CommonConstants.Server.*;


/**
 * Starter for Pinot server.
 * <p>When the server starts for the first time, it will automatically join the Helix cluster with the default tag.
 * <ul>
 *   <li>
 *     Optional start-up checks:
 *     <ul>
 *       <li>Service status check (ON by default)</li>
 *     </ul>
 *   </li>
 *   <li>
 *     Optional shut-down checks:
 *     <ul>
 *       <li>Query check (drains and finishes existing queries, ON by default)</li>
 *       <li>Resource check (wait for all resources OFFLINE, OFF by default)</li>
 *     </ul>
 *   </li>
 * </ul>
 */
public class HelixServerStarter {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixServerStarter.class);

  private final String _helixClusterName;
  private final Configuration _serverConf;
  private final String _instanceId;
  private final HelixManager _helixManager;
  private final HelixAdmin _helixAdmin;
  private final ServerInstance _serverInstance;
  private final AdminApiApplication _adminApiApplication;
  private final String _zkServers;

  public HelixServerStarter(String helixClusterName, String zkServer, Configuration serverConf)
      throws Exception {
    LOGGER.info("Starting Pinot server");
    long startTimeMs = System.currentTimeMillis();
    _helixClusterName = helixClusterName;

    // Make a clone so that changes to the config won't propagate to the caller
    _serverConf = ConfigurationUtils.cloneConfiguration(serverConf);

    // Log warnings for usage of deprecated config keys
    Map<String, String> deprecatedConfigKeyWarnings = new HashMap<String, String>() {{
      //noinspection deprecation
      put(CONFIG_OF_STARTER_ENABLE_SEGMENTS_LOADING_CHECK, String.format(
          "use %s instead, which will check the service status instead of comparing currentState/externalView with idealState (enabled by default)",
          CONFIG_OF_STARTUP_ENABLE_SERVICE_STATUS_CHECK));
      //noinspection deprecation
      put(CONFIG_OF_STARTER_TIMEOUT_IN_SECONDS, String
          .format("use %s instead, which is the timeout for the whole startup process (10 minutes by default)",
              CONFIG_OF_STARTUP_TIMEOUT_MS));
      //noinspection deprecation
      put(CONFIG_OF_ENABLE_SHUTDOWN_DELAY, String.format(
          "use %s instead, which will drain the queries (no incoming queries and all existing queries finished) (enabled by default)",
          CONFIG_OF_SHUTDOWN_ENABLE_QUERY_CHECK));
      //noinspection deprecation
      put(CONFIG_OF_INSTANCE_MAX_SHUTDOWN_WAIT_TIME, String
          .format("use %s instead, which is the timeout for the whole shutdown process (10 minutes by default)",
              CONFIG_OF_SHUTDOWN_TIMEOUT_MS));
      //noinspection deprecation
      put(CONFIG_OF_INSTANCE_CHECK_INTERVAL_TIME, String
          .format("use %s instead, which is the interval for the resource check (10 seconds by default)",
              CONFIG_OF_SHUTDOWN_RESOURCE_CHECK_INTERVAL_MS));
    }};
    for (Map.Entry<String, String> entry : deprecatedConfigKeyWarnings.entrySet()) {
      String deprecatedConfigKey = entry.getKey();
      if (_serverConf.containsKey(deprecatedConfigKey)) {
        LOGGER.warn("Found usage of deprecated config key: {}, {}", deprecatedConfigKey, entry.getValue());
      }
    }

    if (_serverConf.containsKey(CONFIG_OF_INSTANCE_ID)) {
      _instanceId = _serverConf.getString(CONFIG_OF_INSTANCE_ID);
    } else {
      String host = _serverConf.getString(KEY_OF_SERVER_NETTY_HOST, NetUtil.getHostAddress());
      int port = _serverConf.getInt(KEY_OF_SERVER_NETTY_PORT, DEFAULT_SERVER_NETTY_PORT);
      _instanceId = PREFIX_OF_SERVER_INSTANCE + host + "_" + port;
      _serverConf.addProperty(CONFIG_OF_INSTANCE_ID, _instanceId);
    }

    LOGGER.info("Connecting Helix components");
    setupHelixSystemProperties();
    // Replace all white-spaces from list of zkServers.
    _zkServers = zkServer.replaceAll("\\s+", "");
    _helixManager =
        HelixManagerFactory.getZKHelixManager(helixClusterName, _instanceId, InstanceType.PARTICIPANT, _zkServers);
    _helixManager.connect();
    _helixAdmin = _helixManager.getClusterManagmentTool();
    addInstanceTagIfNeeded(helixClusterName, _instanceId);
    ZkHelixPropertyStore<ZNRecord> propertyStore = _helixManager.getHelixPropertyStore();

    LOGGER.info("Starting server instance");
    Utils.logVersions();
    ServerConf serverInstanceConfig = DefaultHelixStarterServerConfig.getDefaultHelixServerConfig(_serverConf);
    // Need to do this before we start receiving state transitions.
    ServerSegmentCompletionProtocolHandler
        .init(_serverConf.subset(SegmentCompletionProtocol.PREFIX_OF_CONFIG_OF_SEGMENT_UPLOADER));
    _serverInstance = new ServerInstance();
    _serverInstance.init(serverInstanceConfig, propertyStore);
    _serverInstance.start();

    // Register state model factory
    SegmentFetcherAndLoader fetcherAndLoader =
        new SegmentFetcherAndLoader(_serverConf, _serverInstance.getInstanceDataManager(), propertyStore);
    StateModelFactory<?> stateModelFactory =
        new SegmentOnlineOfflineStateModelFactory(_instanceId, _serverInstance.getInstanceDataManager(),
            fetcherAndLoader, propertyStore);
    _helixManager.getStateMachineEngine()
        .registerStateModelFactory(SegmentOnlineOfflineStateModelFactory.getStateModelName(), stateModelFactory);

    // Start restlet server for admin API endpoint
    int adminApiPort = _serverConf.getInt(CONFIG_OF_ADMIN_API_PORT, DEFAULT_ADMIN_API_PORT);
    _adminApiApplication = new AdminApiApplication(_serverInstance);
    _adminApiApplication.start(adminApiPort);
    setAdminApiPort(adminApiPort);

    ServerMetrics serverMetrics = _serverInstance.getServerMetrics();
    // Register message handler factory
    SegmentMessageHandlerFactory messageHandlerFactory =
        new SegmentMessageHandlerFactory(fetcherAndLoader, _serverInstance.getInstanceDataManager(), serverMetrics);
    _helixManager.getMessagingService()
        .registerMessageHandlerFactory(Message.MessageType.USER_DEFINE_MSG.toString(), messageHandlerFactory);

    serverMetrics.addCallbackGauge(INSTANCE_CONNECTED_METRIC_NAME, () -> _helixManager.isConnected() ? 1L : 0L);
    _helixManager
        .addPreConnectCallback(() -> serverMetrics.addMeteredGlobalValue(ServerMeter.HELIX_ZOOKEEPER_RECONNECTS, 1L));

    // Register the service status handler
    registerServiceStatusHandler();

    ControllerLeaderLocator.create(_helixManager);

    if (_serverConf
        .getBoolean(CONFIG_OF_STARTUP_ENABLE_SERVICE_STATUS_CHECK, DEFAULT_STARTUP_ENABLE_SERVICE_STATUS_CHECK)) {
      long endTimeMs = startTimeMs + _serverConf.getLong(CONFIG_OF_STARTUP_TIMEOUT_MS, DEFAULT_STARTUP_TIMEOUT_MS);
      startupServiceStatusCheck(endTimeMs);
    }
    setShuttingDownStatus(false);
    LOGGER.info("Pinot server ready");

    // Create metrics for mmap stuff
    serverMetrics.addCallbackGauge("memory.directBufferCount", PinotDataBuffer::getDirectBufferCount);
    serverMetrics.addCallbackGauge("memory.directBufferUsage", PinotDataBuffer::getDirectBufferUsage);
    serverMetrics.addCallbackGauge("memory.mmapBufferCount", PinotDataBuffer::getMmapBufferCount);
    serverMetrics.addCallbackGauge("memory.mmapBufferUsage", PinotDataBuffer::getMmapBufferUsage);
    serverMetrics.addCallbackGauge("memory.allocationFailureCount", PinotDataBuffer::getAllocationFailureCount);
  }

  /**
   * Fetches the resources to monitor and registers the {@link org.apache.pinot.common.utils.ServiceStatus.ServiceStatusCallback}s
   */
  private void registerServiceStatusHandler() {

    double minResourcePercentForStartup = _serverConf
        .getDouble(CONFIG_OF_SERVER_MIN_RESOURCE_PERCENT_FOR_START, DEFAULT_SERVER_MIN_RESOURCE_PERCENT_FOR_START);
    int realtimeConsumptionCatchupWaitMs = _serverConf.getInt(
        CommonConstants.Server.CONFIG_OF_STARTUP_REALTIME_CONSUMPTION_CATCHUP_WAIT_MS,
        CommonConstants.Server.DEFAULT_STARTUP_REALTIME_CONSUMPTION_CATCHUP_WAIT_MS);

    // collect all resources which have this instance in the ideal state
    List<String> resourcesToMonitor = new ArrayList<>();
    // if even 1 resource has this instance in ideal state with state CONSUMING, set this to true
    boolean foundConsuming = false;
    boolean checkRealtime = realtimeConsumptionCatchupWaitMs > 0;

    for (String resourceName : _helixAdmin.getResourcesInCluster(_helixClusterName)) {
      // Only monitor table resources
      if (!TableNameBuilder.isTableResource(resourceName)) {
        continue;
      }

      // Only monitor enabled resources
      IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, resourceName);
      if (idealState.isEnabled()) {

        for (String partitionName : idealState.getPartitionSet()) {
          if (idealState.getInstanceSet(partitionName).contains(_instanceId)) {
            resourcesToMonitor.add(resourceName);
            break;
          }
        }
        if (checkRealtime && !foundConsuming && TableNameBuilder.isRealtimeTableResource(resourceName)) {
          for (String partitionName : idealState.getPartitionSet()) {
            if (CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING.equals(
                idealState.getInstanceStateMap(partitionName).get(_instanceId))) {
              foundConsuming = true;
              break;
            }
          }
        }
      }
    }

    ImmutableList.Builder<ServiceStatus.ServiceStatusCallback> serviceStatusCallbackListBuilder =
        new ImmutableList.Builder<>();
    serviceStatusCallbackListBuilder.add(
        new ServiceStatus.IdealStateAndCurrentStateMatchServiceStatusCallback(_helixManager, _helixClusterName,
            _instanceId, resourcesToMonitor, minResourcePercentForStartup));
    serviceStatusCallbackListBuilder.add(
        new ServiceStatus.IdealStateAndExternalViewMatchServiceStatusCallback(_helixManager, _helixClusterName,
            _instanceId, resourcesToMonitor, minResourcePercentForStartup));
    if (checkRealtime && foundConsuming) {
      serviceStatusCallbackListBuilder.add(
          new ServiceStatus.RealtimeConsumptionCatchupServiceStatusCallback(_helixManager, _helixClusterName,
              _instanceId, realtimeConsumptionCatchupWaitMs));
    }
    LOGGER.info("Registering service status handler");
    ServiceStatus.setServiceStatusCallback(
        new ServiceStatus.MultipleCallbackServiceStatusCallback(serviceStatusCallbackListBuilder.build()));
  }

  private void setAdminApiPort(int adminApiPort) {
    Map<String, String> propToUpdate = new HashMap<>();
    propToUpdate.put(Instance.ADMIN_PORT_KEY, String.valueOf(adminApiPort));
    updateInstanceConfigInHelix(propToUpdate);
  }

  private void setShuttingDownStatus(boolean shuttingDownStatus) {
    Map<String, String> propToUpdate = new HashMap<>();
    propToUpdate.put(IS_SHUTDOWN_IN_PROGRESS, String.valueOf(shuttingDownStatus));
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
        _helixAdmin.addInstanceTag(clusterName, instanceName, UNTAGGED_SERVER_INSTANCE);
      }
    }
  }

  private void setupHelixSystemProperties() {
    // NOTE: Helix will disconnect the manager and disable the instance if it detects flapping (too frequent disconnect
    // from ZooKeeper). Setting flapping time window to a small value can avoid this from happening. Helix ignores the
    // non-positive value, so set the default value as 1.
    System.setProperty(SystemPropertyKeys.FLAPPING_TIME_WINDOW,
        _serverConf.getString(CONFIG_OF_SERVER_FLAPPING_TIME_WINDOW_MS, DEFAULT_FLAPPING_TIME_WINDOW_MS));
  }

  /**
   * When the server starts, check if the service status turns GOOD.
   *
   * @param endTimeMs Timeout for the check
   */
  private void startupServiceStatusCheck(long endTimeMs) {
    LOGGER.info("Starting startup service status check");
    long startTimeMs = System.currentTimeMillis();
    long checkIntervalMs = _serverConf
        .getLong(CONFIG_OF_STARTUP_SERVICE_STATUS_CHECK_INTERVAL_MS, DEFAULT_STARTUP_SERVICE_STATUS_CHECK_INTERVAL_MS);

    while (System.currentTimeMillis() < endTimeMs) {
      Status serviceStatus = ServiceStatus.getServiceStatus();
      long currentTimeMs = System.currentTimeMillis();
      if (serviceStatus == Status.GOOD) {
        LOGGER.info("Service status is GOOD after {}ms", currentTimeMs - startTimeMs);
        return;
      } else if (serviceStatus == Status.BAD) {
        throw new IllegalStateException("Service status is BAD");
      }
      long sleepTimeMs = Math.min(checkIntervalMs, endTimeMs - currentTimeMs);
      if (sleepTimeMs > 0) {
        LOGGER.info("Sleep for {}ms as service status has not turned GOOD: {}", sleepTimeMs, ServiceStatus.getStatusDescription());
        try {
          Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
          LOGGER.warn("Got interrupted while checking service status", e);
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    LOGGER.warn("Service status has not turned GOOD within {}ms: {}", System.currentTimeMillis() - startTimeMs,
        ServiceStatus.getStatusDescription());
  }

  public void stop() {
    LOGGER.info("Shutting down Pinot server");
    long startTimeMs = System.currentTimeMillis();

    try {
      LOGGER.info("Closing PinotFS classes");
      PinotFSFactory.shutdown();
    } catch (IOException e) {
      LOGGER.warn("Caught exception closing PinotFS classes", e);
    }
    _adminApiApplication.stop();
    setShuttingDownStatus(true);

    long endTimeMs = startTimeMs + _serverConf.getLong(CONFIG_OF_SHUTDOWN_TIMEOUT_MS, DEFAULT_SHUTDOWN_TIMEOUT_MS);
    if (_serverConf.getBoolean(CONFIG_OF_SHUTDOWN_ENABLE_QUERY_CHECK, DEFAULT_SHUTDOWN_ENABLE_QUERY_CHECK)) {
      shutdownQueryCheck(endTimeMs);
    }
    _helixManager.disconnect();
    _serverInstance.shutDown();
    if (_serverConf.getBoolean(CONFIG_OF_SHUTDOWN_ENABLE_RESOURCE_CHECK, DEFAULT_SHUTDOWN_ENABLE_RESOURCE_CHECK)) {
      shutdownResourceCheck(endTimeMs);
    }
  }

  /**
   * When shutting down the server, drains the queries (no incoming queries and all existing queries finished).
   *
   * @param endTimeMs Timeout for the check
   */
  private void shutdownQueryCheck(long endTimeMs) {
    LOGGER.info("Starting shutdown query check");
    long startTimeMs = System.currentTimeMillis();

    long maxQueryTimeMs = _serverConf.getLong(CONFIG_OF_QUERY_EXECUTOR_TIMEOUT, DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    long noQueryThresholdMs = _serverConf.getLong(CONFIG_OF_SHUTDOWN_NO_QUERY_THRESHOLD_MS, maxQueryTimeMs);

    // Wait until no incoming queries
    boolean noIncomingQueries = false;
    long currentTimeMs;
    while ((currentTimeMs = System.currentTimeMillis()) < endTimeMs) {
      long noQueryTimeMs = currentTimeMs - _serverInstance.getLatestQueryTime();
      if (noQueryTimeMs >= noQueryThresholdMs) {
        LOGGER.info("No query received within {}ms (larger than the threshold: {}ms), mark it as no incoming queries",
            noQueryTimeMs, noQueryThresholdMs);
        noIncomingQueries = true;
        break;
      }
      long sleepTimeMs = Math.min(noQueryThresholdMs - noQueryTimeMs, endTimeMs - currentTimeMs);
      LOGGER.info(
          "Sleep for {}ms as there are still incoming queries (no query time: {}ms is smaller than the threshold: {}ms)",
          sleepTimeMs, noQueryTimeMs, noQueryThresholdMs);
      try {
        Thread.sleep(sleepTimeMs);
      } catch (InterruptedException e) {
        LOGGER.warn("Got interrupted while waiting for no incoming queries", e);
        Thread.currentThread().interrupt();
        break;
      }
    }
    if (noIncomingQueries) {
      // Ensure all the existing queries are finished
      long latestQueryFinishTimeMs = _serverInstance.getLatestQueryTime() + maxQueryTimeMs;
      if (latestQueryFinishTimeMs > currentTimeMs) {
        long sleepTimeMs = latestQueryFinishTimeMs - currentTimeMs;
        LOGGER.info("Sleep for {}ms to ensure all the existing queries are finished", sleepTimeMs);
        try {
          Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
          LOGGER.warn("Got interrupted while waiting for all the existing queries to be finished", e);
          Thread.currentThread().interrupt();
        }
      }
      LOGGER.info("Finished draining queries after {}ms", System.currentTimeMillis() - startTimeMs);
    } else {
      LOGGER.warn("Failed to drain queries within {}ms", System.currentTimeMillis() - startTimeMs);
    }
  }

  /**
   * When shutting down the server, waits for all the resources turn OFFLINE (all partitions served by the server are
   * neither ONLINE or CONSUMING).
   *
   * @param endTimeMs Timeout for the check
   */
  private void shutdownResourceCheck(long endTimeMs) {
    LOGGER.info("Starting shutdown resource check");
    long startTimeMs = System.currentTimeMillis();

    if (startTimeMs >= endTimeMs) {
      LOGGER.warn("Skipping shutdown resource check because shutdown timeout is already reached");
      return;
    }

    HelixAdmin helixAdmin = null;
    try {
      helixAdmin = new ZKHelixAdmin(_zkServers);

      // Monitor all enabled table resources that the server serves
      Set<String> resourcesToMonitor = new HashSet<>();
      for (String resourceName : helixAdmin.getResourcesInCluster(_helixClusterName)) {
        if (TableNameBuilder.isTableResource(resourceName)) {
          IdealState idealState = helixAdmin.getResourceIdealState(_helixClusterName, resourceName);
          if (idealState == null || !idealState.isEnabled()) {
            continue;
          }
          for (String partition : idealState.getPartitionSet()) {
            if (idealState.getInstanceSet(partition).contains(_instanceId)) {
              resourcesToMonitor.add(resourceName);
              break;
            }
          }
        }
      }

      long checkIntervalMs = _serverConf
          .getLong(CONFIG_OF_SHUTDOWN_RESOURCE_CHECK_INTERVAL_MS, DEFAULT_SHUTDOWN_RESOURCE_CHECK_INTERVAL_MS);
      while (System.currentTimeMillis() < endTimeMs) {
        Iterator<String> iterator = resourcesToMonitor.iterator();
        String currentResource = null;
        while (iterator.hasNext()) {
          currentResource = iterator.next();
          if (isResourceOffline(helixAdmin, currentResource)) {
            iterator.remove();
          } else {
            // Do not check remaining resources if one resource is not OFFLINE
            break;
          }
        }
        long currentTimeMs = System.currentTimeMillis();
        if (resourcesToMonitor.isEmpty()) {
          LOGGER.info("All resources are OFFLINE after {}ms", currentTimeMs - startTimeMs);
          return;
        }
        long sleepTimeMs = Math.min(checkIntervalMs, endTimeMs - currentTimeMs);
        LOGGER.info("Sleep for {}ms as some resources [{}, ...] are still ONLINE", sleepTimeMs, currentResource);
        try {
          Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
          LOGGER.warn("Got interrupted while waiting for all resources OFFLINE", e);
          Thread.currentThread().interrupt();
          break;
        }
      }

      // Check all remaining resources
      Iterator<String> iterator = resourcesToMonitor.iterator();
      while (iterator.hasNext()) {
        if (isResourceOffline(helixAdmin, iterator.next())) {
          iterator.remove();
        }
      }
      long currentTimeMs = System.currentTimeMillis();
      if (resourcesToMonitor.isEmpty()) {
        LOGGER.info("All resources are OFFLINE after {}ms", currentTimeMs - startTimeMs);
      } else {
        LOGGER.warn("There are still {} resources ONLINE within {}ms: {}", resourcesToMonitor.size(),
            currentTimeMs - startTimeMs, resourcesToMonitor);
      }
    } finally {
      if (helixAdmin != null) {
        helixAdmin.close();
      }
    }
  }

  private boolean isResourceOffline(HelixAdmin helixAdmin, String resource) {
    ExternalView externalView = helixAdmin.getResourceExternalView(_helixClusterName, resource);
    // Treat deleted resource as OFFLINE
    if (externalView == null) {
      return true;
    }
    for (String partition : externalView.getPartitionSet()) {
      Map<String, String> instanceStateMap = externalView.getStateMap(partition);
      String state = instanceStateMap.get(_instanceId);
      if (StateModel.SegmentOnlineOfflineStateModel.ONLINE.equals(state)
          || StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING.equals(state)) {
        return false;
      }
    }
    return true;
  }

  /**
   * This method is for reference purpose only.
   */
  @SuppressWarnings("UnusedReturnValue")
  public static HelixServerStarter startDefault()
      throws Exception {
    Configuration serverConf = new BaseConfiguration();
    int port = 8003;
    serverConf.addProperty(KEY_OF_SERVER_NETTY_PORT, port);
    serverConf.addProperty(CONFIG_OF_INSTANCE_DATA_DIR, "/tmp/PinotServer/test" + port + "/index");
    serverConf.addProperty(CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR, "/tmp/PinotServer/test" + port + "/segmentTar");
    return new HelixServerStarter("quickstart", "localhost:2191", serverConf);
  }

  public static void main(String[] args)
      throws Exception {
    startDefault();
  }
}
