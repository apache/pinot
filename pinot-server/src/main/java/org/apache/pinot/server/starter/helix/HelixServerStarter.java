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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.common.utils.ServiceStatus.Status;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.query.request.context.ThreadTimer;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.core.util.ListenerConfigUtil;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneIndexRefreshState;
import org.apache.pinot.segment.local.segment.memory.PinotDataBuffer;
import org.apache.pinot.server.api.access.AccessControlFactory;
import org.apache.pinot.server.conf.ServerConf;
import org.apache.pinot.server.realtime.ControllerLeaderLocator;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.apache.pinot.server.starter.ServerInstance;
import org.apache.pinot.server.starter.ServerQueriesDisabledTracker;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.spi.services.ServiceStartable;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.CommonConstants.Helix.Instance;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.CommonConstants.Server.SegmentCompletionProtocol;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
public class HelixServerStarter implements ServiceStartable {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixServerStarter.class);

  private final String _helixClusterName;
  private final String _zkAddress;
  private final PinotConfiguration _serverConf;
  private final List<ListenerConfig> _listenerConfigs;
  private final String _host;
  private final int _port;
  private final String _instanceId;
  private final HelixConfigScope _instanceConfigScope;
  private HelixManager _helixManager;
  private HelixAdmin _helixAdmin;
  private ServerInstance _serverInstance;
  private AdminApiApplication _adminApiApplication;
  private ServerQueriesDisabledTracker _serverQueriesDisabledTracker;
  private RealtimeLuceneIndexRefreshState _realtimeLuceneIndexRefreshState;

  public HelixServerStarter(String helixClusterName, String zkAddress, PinotConfiguration serverConf)
      throws Exception {
    _helixClusterName = helixClusterName;
    _zkAddress = zkAddress;
    // Make a clone so that changes to the config won't propagate to the caller
    _serverConf = serverConf.clone();
    _listenerConfigs = ListenerConfigUtil.buildServerAdminConfigs(_serverConf);

    _host = _serverConf.getProperty(Helix.KEY_OF_SERVER_NETTY_HOST,
        _serverConf.getProperty(Helix.SET_INSTANCE_ID_TO_HOSTNAME_KEY, false) ? NetUtils.getHostnameOrAddress()
            : NetUtils.getHostAddress());
    _port = _serverConf.getProperty(Helix.KEY_OF_SERVER_NETTY_PORT, Helix.DEFAULT_SERVER_NETTY_PORT);

    String instanceId = _serverConf.getProperty(Server.CONFIG_OF_INSTANCE_ID);
    if (instanceId == null) {
      instanceId = Helix.PREFIX_OF_SERVER_INSTANCE + _host + "_" + _port;
      _serverConf.addProperty(Server.CONFIG_OF_INSTANCE_ID, instanceId);
    }
    _instanceId = instanceId;

    _instanceConfigScope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.PARTICIPANT, _helixClusterName).forParticipant(_instanceId)
            .build();

    // Enable/disable thread CPU time measurement through instance config.
    ThreadTimer.setThreadCpuTimeMeasurementEnabled(_serverConf
        .getProperty(Server.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT,
            Server.DEFAULT_ENABLE_THREAD_CPU_TIME_MEASUREMENT));

    // Set data table version send to broker.
    DataTableBuilder.setCurrentDataTableVersion(_serverConf
        .getProperty(Server.CONFIG_OF_CURRENT_DATA_TABLE_VERSION,
            Server.DEFAULT_CURRENT_DATA_TABLE_VERSION));
  }

  /**
   * Fetches the resources to monitor and registers the {@link org.apache.pinot.common.utils.ServiceStatus.ServiceStatusCallback}s
   */
  private void registerServiceStatusHandler() {
    double minResourcePercentForStartup = _serverConf
        .getProperty(Server.CONFIG_OF_SERVER_MIN_RESOURCE_PERCENT_FOR_START,
            Server.DEFAULT_SERVER_MIN_RESOURCE_PERCENT_FOR_START);
    int realtimeConsumptionCatchupWaitMs = _serverConf
        .getProperty(Server.CONFIG_OF_STARTUP_REALTIME_CONSUMPTION_CATCHUP_WAIT_MS,
            Server.DEFAULT_STARTUP_REALTIME_CONSUMPTION_CATCHUP_WAIT_MS);

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
            if (StateModel.SegmentStateModel.CONSUMING
                .equals(idealState.getInstanceStateMap(partitionName).get(_instanceId))) {
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
    ServiceStatus.setServiceStatusCallback(_instanceId,
        new ServiceStatus.MultipleCallbackServiceStatusCallback(serviceStatusCallbackListBuilder.build()));
  }

  private void updateInstanceConfigIfNeeded(String host, int port) {
    InstanceConfig instanceConfig = _helixAdmin.getInstanceConfig(_helixClusterName, _instanceId);
    boolean needToUpdateInstanceConfig = false;

    // Add default instance tags if not exist
    List<String> instanceTags = instanceConfig.getTags();
    if (instanceTags == null || instanceTags.size() == 0) {
      if (ZKMetadataProvider.getClusterTenantIsolationEnabled(_helixManager.getHelixPropertyStore())) {
        instanceConfig.addTag(TagNameUtils.getOfflineTagForTenant(null));
        instanceConfig.addTag(TagNameUtils.getRealtimeTagForTenant(null));
      } else {
        instanceConfig.addTag(Helix.UNTAGGED_SERVER_INSTANCE);
      }
      needToUpdateInstanceConfig = true;
    }

    // Update host and port if needed
    if (!host.equals(instanceConfig.getHostName())) {
      instanceConfig.setHostName(host);
      needToUpdateInstanceConfig = true;
    }
    String portStr = Integer.toString(port);
    if (!portStr.equals(instanceConfig.getPort())) {
      instanceConfig.setPort(portStr);
      needToUpdateInstanceConfig = true;
    }

    if (needToUpdateInstanceConfig) {
      LOGGER.info("Updating instance config for instance: {} with instance tags: {}, host: {}, port: {}", _instanceId,
          instanceTags, host, port);
    } else {
      LOGGER.info("Instance config for instance: {} has instance tags: {}, host: {}, port: {}, no need to update",
          _instanceId, instanceTags, host, port);
      return;
    }

    // NOTE: Use HelixDataAccessor.setProperty() instead of HelixAdmin.setInstanceConfig() because the latter explicitly
    // forbids instance host/port modification
    HelixDataAccessor helixDataAccessor = _helixManager.getHelixDataAccessor();
    Preconditions.checkState(
        helixDataAccessor.setProperty(helixDataAccessor.keyBuilder().instanceConfig(_instanceId), instanceConfig),
        "Failed to update instance config");
  }

  private void setupHelixSystemProperties() {
    // NOTE: Helix will disconnect the manager and disable the instance if it detects flapping (too frequent disconnect
    // from ZooKeeper). Setting flapping time window to a small value can avoid this from happening. Helix ignores the
    // non-positive value, so set the default value as 1.
    System.setProperty(SystemPropertyKeys.FLAPPING_TIME_WINDOW,
        _serverConf.getProperty(Helix.CONFIG_OF_SERVER_FLAPPING_TIME_WINDOW_MS, Helix.DEFAULT_FLAPPING_TIME_WINDOW_MS));
  }

  /**
   * When the server starts, check if the service status turns GOOD.
   *
   * @param endTimeMs Timeout for the check
   */
  private void startupServiceStatusCheck(long endTimeMs) {
    LOGGER.info("Starting startup service status check");
    long startTimeMs = System.currentTimeMillis();
    long checkIntervalMs = _serverConf.getProperty(Server.CONFIG_OF_STARTUP_SERVICE_STATUS_CHECK_INTERVAL_MS,
        Server.DEFAULT_STARTUP_SERVICE_STATUS_CHECK_INTERVAL_MS);

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
        LOGGER.info("Sleep for {}ms as service status has not turned GOOD: {}", sleepTimeMs,
            ServiceStatus.getStatusDescription());
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

  @Override
  public ServiceRole getServiceRole() {
    return ServiceRole.SERVER;
  }

  @Override
  public void start()
      throws Exception {
    LOGGER.info("Starting Pinot server");
    long startTimeMs = System.currentTimeMillis();

    LOGGER.info("Initializing Helix manager with zkAddress: {}, clusterName: {}, instanceId: {}", _zkAddress,
        _helixClusterName, _instanceId);
    setupHelixSystemProperties();
    _helixManager =
        HelixManagerFactory.getZKHelixManager(_helixClusterName, _instanceId, InstanceType.PARTICIPANT, _zkAddress);

    LOGGER.info("Initializing server instance and registering state model factory");
    Utils.logVersions();
    ControllerLeaderLocator.create(_helixManager);
    ServerSegmentCompletionProtocolHandler
        .init(_serverConf.subset(SegmentCompletionProtocol.PREFIX_OF_CONFIG_OF_SEGMENT_UPLOADER));
    ServerConf serverInstanceConfig = DefaultHelixStarterServerConfig.getDefaultHelixServerConfig(_serverConf);
    _serverInstance = new ServerInstance(serverInstanceConfig, _helixManager);
    ServerMetrics serverMetrics = _serverInstance.getServerMetrics();
    InstanceDataManager instanceDataManager = _serverInstance.getInstanceDataManager();
    SegmentFetcherAndLoader fetcherAndLoader =
        new SegmentFetcherAndLoader(_serverConf, instanceDataManager, serverMetrics);
    StateModelFactory<?> stateModelFactory =
        new SegmentOnlineOfflineStateModelFactory(_instanceId, instanceDataManager, fetcherAndLoader);
    _helixManager.getStateMachineEngine()
        .registerStateModelFactory(SegmentOnlineOfflineStateModelFactory.getStateModelName(), stateModelFactory);
    // Start the server instance as a pre-connect callback so that it starts after connecting to the ZK in order to
    // access the property store, but before receiving state transitions
    _helixManager.addPreConnectCallback(_serverInstance::start);

    LOGGER.info("Connecting Helix manager");
    _helixManager.connect();
    _helixAdmin = _helixManager.getClusterManagmentTool();
    updateInstanceConfigIfNeeded(_host, _port);

    // Start restlet server for admin API endpoint
    String accessControlFactoryClass =
        _serverConf.getProperty(Server.ACCESS_CONTROL_FACTORY_CLASS, Server.DEFAULT_ACCESS_CONTROL_FACTORY_CLASS);
    LOGGER.info("Using class: {} as the AccessControlFactory", accessControlFactoryClass);
    final AccessControlFactory accessControlFactory;
    try {
      accessControlFactory = PluginManager.get().createInstance(accessControlFactoryClass);
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while creating new AccessControlFactory instance using class '" + accessControlFactoryClass
              + "'", e);
    }

    // Update admin API port
    LOGGER.info("Starting server admin application on: {}", ListenerConfigUtil.toString(_listenerConfigs));
    _adminApiApplication = new AdminApiApplication(_serverInstance, accessControlFactory, _serverConf);
    _adminApiApplication.start(_listenerConfigs);

    // Update http admin port
    Optional<ListenerConfig> adminApiHttp = _listenerConfigs.stream()
        .filter(listener -> CommonConstants.HTTP_PROTOCOL.equals(listener.getProtocol())).findFirst();
    if (adminApiHttp.isPresent()) {
      _helixAdmin.setConfig(_instanceConfigScope,
          Collections.singletonMap(Instance.ADMIN_PORT_KEY, String.valueOf(adminApiHttp.get().getPort())));
    } else {
      _helixAdmin.removeConfig(_instanceConfigScope, Collections.singletonList(Instance.ADMIN_PORT_KEY));
    }

    // Update https admin port
    Optional<ListenerConfig> adminApiHttps = _listenerConfigs.stream()
        .filter(listener -> CommonConstants.HTTPS_PROTOCOL.equals(listener.getProtocol())).findFirst();
    if (adminApiHttps.isPresent()) {
      _helixAdmin.setConfig(_instanceConfigScope,
          Collections.singletonMap(Instance.ADMIN_HTTPS_PORT_KEY, String.valueOf(adminApiHttps.get().getPort())));
    } else {
      _helixAdmin.removeConfig(_instanceConfigScope, Collections.singletonList(Instance.ADMIN_HTTPS_PORT_KEY));
    }

    // Update nettytls port
    if (serverInstanceConfig.isNettyTlsServerEnabled()) {
      _helixAdmin.setConfig(_instanceConfigScope,
          Collections.singletonMap(Instance.NETTYTLS_PORT_KEY, String.valueOf(serverInstanceConfig.getNettyTlsPort())));
    } else {
      _helixAdmin.removeConfig(_instanceConfigScope, Collections.singletonList(Instance.NETTYTLS_PORT_KEY));
    }

    // Update gRPC port
    if (serverInstanceConfig.isEnableGrpcServer()) {
      _helixAdmin.setConfig(_instanceConfigScope,
          Collections.singletonMap(Instance.GRPC_PORT_KEY, String.valueOf(serverInstanceConfig.getGrpcPort())));
    } else {
      _helixAdmin.removeConfig(_instanceConfigScope, Collections.singletonList(Instance.GRPC_PORT_KEY));
    }

    // Register message handler factory
    SegmentMessageHandlerFactory messageHandlerFactory =
        new SegmentMessageHandlerFactory(fetcherAndLoader, instanceDataManager, serverMetrics);
    _helixManager.getMessagingService()
        .registerMessageHandlerFactory(Message.MessageType.USER_DEFINE_MSG.toString(), messageHandlerFactory);

    serverMetrics.addCallbackGauge(Helix.INSTANCE_CONNECTED_METRIC_NAME, () -> _helixManager.isConnected() ? 1L : 0L);
    _helixManager
        .addPreConnectCallback(() -> serverMetrics.addMeteredGlobalValue(ServerMeter.HELIX_ZOOKEEPER_RECONNECTS, 1L));

    // Register the service status handler
    registerServiceStatusHandler();

    if (_serverConf.getProperty(Server.CONFIG_OF_STARTUP_ENABLE_SERVICE_STATUS_CHECK,
        Server.DEFAULT_STARTUP_ENABLE_SERVICE_STATUS_CHECK)) {
      long endTimeMs =
          startTimeMs + _serverConf.getProperty(Server.CONFIG_OF_STARTUP_TIMEOUT_MS, Server.DEFAULT_STARTUP_TIMEOUT_MS);
      startupServiceStatusCheck(endTimeMs);
    }
    _helixAdmin.setConfig(_instanceConfigScope,
        Collections.singletonMap(Helix.IS_SHUTDOWN_IN_PROGRESS, Boolean.toString(false)));
    LOGGER.info("Pinot server ready");

    // Create metrics for mmap stuff
    serverMetrics.addCallbackGauge("memory.directBufferCount", PinotDataBuffer::getDirectBufferCount);
    serverMetrics.addCallbackGauge("memory.directBufferUsage", PinotDataBuffer::getDirectBufferUsage);
    serverMetrics.addCallbackGauge("memory.mmapBufferCount", PinotDataBuffer::getMmapBufferCount);
    serverMetrics.addCallbackGauge("memory.mmapBufferUsage", PinotDataBuffer::getMmapBufferUsage);
    serverMetrics.addCallbackGauge("memory.allocationFailureCount", PinotDataBuffer::getAllocationFailureCount);

    // Track metric for queries disabled
    _serverQueriesDisabledTracker = new ServerQueriesDisabledTracker(_helixClusterName, _instanceId, _helixManager, serverMetrics);
    _serverQueriesDisabledTracker.start();

    _realtimeLuceneIndexRefreshState = RealtimeLuceneIndexRefreshState.getInstance();
    _realtimeLuceneIndexRefreshState.start();
  }

  @Override
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
    _helixAdmin.setConfig(_instanceConfigScope,
        Collections.singletonMap(Helix.IS_SHUTDOWN_IN_PROGRESS, Boolean.toString(true)));

    long endTimeMs =
        startTimeMs + _serverConf.getProperty(Server.CONFIG_OF_SHUTDOWN_TIMEOUT_MS, Server.DEFAULT_SHUTDOWN_TIMEOUT_MS);
    if (_serverConf
        .getProperty(Server.CONFIG_OF_SHUTDOWN_ENABLE_QUERY_CHECK, Server.DEFAULT_SHUTDOWN_ENABLE_QUERY_CHECK)) {
      shutdownQueryCheck(endTimeMs);
    }
    _helixManager.disconnect();
    _serverInstance.shutDown();
    if (_serverConf
        .getProperty(Server.CONFIG_OF_SHUTDOWN_ENABLE_RESOURCE_CHECK, Server.DEFAULT_SHUTDOWN_ENABLE_RESOURCE_CHECK)) {
      shutdownResourceCheck(endTimeMs);
    }
    _serverQueriesDisabledTracker.stop();
    _realtimeLuceneIndexRefreshState.stop();
    LOGGER.info("Deregistering service status handler");
    ServiceStatus.removeServiceStatusCallback(_instanceId);
    LOGGER.info("Finish shutting down Pinot server for {}", _instanceId);
  }

  /**
   * When shutting down the server, drains the queries (no incoming queries and all existing queries finished).
   *
   * @param endTimeMs Timeout for the check
   */
  private void shutdownQueryCheck(long endTimeMs) {
    LOGGER.info("Starting shutdown query check");
    long startTimeMs = System.currentTimeMillis();

    long maxQueryTimeMs =
        _serverConf.getProperty(Server.CONFIG_OF_QUERY_EXECUTOR_TIMEOUT, Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    long noQueryThresholdMs = _serverConf.getProperty(Server.CONFIG_OF_SHUTDOWN_NO_QUERY_THRESHOLD_MS, maxQueryTimeMs);

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
      helixAdmin = new ZKHelixAdmin(_zkAddress);

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

      long checkIntervalMs = _serverConf.getProperty(Server.CONFIG_OF_SHUTDOWN_RESOURCE_CHECK_INTERVAL_MS,
          Server.DEFAULT_SHUTDOWN_RESOURCE_CHECK_INTERVAL_MS);
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
        if (sleepTimeMs > 0) {
          LOGGER.info("Sleep for {}ms as some resources [{}, ...] are still ONLINE", sleepTimeMs, currentResource);
          try {
            Thread.sleep(sleepTimeMs);
          } catch (InterruptedException e) {
            LOGGER.warn("Got interrupted while waiting for all resources OFFLINE", e);
            Thread.currentThread().interrupt();
            break;
          }
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
      if (StateModel.SegmentStateModel.ONLINE.equals(state) || StateModel.SegmentStateModel.CONSUMING.equals(state)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String getInstanceId() {
    return _instanceId;
  }

  @Override
  public PinotConfiguration getConfig() {
    return _serverConf;
  }

  /**
   * This method is for reference purpose only.
   */
  public static HelixServerStarter startDefault()
      throws Exception {
    Map<String, Object> properties = new HashMap<>();
    int port = 8003;
    properties.put(Helix.KEY_OF_SERVER_NETTY_PORT, port);
    properties.put(Server.CONFIG_OF_INSTANCE_DATA_DIR, "/tmp/PinotServer/test" + port + "/index");
    properties.put(Server.CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR, "/tmp/PinotServer/test" + port + "/segmentTar");

    HelixServerStarter serverStarter =
        new HelixServerStarter("quickstart", "localhost:2191", new PinotConfiguration(properties));
    serverStarter.start();
    return serverStarter;
  }

  public static void main(String[] args)
      throws Exception {
    startDefault();
  }
}
