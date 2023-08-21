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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.HelixAdmin;
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
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.restlet.resources.SystemResourceInfo;
import org.apache.pinot.common.utils.PinotAppConfigs;
import org.apache.pinot.common.utils.ServiceStartableUtils;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.common.utils.ServiceStatus.Status;
import org.apache.pinot.common.utils.TlsUtils;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeConsumptionRateManager;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.core.util.ListenerConfigUtil;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneIndexRefreshState;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.server.access.AccessControlFactory;
import org.apache.pinot.server.api.AdminApiApplication;
import org.apache.pinot.server.conf.ServerConf;
import org.apache.pinot.server.realtime.ControllerLeaderLocator;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.apache.pinot.server.starter.ServerInstance;
import org.apache.pinot.server.starter.ServerQueriesDisabledTracker;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.crypt.PinotCrypterFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.environmentprovider.PinotEnvironmentProvider;
import org.apache.pinot.spi.environmentprovider.PinotEnvironmentProviderFactory;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.spi.services.ServiceStartable;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.CommonConstants.Helix.Instance;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.CommonConstants.Server.SegmentCompletionProtocol;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.rewriter.QueryRewriterFactory;
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
public abstract class BaseServerStarter implements ServiceStartable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseServerStarter.class);

  protected String _helixClusterName;
  protected String _zkAddress;
  protected PinotConfiguration _serverConf;
  protected List<ListenerConfig> _listenerConfigs;
  protected String _hostname;
  protected int _port;
  protected String _instanceId;
  protected HelixConfigScope _instanceConfigScope;
  protected HelixManager _helixManager;
  protected HelixAdmin _helixAdmin;
  protected ServerInstance _serverInstance;
  protected AdminApiApplication _adminApiApplication;
  protected ServerQueriesDisabledTracker _serverQueriesDisabledTracker;
  protected RealtimeLuceneIndexRefreshState _realtimeLuceneIndexRefreshState;
  protected PinotEnvironmentProvider _pinotEnvironmentProvider;
  protected volatile boolean _isServerReadyToServeQueries = false;

  @Override
  public void init(PinotConfiguration serverConf)
      throws Exception {
    // Make a clone so that changes to the config won't propagate to the caller
    _serverConf = serverConf.clone();
    _zkAddress = _serverConf.getProperty(CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER);
    _helixClusterName = _serverConf.getProperty(CommonConstants.Helix.CONFIG_OF_CLUSTER_NAME);
    ServiceStartableUtils.applyClusterConfig(_serverConf, _zkAddress, _helixClusterName, ServiceRole.SERVER);

    setupHelixSystemProperties();
    _listenerConfigs = ListenerConfigUtil.buildServerAdminConfigs(_serverConf);
    _hostname = _serverConf.getProperty(Helix.KEY_OF_SERVER_NETTY_HOST,
        _serverConf.getProperty(Helix.SET_INSTANCE_ID_TO_HOSTNAME_KEY, false) ? NetUtils.getHostnameOrAddress()
            : NetUtils.getHostAddress());
    _port = _serverConf.getProperty(Helix.KEY_OF_SERVER_NETTY_PORT, Helix.DEFAULT_SERVER_NETTY_PORT);

    _instanceId = _serverConf.getProperty(Server.CONFIG_OF_INSTANCE_ID);
    if (_instanceId != null) {
      // NOTE:
      //   - Force all instances to have the same prefix in order to derive the instance type based on the instance id
      //   - Only log a warning instead of throw exception here for backward-compatibility
      if (!_instanceId.startsWith(Helix.PREFIX_OF_SERVER_INSTANCE)) {
        Preconditions.checkState(InstanceTypeUtils.isServer(_instanceId), "Invalid instance id '%s' for server",
            _instanceId);
        LOGGER.warn("Instance id '{}' does not have prefix '{}'", _instanceId, Helix.PREFIX_OF_SERVER_INSTANCE);
      }
    } else {
      _instanceId = Helix.PREFIX_OF_SERVER_INSTANCE + _hostname + "_" + _port;
      // NOTE: Need to add the instance id to the config because it is required in HelixInstanceDataManagerConfig
      _serverConf.addProperty(Server.CONFIG_OF_INSTANCE_ID, _instanceId);
    }
    if (_serverConf.getProperty(QueryConfig.KEY_OF_QUERY_SERVER_PORT, QueryConfig.DEFAULT_QUERY_SERVER_PORT) == 0) {
      _serverConf.setProperty(QueryConfig.KEY_OF_QUERY_SERVER_PORT, NetUtils.findOpenPort());
    }
    if (_serverConf.getProperty(QueryConfig.KEY_OF_QUERY_RUNNER_PORT, QueryConfig.DEFAULT_QUERY_RUNNER_PORT) == 0) {
      _serverConf.setProperty(QueryConfig.KEY_OF_QUERY_RUNNER_PORT, NetUtils.findOpenPort());
    }

    _instanceConfigScope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.PARTICIPANT, _helixClusterName).forParticipant(_instanceId)
            .build();

    // Initialize Pinot Environment Provider
    _pinotEnvironmentProvider = initializePinotEnvironmentProvider();

    // Initialize the data buffer factory
    PinotDataBuffer.loadDefaultFactory(serverConf);

    // Enable/disable thread CPU time measurement through instance config.
    ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(
        _serverConf.getProperty(Server.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT,
            Server.DEFAULT_ENABLE_THREAD_CPU_TIME_MEASUREMENT));
    // Enable/disable thread memory allocation tracking through instance config
    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(
        _serverConf.getProperty(Server.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT,
        Server.DEFAULT_THREAD_ALLOCATED_BYTES_MEASUREMENT));

    // Set data table version send to broker.
    int dataTableVersion =
        _serverConf.getProperty(Server.CONFIG_OF_CURRENT_DATA_TABLE_VERSION, DataTableBuilderFactory.DEFAULT_VERSION);
    if (dataTableVersion > DataTableBuilderFactory.DEFAULT_VERSION) {
      LOGGER.warn("Setting experimental DataTable version newer than default via config could result in"
          + " backward-compatibility issues. Current default DataTable version: "
          + DataTableBuilderFactory.DEFAULT_VERSION);
    }
    DataTableBuilderFactory.setDataTableVersion(dataTableVersion);

    LOGGER.info("Initializing Helix manager with zkAddress: {}, clusterName: {}, instanceId: {}", _zkAddress,
        _helixClusterName, _instanceId);
    _helixManager =
        HelixManagerFactory.getZKHelixManager(_helixClusterName, _instanceId, InstanceType.PARTICIPANT, _zkAddress);
  }

  /**
   *  Invoke pinot environment provider factory's init method to register the environment provider &
   *  return the instantiated environment provider.
   */
  @Nullable
  private PinotEnvironmentProvider initializePinotEnvironmentProvider() {
    PinotConfiguration environmentProviderConfigs =
        _serverConf.subset(Server.PREFIX_OF_CONFIG_OF_ENVIRONMENT_PROVIDER_FACTORY);
    if (environmentProviderConfigs.toMap().isEmpty()) {
      LOGGER.info("No environment provider config values provided for server property: {}",
          Server.PREFIX_OF_CONFIG_OF_ENVIRONMENT_PROVIDER_FACTORY);
      return null;
    }

    // Invoke pinot environment provider factory's init method
    PinotEnvironmentProviderFactory.init(environmentProviderConfigs);

    String environmentProviderClassName = _serverConf.getProperty(Server.ENVIRONMENT_PROVIDER_CLASS_NAME);
    if (environmentProviderClassName == null) {
      LOGGER.info("No className value provided for property: {}", Server.ENVIRONMENT_PROVIDER_CLASS_NAME);
      return null;
    }

    // Fetch environment provider instance
    return PinotEnvironmentProviderFactory.getEnvironmentProvider(environmentProviderClassName.toLowerCase());
  }

  /**
   * Fetches the resources to monitor and registers the
   * {@link org.apache.pinot.common.utils.ServiceStatus.ServiceStatusCallback}s
   */
  private void registerServiceStatusHandler() {
    double minResourcePercentForStartup =
        _serverConf.getProperty(Server.CONFIG_OF_SERVER_MIN_RESOURCE_PERCENT_FOR_START,
            Server.DEFAULT_SERVER_MIN_RESOURCE_PERCENT_FOR_START);
    int realtimeConsumptionCatchupWaitMs =
        _serverConf.getProperty(Server.CONFIG_OF_STARTUP_REALTIME_CONSUMPTION_CATCHUP_WAIT_MS,
            Server.DEFAULT_STARTUP_REALTIME_CONSUMPTION_CATCHUP_WAIT_MS);
    boolean isOffsetBasedConsumptionStatusCheckerEnabled =
        _serverConf.getProperty(Server.CONFIG_OF_ENABLE_REALTIME_OFFSET_BASED_CONSUMPTION_STATUS_CHECKER,
            Server.DEFAULT_ENABLE_REALTIME_OFFSET_BASED_CONSUMPTION_STATUS_CHECKER);
    boolean isFreshnessStatusCheckerEnabled =
        _serverConf.getProperty(Server.CONFIG_OF_ENABLE_REALTIME_FRESHNESS_BASED_CONSUMPTION_STATUS_CHECKER,
            Server.DEFAULT_ENABLE_REALTIME_FRESHNESS_BASED_CONSUMPTION_STATUS_CHECKER);
    int realtimeMinFreshnessMs = _serverConf.getProperty(Server.CONFIG_OF_STARTUP_REALTIME_MIN_FRESHNESS_MS,
        Server.DEFAULT_STARTUP_REALTIME_MIN_FRESHNESS_MS);

    // collect all resources which have this instance in the ideal state
    List<String> resourcesToMonitor = new ArrayList<>();

    Set<String> consumingSegments = new HashSet<>();
    boolean checkRealtime = realtimeConsumptionCatchupWaitMs > 0;
    if (isFreshnessStatusCheckerEnabled && realtimeMinFreshnessMs <= 0) {
      LOGGER.warn("Realtime min freshness {} must be > 0. Setting relatime min freshness to default {}.",
          realtimeMinFreshnessMs, Server.DEFAULT_STARTUP_REALTIME_MIN_FRESHNESS_MS);
      realtimeMinFreshnessMs = Server.DEFAULT_STARTUP_REALTIME_MIN_FRESHNESS_MS;
    }

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
        if (checkRealtime && TableNameBuilder.isRealtimeTableResource(resourceName)) {
          for (String partitionName : idealState.getPartitionSet()) {
            if (StateModel.SegmentStateModel.CONSUMING.equals(
                idealState.getInstanceStateMap(partitionName).get(_instanceId))) {
              consumingSegments.add(partitionName);
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
    boolean foundConsuming = !consumingSegments.isEmpty();
    if (checkRealtime && foundConsuming) {
      // We specifically put the freshness based checker first to ensure it's the only one setup if both checkers
      // are accidentally enabled together. The freshness based checker is a stricter version of the offset based
      // checker. But in the end, both checkers are bounded in time by realtimeConsumptionCatchupWaitMs.
      if (isFreshnessStatusCheckerEnabled) {
        LOGGER.info("Setting up freshness based status checker");
        FreshnessBasedConsumptionStatusChecker freshnessStatusChecker =
            new FreshnessBasedConsumptionStatusChecker(_serverInstance.getInstanceDataManager(), consumingSegments,
                realtimeMinFreshnessMs);
        Supplier<Integer> getNumConsumingSegmentsNotReachedMinFreshness =
            freshnessStatusChecker::getNumConsumingSegmentsNotReachedIngestionCriteria;
        serviceStatusCallbackListBuilder.add(
            new ServiceStatus.RealtimeConsumptionCatchupServiceStatusCallback(_helixManager, _helixClusterName,
                _instanceId, realtimeConsumptionCatchupWaitMs, getNumConsumingSegmentsNotReachedMinFreshness));
      } else if (isOffsetBasedConsumptionStatusCheckerEnabled) {
        LOGGER.info("Setting up offset based status checker");
        OffsetBasedConsumptionStatusChecker consumptionStatusChecker =
            new OffsetBasedConsumptionStatusChecker(_serverInstance.getInstanceDataManager(), consumingSegments);
        Supplier<Integer> getNumConsumingSegmentsNotReachedTheirLatestOffset =
            consumptionStatusChecker::getNumConsumingSegmentsNotReachedIngestionCriteria;
        serviceStatusCallbackListBuilder.add(
            new ServiceStatus.RealtimeConsumptionCatchupServiceStatusCallback(_helixManager, _helixClusterName,
                _instanceId, realtimeConsumptionCatchupWaitMs, getNumConsumingSegmentsNotReachedTheirLatestOffset));
      } else {
        LOGGER.info("Setting up static time based status checker");
        serviceStatusCallbackListBuilder.add(
            new ServiceStatus.RealtimeConsumptionCatchupServiceStatusCallback(_helixManager, _helixClusterName,
                _instanceId, realtimeConsumptionCatchupWaitMs, null));
      }
    }
    LOGGER.info("Registering service status handler");
    ServiceStatus.setServiceStatusCallback(_instanceId,
        new ServiceStatus.MultipleCallbackServiceStatusCallback(serviceStatusCallbackListBuilder.build()));
  }

  private void updateInstanceConfigIfNeeded(ServerConf serverConf) {
    InstanceConfig instanceConfig = HelixHelper.getInstanceConfig(_helixManager, _instanceId);

    // Update hostname and port
    boolean updated = HelixHelper.updateHostnamePort(instanceConfig, _hostname, _port);

    // Update tags
    updated |= HelixHelper.addDefaultTags(instanceConfig, () -> {
      if (ZKMetadataProvider.getClusterTenantIsolationEnabled(_helixManager.getHelixPropertyStore())) {
        return Arrays.asList(TagNameUtils.getOfflineTagForTenant(null), TagNameUtils.getRealtimeTagForTenant(null));
      } else {
        return Collections.singletonList(Helix.UNTAGGED_SERVER_INSTANCE);
      }
    });

    // Remove disabled partitions
    updated |= HelixHelper.removeDisabledPartitions(instanceConfig);

    // Update admin HTTP/HTTPS port
    int adminHttpPort = Integer.MIN_VALUE;
    int adminHttpsPort = Integer.MIN_VALUE;
    for (ListenerConfig listenerConfig : _listenerConfigs) {
      String protocol = listenerConfig.getProtocol();
      if (CommonConstants.HTTP_PROTOCOL.equals(protocol)) {
        adminHttpPort = listenerConfig.getPort();
      } else if (CommonConstants.HTTPS_PROTOCOL.equals(protocol)) {
        adminHttpsPort = listenerConfig.getPort();
      }
    }
    ZNRecord znRecord = instanceConfig.getRecord();
    Map<String, String> simpleFields = znRecord.getSimpleFields();
    updated |= updatePortIfNeeded(simpleFields, Instance.ADMIN_PORT_KEY, adminHttpPort);
    updated |= updatePortIfNeeded(simpleFields, Instance.ADMIN_HTTPS_PORT_KEY, adminHttpsPort);

    // Update netty TLS port
    int nettyTlsPort = serverConf.isNettyTlsServerEnabled() ? serverConf.getNettyTlsPort() : Integer.MIN_VALUE;
    updated |= updatePortIfNeeded(simpleFields, Instance.NETTY_TLS_PORT_KEY, nettyTlsPort);

    // Update gRPC port
    int grpcPort = serverConf.isEnableGrpcServer() ? serverConf.getGrpcPort() : Integer.MIN_VALUE;
    updated |= updatePortIfNeeded(simpleFields, Instance.GRPC_PORT_KEY, grpcPort);

    // Update multi-stage query engine ports
    if (serverConf.isMultiStageServerEnabled()) {
      updated |= updatePortIfNeeded(simpleFields, Instance.MULTI_STAGE_QUERY_ENGINE_SERVICE_PORT_KEY,
          serverConf.getMultiStageServicePort());
      updated |= updatePortIfNeeded(simpleFields, Instance.MULTI_STAGE_QUERY_ENGINE_MAILBOX_PORT_KEY,
          serverConf.getMultiStageMailboxPort());
    }

    // Update environment properties
    if (_pinotEnvironmentProvider != null) {
      // Retrieve failure domain information and add to the environment properties map
      String failureDomain = _pinotEnvironmentProvider.getFailureDomain();
      Map<String, String> environmentProperties =
          Collections.singletonMap(CommonConstants.INSTANCE_FAILURE_DOMAIN, failureDomain);
      if (!environmentProperties.equals(znRecord.getMapField(CommonConstants.ENVIRONMENT_IDENTIFIER))) {
        LOGGER.info("Updating instance: {} with environment properties: {}", _instanceId, environmentProperties);
        znRecord.setMapField(CommonConstants.ENVIRONMENT_IDENTIFIER, environmentProperties);
        updated = true;
      }
    }

    // Update system resource info (CPU, memory, etc)
    Map<String, String> newSystemResourceInfoMap = new SystemResourceInfo().toMap();
    Map<String, String> existingSystemResourceInfoMap =
        znRecord.getMapField(CommonConstants.Helix.Instance.SYSTEM_RESOURCE_INFO_KEY);
    if (!newSystemResourceInfoMap.equals(existingSystemResourceInfoMap)) {
      LOGGER.info("Updating instance: {} with new system resource info: {}", _instanceId, newSystemResourceInfoMap);
      if (existingSystemResourceInfoMap == null) {
        existingSystemResourceInfoMap = newSystemResourceInfoMap;
      } else {
        // existingSystemResourceInfoMap may contains more KV pairs than newSystemResourceInfoMap,
        // we need to preserve those KV pairs and only update the different values.
        for (Map.Entry<String, String> entry : newSystemResourceInfoMap.entrySet()) {
          existingSystemResourceInfoMap.put(entry.getKey(), entry.getValue());
        }
      }
      znRecord.setMapField(Instance.SYSTEM_RESOURCE_INFO_KEY, existingSystemResourceInfoMap);
      updated = true;
    }

    // If 'shutdownInProgress' is not set (new instance, or not shut down properly), set it to prevent brokers routing
    // queries to it before finishing the startup check
    if (!Boolean.parseBoolean(simpleFields.get(Helix.IS_SHUTDOWN_IN_PROGRESS))) {
      LOGGER.info(
          "Updating instance: {} with '{}' to prevent brokers routing queries to it before finishing the startup check",
          _instanceId, Helix.IS_SHUTDOWN_IN_PROGRESS);
      simpleFields.put(Helix.IS_SHUTDOWN_IN_PROGRESS, Boolean.toString(true));
      updated = true;
    }

    if (updated) {
      HelixHelper.updateInstanceConfig(_helixManager, instanceConfig);
    }
  }

  private boolean updatePortIfNeeded(Map<String, String> instanceConfigSimpleFields, String key, int port) {
    String existingPortStr = instanceConfigSimpleFields.get(key);
    if (port > 0) {
      String portStr = Integer.toString(port);
      if (!portStr.equals(existingPortStr)) {
        LOGGER.info("Updating '{}' for instance: {} to: {}", key, _instanceId, port);
        instanceConfigSimpleFields.put(key, portStr);
        return true;
      }
    } else {
      if (existingPortStr != null) {
        LOGGER.info("Removing '{}' from instance: {}", key, _instanceId);
        instanceConfigSimpleFields.remove(key);
        return true;
      }
    }
    return false;
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

    Status serviceStatus = null;
    while (System.currentTimeMillis() < endTimeMs) {
      serviceStatus = ServiceStatus.getServiceStatus(_instanceId);
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

    boolean exitServerOnIncompleteStartup = _serverConf.getProperty(Server.CONFIG_OF_EXIT_ON_SERVICE_STATUS_CHECK_FAILURE,
        Server.DEFAULT_EXIT_ON_SERVICE_STATUS_CHECK_FAILURE);
    if (exitServerOnIncompleteStartup) {
      String errorMessage = String.format("Service status %s has not turned GOOD within %dms: %s. Exiting server.",
          serviceStatus, System.currentTimeMillis() - startTimeMs, ServiceStatus.getStatusDescription());
      throw new IllegalStateException(errorMessage);
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
    LOGGER.info("Starting Pinot server (Version: {})", PinotVersion.VERSION);
    LOGGER.info("Server configs: {}", new PinotAppConfigs(getConfig()).toJSONString());
    long startTimeMs = System.currentTimeMillis();

    // install default SSL context if necessary (even if not force-enabled everywhere)
    TlsConfig tlsDefaults = TlsUtils.extractTlsConfig(_serverConf, Server.SERVER_TLS_PREFIX);
    if (StringUtils.isNotBlank(tlsDefaults.getKeyStorePath()) || StringUtils.isNotBlank(
        tlsDefaults.getTrustStorePath())) {
      LOGGER.info("Installing default SSL context for any client requests");
      TlsUtils.installDefaultSSLSocketFactory(tlsDefaults);
    }

    LOGGER.info("Initializing accessControlFactory");
    String accessControlFactoryClass =
        _serverConf.getProperty(Server.ACCESS_CONTROL_FACTORY_CLASS, Server.DEFAULT_ACCESS_CONTROL_FACTORY_CLASS);
    LOGGER.info("Using class: {} as the AccessControlFactory", accessControlFactoryClass);
    AccessControlFactory accessControlFactory;
    try {
      accessControlFactory = PluginManager.get().createInstance(accessControlFactoryClass);
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while creating new AccessControlFactory instance using class '" + accessControlFactoryClass
              + "'", e);
    }

    LOGGER.info("Initializing server instance and registering state model factory");
    Utils.logVersions();
    ControllerLeaderLocator.create(_helixManager);
    ServerSegmentCompletionProtocolHandler.init(
        _serverConf.subset(SegmentCompletionProtocol.PREFIX_OF_CONFIG_OF_SEGMENT_UPLOADER));
    ServerConf serverConf = new ServerConf(_serverConf);
    _serverInstance = new ServerInstance(serverConf, _helixManager, accessControlFactory);
    ServerMetrics serverMetrics = _serverInstance.getServerMetrics();
    InstanceDataManager instanceDataManager = _serverInstance.getInstanceDataManager();
    instanceDataManager.setSupplierOfIsServerReadyToServeQueries(() -> _isServerReadyToServeQueries);
    // initialize the thread accountant for query killing
    Tracing.ThreadAccountantOps
        .initializeThreadAccountant(_serverConf.subset(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX), _instanceId);
    initSegmentFetcher(_serverConf);
    StateModelFactory<?> stateModelFactory =
        new SegmentOnlineOfflineStateModelFactory(_instanceId, instanceDataManager);
    _helixManager.getStateMachineEngine()
        .registerStateModelFactory(SegmentOnlineOfflineStateModelFactory.getStateModelName(), stateModelFactory);
    // Start the data manager as a pre-connect callback so that it starts after connecting to the ZK in order to access
    // the property store, but before receiving state transitions
    _helixManager.addPreConnectCallback(_serverInstance::startDataManager);

    LOGGER.info("Connecting Helix manager");
    _helixManager.connect();
    _helixAdmin = _helixManager.getClusterManagmentTool();
    updateInstanceConfigIfNeeded(serverConf);

    // Start restlet server for admin API endpoint
    LOGGER.info("Starting server admin application on: {}", ListenerConfigUtil.toString(_listenerConfigs));
    _adminApiApplication = new AdminApiApplication(_serverInstance, accessControlFactory, _serverConf);
    _adminApiApplication.start(_listenerConfigs);

    // Init QueryRewriterFactory
    LOGGER.info("Initializing QueryRewriterFactory");
    QueryRewriterFactory.init(_serverConf.getProperty(Server.CONFIG_OF_SERVER_QUERY_REWRITER_CLASS_NAMES));

    // Register message handler factory
    SegmentMessageHandlerFactory messageHandlerFactory =
        new SegmentMessageHandlerFactory(instanceDataManager, serverMetrics);
    _helixManager.getMessagingService()
        .registerMessageHandlerFactory(Message.MessageType.USER_DEFINE_MSG.toString(), messageHandlerFactory);

    serverMetrics.addCallbackGauge(Helix.INSTANCE_CONNECTED_METRIC_NAME, () -> _helixManager.isConnected() ? 1L : 0L);
    _helixManager.addPreConnectCallback(
        () -> serverMetrics.addMeteredGlobalValue(ServerMeter.HELIX_ZOOKEEPER_RECONNECTS, 1L));

    // Register the service status handler
    registerServiceStatusHandler();

    if (_serverConf.getProperty(Server.CONFIG_OF_STARTUP_ENABLE_SERVICE_STATUS_CHECK,
        Server.DEFAULT_STARTUP_ENABLE_SERVICE_STATUS_CHECK)) {
      long endTimeMs =
          startTimeMs + _serverConf.getProperty(Server.CONFIG_OF_STARTUP_TIMEOUT_MS, Server.DEFAULT_STARTUP_TIMEOUT_MS);
      try {
        startupServiceStatusCheck(endTimeMs);
      } catch (Exception e) {
        LOGGER.error("Caught exception while checking service status. Stopping server.", e);
        // If we exit here, only the _adminApiApplication and _helixManager are initialized, so we only stop them
        _adminApiApplication.stop();
        _helixManager.disconnect();
        throw e;
      }
    }

    preServeQueries();

    // Start the query server after finishing the service status check. If the query server is started before all the
    // segments are loaded, broker might not have finished processing the callback of routing table update, and start
    // querying the server pre-maturely.
    _serverInstance.startQueryServer();
    _helixAdmin.setConfig(_instanceConfigScope,
        Collections.singletonMap(Helix.IS_SHUTDOWN_IN_PROGRESS, Boolean.toString(false)));
    _isServerReadyToServeQueries = true;
    // Throttling for realtime consumption is disabled up to this point to allow maximum consumption during startup time
    RealtimeConsumptionRateManager.getInstance().enableThrottling();

    LOGGER.info("Pinot server ready");

    // Create metrics for mmap stuff
    serverMetrics.addCallbackGauge("memory.directBufferCount", PinotDataBuffer::getDirectBufferCount);
    serverMetrics.addCallbackGauge("memory.directBufferUsage", PinotDataBuffer::getDirectBufferUsage);
    serverMetrics.addCallbackGauge("memory.mmapBufferCount", PinotDataBuffer::getMmapBufferCount);
    serverMetrics.addCallbackGauge("memory.mmapBufferUsage", PinotDataBuffer::getMmapBufferUsage);
    serverMetrics.addCallbackGauge("memory.allocationFailureCount", PinotDataBuffer::getAllocationFailureCount);

    // Track metric for queries disabled
    _serverQueriesDisabledTracker =
        new ServerQueriesDisabledTracker(_helixClusterName, _instanceId, _helixManager, serverMetrics);
    _serverQueriesDisabledTracker.start();

    _realtimeLuceneIndexRefreshState = RealtimeLuceneIndexRefreshState.getInstance();
    _realtimeLuceneIndexRefreshState.start();
  }

  /**
   * Can be overridden to perform operations before server starts serving queries.
   */
  protected void preServeQueries() {
  }

  @Override
  public void stop() {
    LOGGER.info("Shutting down Pinot server");
    long startTimeMs = System.currentTimeMillis();

    _adminApiApplication.startShuttingDown();
    _helixAdmin.setConfig(_instanceConfigScope,
        Collections.singletonMap(Helix.IS_SHUTDOWN_IN_PROGRESS, Boolean.toString(true)));

    long endTimeMs =
        startTimeMs + _serverConf.getProperty(Server.CONFIG_OF_SHUTDOWN_TIMEOUT_MS, Server.DEFAULT_SHUTDOWN_TIMEOUT_MS);
    if (_serverConf.getProperty(Server.CONFIG_OF_SHUTDOWN_ENABLE_QUERY_CHECK,
        Server.DEFAULT_SHUTDOWN_ENABLE_QUERY_CHECK)) {
      shutdownQueryCheck(endTimeMs);
    }
    _helixManager.disconnect();
    _serverInstance.shutDown();
    if (_serverConf.getProperty(Server.CONFIG_OF_SHUTDOWN_ENABLE_RESOURCE_CHECK,
        Server.DEFAULT_SHUTDOWN_ENABLE_RESOURCE_CHECK)) {
      shutdownResourceCheck(endTimeMs);
    }
    if (_serverQueriesDisabledTracker != null) {
      _serverQueriesDisabledTracker.stop();
    }
    if (_realtimeLuceneIndexRefreshState != null) {
      _realtimeLuceneIndexRefreshState.stop();
    }
    try {
      // Close PinotFS after all data managers are shutdown. Otherwise, segments which are being committed will not
      // be uploaded to the deep-store.
      LOGGER.info("Closing PinotFS classes");
      PinotFSFactory.shutdown();
    } catch (IOException e) {
      LOGGER.warn("Caught exception closing PinotFS classes", e);
    }
    LOGGER.info("Deregistering service status handler");
    ServiceStatus.removeServiceStatusCallback(_instanceId);
    _adminApiApplication.stop();
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
      // Ensure we wait the full noQueryTimeMs since the start of shutdown
      long noQueryTimeMs = currentTimeMs - Math.max(startTimeMs, _serverInstance.getLatestQueryTime());
      if (noQueryTimeMs >= noQueryThresholdMs) {
        LOGGER.info("No query received within {}ms (larger than the threshold: {}ms), mark it as no incoming queries",
            noQueryTimeMs, noQueryThresholdMs);
        noIncomingQueries = true;
        break;
      }
      // Otherwise sleep the difference, or use shutdown execution timeout if it's smaller
      long sleepTimeMs = Math.min(noQueryThresholdMs - noQueryTimeMs, endTimeMs - currentTimeMs);
      LOGGER.info(
          "Sleep for {}ms as there are still incoming queries (no query time: {}ms is smaller than the threshold: "
              + "{}ms)", sleepTimeMs, noQueryTimeMs, noQueryThresholdMs);
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

  @VisibleForTesting
  public ServerInstance getServerInstance() {
    return _serverInstance;
  }

  /**
   * Initialize the components to download segments from deep store. They used to be
   * initialized in SegmentFetcherAndLoader, which has been removed to consolidate
   * segment download functionality for both Offline and Realtime tables. So those
   * components are initialized where SegmentFetcherAndLoader was initialized.
   */
  private void initSegmentFetcher(PinotConfiguration config)
      throws Exception {
    PinotConfiguration pinotFSConfig = config.subset(CommonConstants.Server.PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY);
    PinotFSFactory.init(pinotFSConfig);

    PinotConfiguration segmentFetcherFactoryConfig =
        config.subset(CommonConstants.Server.PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY);
    SegmentFetcherFactory.init(segmentFetcherFactoryConfig);

    PinotConfiguration pinotCrypterConfig = config.subset(CommonConstants.Server.PREFIX_OF_CONFIG_OF_PINOT_CRYPTER);
    PinotCrypterFactory.init(pinotCrypterConfig);
  }
}
