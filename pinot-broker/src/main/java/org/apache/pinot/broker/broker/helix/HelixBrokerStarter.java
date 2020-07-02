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
package org.apache.pinot.broker.broker.helix;

import com.google.common.collect.ImmutableList;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.broker.BrokerAdminApiApplication;
import org.apache.pinot.broker.queryquota.HelixExternalViewBasedQueryQuotaManager;
import org.apache.pinot.broker.requesthandler.BrokerRequestHandler;
import org.apache.pinot.broker.requesthandler.SingleConnectionBrokerRequestHandler;
import org.apache.pinot.broker.routing.RoutingManager;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.MetricsHelper;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.CommonConstants.Broker;
import org.apache.pinot.common.utils.CommonConstants.Helix;
import org.apache.pinot.common.utils.NetUtil;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("unused")
public class HelixBrokerStarter {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixBrokerStarter.class);

  private final Configuration _brokerConf;
  private final String _clusterName;
  private final String _zkServers;
  private final String _brokerId;

  private final List<ClusterChangeHandler> _externalViewChangeHandlers = new ArrayList<>();
  private final List<ClusterChangeHandler> _instanceConfigChangeHandlers = new ArrayList<>();
  private final List<ClusterChangeHandler> _liveInstanceChangeHandlers = new ArrayList<>();

  // Spectator Helix manager handles the custom change listeners, properties read/write
  private HelixManager _spectatorHelixManager;
  private HelixAdmin _helixAdmin;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private HelixDataAccessor _helixDataAccessor;

  private MetricsRegistry _metricsRegistry;
  private BrokerMetrics _brokerMetrics;
  private RoutingManager _routingManager;
  private AccessControlFactory _accessControlFactory;
  private BrokerRequestHandler _brokerRequestHandler;
  private BrokerAdminApiApplication _brokerAdminApplication;
  private ClusterChangeMediator _clusterChangeMediator;

  // Participant Helix manager handles Helix functionality such as state transitions and messages
  private HelixManager _participantHelixManager;

  public HelixBrokerStarter(Configuration brokerConf, String clusterName, String zkServer)
      throws Exception {
    this(brokerConf, clusterName, zkServer, null);
  }

  public HelixBrokerStarter(Configuration brokerConf, String clusterName, String zkServer, @Nullable String brokerHost)
      throws Exception {
    _brokerConf = brokerConf;
    setupHelixSystemProperties();

    _clusterName = clusterName;

    // Remove all white-spaces from the list of zkServers (if any).
    _zkServers = zkServer.replaceAll("\\s+", "");

    if (brokerHost == null) {
      brokerHost = _brokerConf.getBoolean(CommonConstants.Helix.SET_INSTANCE_ID_TO_HOSTNAME_KEY, false) ? NetUtil
          .getHostnameOrAddress() : NetUtil.getHostAddress();
    }
    _brokerId = _brokerConf.getString(Helix.Instance.INSTANCE_ID_KEY,
        Helix.PREFIX_OF_BROKER_INSTANCE + brokerHost + "_" + _brokerConf
            .getInt(Helix.KEY_OF_BROKER_QUERY_PORT, Helix.DEFAULT_BROKER_QUERY_PORT));
    _brokerConf.addProperty(Broker.CONFIG_OF_BROKER_ID, _brokerId);
  }

  private void setupHelixSystemProperties() {
    // NOTE: Helix will disconnect the manager and disable the instance if it detects flapping (too frequent disconnect
    // from ZooKeeper). Setting flapping time window to a small value can avoid this from happening. Helix ignores the
    // non-positive value, so set the default value as 1.
    System.setProperty(SystemPropertyKeys.FLAPPING_TIME_WINDOW,
        _brokerConf.getString(Helix.CONFIG_OF_BROKER_FLAPPING_TIME_WINDOW_MS, Helix.DEFAULT_FLAPPING_TIME_WINDOW_MS));
  }

  /**
   * Adds an external view change handler to handle Helix external view change callbacks.
   * <p>NOTE: all change handlers will be run in a single thread, so any slow change handler can block other change
   * handlers from running. For slow change handler, make it asynchronous.
   */
  public void addExternalViewChangeHandler(ClusterChangeHandler externalViewChangeHandler) {
    _externalViewChangeHandlers.add(externalViewChangeHandler);
  }

  /**
   * Adds an instance config change handler to handle Helix instance config change callbacks.
   * <p>NOTE: all change handlers will be run in a single thread, so any slow change handler can block other change
   * handlers from running. For slow change handler, make it asynchronous.
   */
  public void addInstanceConfigChangeHandler(ClusterChangeHandler instanceConfigChangeHandler) {
    _instanceConfigChangeHandlers.add(instanceConfigChangeHandler);
  }

  /**
   * Adds a live instance change handler to handle Helix live instance change callbacks.
   * <p>NOTE: all change handlers will be run in a single thread, so any slow change handler can block other change
   * handlers from running. For slow change handler, make it asynchronous.
   */
  public void addLiveInstanceChangeHandler(ClusterChangeHandler liveInstanceChangeHandler) {
    _liveInstanceChangeHandlers.add(liveInstanceChangeHandler);
  }

  public void start()
      throws Exception {
    LOGGER.info("Starting Pinot broker");
    Utils.logVersions();

    LOGGER.info("Connecting spectator Helix manager");
    _spectatorHelixManager =
        HelixManagerFactory.getZKHelixManager(_clusterName, _brokerId, InstanceType.SPECTATOR, _zkServers);
    _spectatorHelixManager.connect();
    _helixAdmin = _spectatorHelixManager.getClusterManagmentTool();
    _propertyStore = _spectatorHelixManager.getHelixPropertyStore();
    _helixDataAccessor = _spectatorHelixManager.getHelixDataAccessor();
    // Fetch cluster level config from ZK
    ConfigAccessor configAccessor = _spectatorHelixManager.getConfigAccessor();
    HelixConfigScope helixConfigScope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(_clusterName).build();
    String enableCaseInsensitivePql = configAccessor.get(helixConfigScope, Helix.ENABLE_CASE_INSENSITIVE_PQL_KEY);
    _brokerConf.setProperty(Helix.ENABLE_CASE_INSENSITIVE_PQL_KEY, Boolean.valueOf(enableCaseInsensitivePql));
    String enableQueryLimitOverride =
        configAccessor.get(helixConfigScope, Broker.CONFIG_OF_ENABLE_QUERY_LIMIT_OVERRIDE);
    _brokerConf.setProperty(Broker.CONFIG_OF_ENABLE_QUERY_LIMIT_OVERRIDE, Boolean.valueOf(enableQueryLimitOverride));

    LOGGER.info("Setting up broker request handler");
    // Set up metric registry and broker metrics
    _metricsRegistry = new MetricsRegistry();
    MetricsHelper.initializeMetrics(_brokerConf.subset(Broker.METRICS_CONFIG_PREFIX));
    MetricsHelper.registerMetricsRegistry(_metricsRegistry);
    _brokerMetrics = new BrokerMetrics(
        _brokerConf.getString(Broker.CONFIG_OF_METRICS_NAME_PREFIX, Broker.DEFAULT_METRICS_NAME_PREFIX),
        _metricsRegistry,
        !_brokerConf.getBoolean(Broker.CONFIG_OF_ENABLE_TABLE_LEVEL_METRICS, !Broker.DEFAULT_METRICS_GLOBAL_ENABLED));
    _brokerMetrics.initializeGlobalMeters();
    // Set up request handling classes
    _routingManager = new RoutingManager(_brokerMetrics);
    _routingManager.init(_spectatorHelixManager);
    _accessControlFactory = AccessControlFactory.loadFactory(_brokerConf.subset(Broker.ACCESS_CONTROL_CONFIG_PREFIX));
    HelixExternalViewBasedQueryQuotaManager queryQuotaManager =
        new HelixExternalViewBasedQueryQuotaManager(_brokerMetrics);
    queryQuotaManager.init(_spectatorHelixManager);
    _brokerRequestHandler =
        new SingleConnectionBrokerRequestHandler(_brokerConf, _routingManager, _accessControlFactory, queryQuotaManager,
            _brokerMetrics, _propertyStore);

    int brokerQueryPort = _brokerConf.getInt(Helix.KEY_OF_BROKER_QUERY_PORT, Helix.DEFAULT_BROKER_QUERY_PORT);
    LOGGER.info("Starting broker admin application on port: {}", brokerQueryPort);
    _brokerAdminApplication = new BrokerAdminApiApplication(_routingManager, _brokerRequestHandler, _brokerMetrics);
    _brokerAdminApplication.start(brokerQueryPort);

    LOGGER.info("Initializing cluster change mediator");
    for (ClusterChangeHandler externalViewChangeHandler : _externalViewChangeHandlers) {
      externalViewChangeHandler.init(_spectatorHelixManager);
    }
    _externalViewChangeHandlers.add(_routingManager);
    _externalViewChangeHandlers.add(queryQuotaManager);
    for (ClusterChangeHandler instanceConfigChangeHandler : _instanceConfigChangeHandlers) {
      instanceConfigChangeHandler.init(_spectatorHelixManager);
    }
    _instanceConfigChangeHandlers.add(_routingManager);
    for (ClusterChangeHandler liveInstanceChangeHandler : _liveInstanceChangeHandlers) {
      liveInstanceChangeHandler.init(_spectatorHelixManager);
    }
    Map<ChangeType, List<ClusterChangeHandler>> clusterChangeHandlersMap = new HashMap<>();
    clusterChangeHandlersMap.put(ChangeType.EXTERNAL_VIEW, _externalViewChangeHandlers);
    clusterChangeHandlersMap.put(ChangeType.INSTANCE_CONFIG, _instanceConfigChangeHandlers);
    if (!_liveInstanceChangeHandlers.isEmpty()) {
      clusterChangeHandlersMap.put(ChangeType.LIVE_INSTANCE, _liveInstanceChangeHandlers);
    }
    _clusterChangeMediator = new ClusterChangeMediator(clusterChangeHandlersMap, _brokerMetrics);
    _clusterChangeMediator.start();
    _spectatorHelixManager.addExternalViewChangeListener(_clusterChangeMediator);
    _spectatorHelixManager.addInstanceConfigChangeListener(_clusterChangeMediator);
    if (!_liveInstanceChangeHandlers.isEmpty()) {
      _spectatorHelixManager.addLiveInstanceChangeListener(_clusterChangeMediator);
    }

    LOGGER.info("Connecting participant Helix manager");
    _participantHelixManager =
        HelixManagerFactory.getZKHelixManager(_clusterName, _brokerId, InstanceType.PARTICIPANT, _zkServers);
    // Register state model factory
    _participantHelixManager.getStateMachineEngine()
        .registerStateModelFactory(BrokerResourceOnlineOfflineStateModelFactory.getStateModelDef(),
            new BrokerResourceOnlineOfflineStateModelFactory(_propertyStore, _helixDataAccessor, _routingManager,
                queryQuotaManager));
    // Register user-define message handler factory
    _participantHelixManager.getMessagingService()
        .registerMessageHandlerFactory(Message.MessageType.USER_DEFINE_MSG.toString(),
            new BrokerUserDefinedMessageHandlerFactory(_routingManager, queryQuotaManager));
    _participantHelixManager.connect();
    addInstanceTagIfNeeded();
    _brokerMetrics
        .addCallbackGauge(Helix.INSTANCE_CONNECTED_METRIC_NAME, () -> _participantHelixManager.isConnected() ? 1L : 0L);
    _participantHelixManager
        .addPreConnectCallback(() -> _brokerMetrics.addMeteredGlobalValue(BrokerMeter.HELIX_ZOOKEEPER_RECONNECTS, 1L));

    // Register the service status handler
    registerServiceStatusHandler();

    LOGGER.info("Finish starting Pinot broker");
  }

  /**
   * Fetches the resources to monitor and registers the {@link org.apache.pinot.common.utils.ServiceStatus.ServiceStatusCallback}s
   */
  private void registerServiceStatusHandler() {
    List<String> resourcesToMonitor = new ArrayList<>(1);
    IdealState brokerResourceIdealState =
        _helixAdmin.getResourceIdealState(_clusterName, Helix.BROKER_RESOURCE_INSTANCE);
    if (brokerResourceIdealState != null && brokerResourceIdealState.isEnabled()) {
      for (String partitionName : brokerResourceIdealState.getPartitionSet()) {
        if (brokerResourceIdealState.getInstanceSet(partitionName).contains(_brokerId)) {
          resourcesToMonitor.add(Helix.BROKER_RESOURCE_INSTANCE);
          break;
        }
      }
    }

    double minResourcePercentForStartup = _brokerConf.getDouble(Broker.CONFIG_OF_BROKER_MIN_RESOURCE_PERCENT_FOR_START,
        Broker.DEFAULT_BROKER_MIN_RESOURCE_PERCENT_FOR_START);

    LOGGER.info("Registering service status handler");
    ServiceStatus.setServiceStatusCallback(new ServiceStatus.MultipleCallbackServiceStatusCallback(ImmutableList
        .of(new ServiceStatus.IdealStateAndCurrentStateMatchServiceStatusCallback(_participantHelixManager,
                _clusterName, _brokerId, resourcesToMonitor, minResourcePercentForStartup),
            new ServiceStatus.IdealStateAndExternalViewMatchServiceStatusCallback(_participantHelixManager,
                _clusterName, _brokerId, resourcesToMonitor, minResourcePercentForStartup))));
  }

  private void addInstanceTagIfNeeded() {
    InstanceConfig instanceConfig =
        _helixDataAccessor.getProperty(_helixDataAccessor.keyBuilder().instanceConfig(_brokerId));
    List<String> instanceTags = instanceConfig.getTags();
    if (instanceTags == null || instanceTags.isEmpty()) {
      if (ZKMetadataProvider.getClusterTenantIsolationEnabled(_propertyStore)) {
        _helixAdmin.addInstanceTag(_clusterName, _brokerId, TagNameUtils.getBrokerTagForTenant(null));
      } else {
        _helixAdmin.addInstanceTag(_clusterName, _brokerId, Helix.UNTAGGED_BROKER_INSTANCE);
      }
    }
  }

  public void shutdown() {
    LOGGER.info("Shutting down Pinot broker");

    LOGGER.info("Disconnecting participant Helix manager");
    _participantHelixManager.disconnect();

    LOGGER.info("Stopping cluster change mediator");
    _clusterChangeMediator.stop();

    // Delay shutdown of request handler so that the pending queries can be finished. The participant Helix manager has
    // been disconnected, so instance should disappear from ExternalView soon and stop getting new queries.
    long delayShutdownTimeMs =
        _brokerConf.getLong(Broker.CONFIG_OF_DELAY_SHUTDOWN_TIME_MS, Broker.DEFAULT_DELAY_SHUTDOWN_TIME_MS);
    LOGGER
        .info("Wait for {}ms before shutting down request handler to finish the pending queries", delayShutdownTimeMs);
    try {
      Thread.sleep(delayShutdownTimeMs);
    } catch (Exception e) {
      LOGGER.error("Caught exception while waiting for shutdown delay of {}ms", delayShutdownTimeMs, e);
    }

    LOGGER.info("Shutting down request handler and broker admin application");
    _brokerRequestHandler.shutDown();
    _brokerAdminApplication.stop();

    LOGGER.info("Disconnecting spectator Helix manager");
    _spectatorHelixManager.disconnect();

    LOGGER.info("Finish shutting down Pinot broker");
  }

  public HelixManager getSpectatorHelixManager() {
    return _spectatorHelixManager;
  }

  public MetricsRegistry getMetricsRegistry() {
    return _metricsRegistry;
  }

  public BrokerMetrics getBrokerMetrics() {
    return _brokerMetrics;
  }

  public RoutingManager getRoutingManager() {
    return _routingManager;
  }

  public AccessControlFactory getAccessControlFactory() {
    return _accessControlFactory;
  }

  public BrokerRequestHandler getBrokerRequestHandler() {
    return _brokerRequestHandler;
  }

  public static HelixBrokerStarter getDefault()
      throws Exception {
    Configuration brokerConf = new BaseConfiguration();
    int port = 5001;
    brokerConf.addProperty(Helix.KEY_OF_BROKER_QUERY_PORT, port);
    brokerConf.addProperty(Broker.CONFIG_OF_BROKER_TIMEOUT_MS, 60 * 1000L);
    return new HelixBrokerStarter(brokerConf, "quickstart", "localhost:2122");
  }

  public static void main(String[] args)
      throws Exception {
    getDefault().start();
  }
}
