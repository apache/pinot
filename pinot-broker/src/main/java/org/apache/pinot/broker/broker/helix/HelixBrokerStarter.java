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
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.broker.BrokerServerBuilder;
import org.apache.pinot.broker.queryquota.HelixExternalViewBasedQueryQuotaManager;
import org.apache.pinot.broker.requesthandler.BrokerRequestHandler;
import org.apache.pinot.broker.routing.HelixExternalViewBasedRouting;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.config.TagNameUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.CommonConstants.Broker;
import org.apache.pinot.common.utils.CommonConstants.Helix;
import org.apache.pinot.common.utils.NetUtil;
import org.apache.pinot.common.utils.ServiceStatus;
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

  // Cluster change handlers
  private HelixExternalViewBasedRouting _helixExternalViewBasedRouting;
  private HelixExternalViewBasedQueryQuotaManager _helixExternalViewBasedQueryQuotaManager;
  private ClusterChangeMediator _clusterChangeMediator;

  private BrokerServerBuilder _brokerServerBuilder;

  // Participant Helix manager handles Helix functionality such as state transitions and messages
  private HelixManager _participantHelixManager;
  private TimeboundaryRefreshMessageHandlerFactory _tbiMessageHandler;

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

    // Connect the spectator Helix manager
    LOGGER.info("Connecting spectator Helix manager");
    _spectatorHelixManager =
        HelixManagerFactory.getZKHelixManager(_clusterName, _brokerId, InstanceType.SPECTATOR, _zkServers);
    _spectatorHelixManager.connect();
    _helixAdmin = _spectatorHelixManager.getClusterManagmentTool();
    _propertyStore = _spectatorHelixManager.getHelixPropertyStore();
    _helixDataAccessor = _spectatorHelixManager.getHelixDataAccessor();
    ConfigAccessor configAccessor = _spectatorHelixManager.getConfigAccessor();

    // Set up the broker server builder
    LOGGER.info("Setting up broker server builder");
    _helixExternalViewBasedRouting =
        new HelixExternalViewBasedRouting(_brokerConf.subset(Broker.ROUTING_TABLE_CONFIG_PREFIX));
    _helixExternalViewBasedRouting.init(_spectatorHelixManager);
    _helixExternalViewBasedQueryQuotaManager = new HelixExternalViewBasedQueryQuotaManager();
    _helixExternalViewBasedQueryQuotaManager.init(_spectatorHelixManager);

    //should we enable case_insensitive_pql
    HelixConfigScope helixConfigScope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(_clusterName).build();
    String enableCaseInsensitivePql = configAccessor.get(helixConfigScope, Helix.ENABLE_CASE_INSENSITIVE_PQL_KEY);
    _brokerConf.setProperty(Helix.ENABLE_CASE_INSENSITIVE_PQL_KEY, Boolean.valueOf(enableCaseInsensitivePql));
    String enableBrokerQueryLimitOverride = configAccessor.get(helixConfigScope, Broker.CONFIG_OF_ENABLE_QUERY_LIMIT_OVERRIDE);
    _brokerConf.setProperty(Broker.CONFIG_OF_ENABLE_QUERY_LIMIT_OVERRIDE, Boolean.valueOf(enableBrokerQueryLimitOverride));
    _brokerServerBuilder = new BrokerServerBuilder(_brokerConf, _helixExternalViewBasedRouting,
        _helixExternalViewBasedRouting.getTimeBoundaryService(), _helixExternalViewBasedQueryQuotaManager, _propertyStore);
    BrokerRequestHandler brokerRequestHandler = _brokerServerBuilder.getBrokerRequestHandler();
    BrokerMetrics brokerMetrics = _brokerServerBuilder.getBrokerMetrics();
    _helixExternalViewBasedRouting.setBrokerMetrics(brokerMetrics);
    _helixExternalViewBasedQueryQuotaManager.setBrokerMetrics(brokerMetrics);
    _brokerServerBuilder.start();

    // Initialize the cluster change mediator
    LOGGER.info("Initializing cluster change mediator");
    for (ClusterChangeHandler externalViewChangeHandler : _externalViewChangeHandlers) {
      externalViewChangeHandler.init(_spectatorHelixManager);
    }
    _externalViewChangeHandlers.add(_helixExternalViewBasedRouting);
    _externalViewChangeHandlers.add(_helixExternalViewBasedQueryQuotaManager);
    for (ClusterChangeHandler instanceConfigChangeHandler : _instanceConfigChangeHandlers) {
      instanceConfigChangeHandler.init(_spectatorHelixManager);
    }
    _instanceConfigChangeHandlers.add(_helixExternalViewBasedRouting);
    for (ClusterChangeHandler liveInstanceChangeHandler : _liveInstanceChangeHandlers) {
      liveInstanceChangeHandler.init(_spectatorHelixManager);
    }
    Map<ChangeType, List<ClusterChangeHandler>> clusterChangeHandlersMap = new HashMap<>();
    clusterChangeHandlersMap.put(ChangeType.EXTERNAL_VIEW, _externalViewChangeHandlers);
    clusterChangeHandlersMap.put(ChangeType.INSTANCE_CONFIG, _instanceConfigChangeHandlers);
    if (!_liveInstanceChangeHandlers.isEmpty()) {
      clusterChangeHandlersMap.put(ChangeType.LIVE_INSTANCE, _liveInstanceChangeHandlers);
    }
    _clusterChangeMediator = new ClusterChangeMediator(clusterChangeHandlersMap, brokerMetrics);
    _clusterChangeMediator.start();
    _spectatorHelixManager.addExternalViewChangeListener(_clusterChangeMediator);
    _spectatorHelixManager.addInstanceConfigChangeListener(_clusterChangeMediator);
    if (!_liveInstanceChangeHandlers.isEmpty()) {
      _spectatorHelixManager.addLiveInstanceChangeListener(_clusterChangeMediator);
    }

    // Connect the participant Helix manager
    LOGGER.info("Connecting participant Helix manager");
    _participantHelixManager =
        HelixManagerFactory.getZKHelixManager(_clusterName, _brokerId, InstanceType.PARTICIPANT, _zkServers);
    StateMachineEngine stateMachineEngine = _participantHelixManager.getStateMachineEngine();
    StateModelFactory<?> stateModelFactory =
        new BrokerResourceOnlineOfflineStateModelFactory(_propertyStore, _helixDataAccessor,
            _helixExternalViewBasedRouting, _helixExternalViewBasedQueryQuotaManager);
    stateMachineEngine
        .registerStateModelFactory(BrokerResourceOnlineOfflineStateModelFactory.getStateModelDef(), stateModelFactory);
    _participantHelixManager.connect();
    _tbiMessageHandler = new TimeboundaryRefreshMessageHandlerFactory(_helixExternalViewBasedRouting, _brokerConf
        .getLong(Broker.CONFIG_OF_BROKER_REFRESH_TIMEBOUNDARY_INFO_SLEEP_INTERVAL,
            Broker.DEFAULT_BROKER_REFRESH_TIMEBOUNDARY_INFO_SLEEP_INTERVAL_MS));
    _participantHelixManager.getMessagingService()
        .registerMessageHandlerFactory(Message.MessageType.USER_DEFINE_MSG.toString(), _tbiMessageHandler);
    addInstanceTagIfNeeded();
    brokerMetrics
        .addCallbackGauge(Helix.INSTANCE_CONNECTED_METRIC_NAME, () -> _participantHelixManager.isConnected() ? 1L : 0L);
    _participantHelixManager
        .addPreConnectCallback(() -> brokerMetrics.addMeteredGlobalValue(BrokerMeter.HELIX_ZOOKEEPER_RECONNECTS, 1L));

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

    if (_tbiMessageHandler != null) {
      LOGGER.info("Shutting down time boundary info refresh message handler");
      _tbiMessageHandler.shutdown();
    }

    if (_participantHelixManager != null) {
      LOGGER.info("Disconnecting participant Helix manager");
      _participantHelixManager.disconnect();
    }

    if (_clusterChangeMediator != null) {
      LOGGER.info("Stopping cluster change mediator");
      _clusterChangeMediator.stop();
    }

    if (_brokerServerBuilder != null) {
      LOGGER.info("Stopping broker server builder");
      _brokerServerBuilder.stop();
    }

    if (_spectatorHelixManager != null) {
      LOGGER.info("Disconnecting spectator Helix manager");
      _spectatorHelixManager.disconnect();
    }

    LOGGER.info("Finish shutting down Pinot broker");
  }

  public AccessControlFactory getAccessControlFactory() {
    return _brokerServerBuilder.getAccessControlFactory();
  }

  public HelixManager getSpectatorHelixManager() {
    return _spectatorHelixManager;
  }

  public HelixExternalViewBasedRouting getHelixExternalViewBasedRouting() {
    return _helixExternalViewBasedRouting;
  }

  public BrokerServerBuilder getBrokerServerBuilder() {
    return _brokerServerBuilder;
  }

  public MetricsRegistry getMetricsRegistry() {
    return _brokerServerBuilder.getMetricsRegistry();
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
