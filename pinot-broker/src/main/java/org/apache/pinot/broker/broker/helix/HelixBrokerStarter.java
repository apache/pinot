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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.broker.BrokerServerBuilder;
import org.apache.pinot.broker.queryquota.TableQueryQuotaManager;
import org.apache.pinot.broker.routing.HelixExternalViewBasedRouting;
import org.apache.pinot.common.config.TagNameUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.NetUtil;
import org.apache.pinot.common.utils.ServiceStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helix Broker Startable
 *
 *
 */
public class HelixBrokerStarter {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixBrokerStarter.class);
  private static final String ROUTING_TABLE_PARAMS_SUBSET_KEY = "pinot.broker.routing.table";

  // Spectator Helix manager handles the custom change listeners, properties read/write
  private final HelixManager _spectatorHelixManager;
  // Participant Helix manager handles Helix functionality such as state transitions and messages
  private final HelixManager _participantHelixManager;

  private final Configuration _brokerConf;
  private final HelixAdmin _helixAdmin;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final HelixDataAccessor _helixDataAccessor;
  private final HelixExternalViewBasedRouting _helixExternalViewBasedRouting;
  private final BrokerServerBuilder _brokerServerBuilder;
  private final LiveInstanceChangeHandler _liveInstanceChangeHandler;
  private final MetricsRegistry _metricsRegistry;
  private final TableQueryQuotaManager _tableQueryQuotaManager;
  private final ClusterChangeMediator _clusterChangeMediator;
  private final TimeboundaryRefreshMessageHandlerFactory _tbiMessageHandler;

  // Set after broker is started, which is actually in the constructor.
  private AccessControlFactory _accessControlFactory;

  public HelixBrokerStarter(String helixClusterName, String zkServer, Configuration brokerConf)
      throws Exception {
    this(null, helixClusterName, zkServer, brokerConf);
  }

  public HelixBrokerStarter(@Nullable String brokerHost, String helixClusterName, String zkServer,
      Configuration brokerConf)
      throws Exception {
    LOGGER.info("Starting Pinot broker");

    _brokerConf = brokerConf;

    if (brokerHost == null) {
      brokerHost = NetUtil.getHostAddress();
    }

    String brokerId = _brokerConf.getString(CommonConstants.Helix.Instance.INSTANCE_ID_KEY,
        CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE + brokerHost + "_" + _brokerConf
            .getInt(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT, CommonConstants.Helix.DEFAULT_BROKER_QUERY_PORT));

    _brokerConf.addProperty(CommonConstants.Broker.CONFIG_OF_BROKER_ID, brokerId);
    setupHelixSystemProperties();

    // Remove all white-spaces from the list of zkServers (if any).
    String zkServers = zkServer.replaceAll("\\s+", "");

    LOGGER.info("Connecting Helix components");
    // Connect spectator Helix manager.
    _spectatorHelixManager =
        HelixManagerFactory.getZKHelixManager(helixClusterName, brokerId, InstanceType.SPECTATOR, zkServers);
    _spectatorHelixManager.connect();
    _helixAdmin = _spectatorHelixManager.getClusterManagmentTool();
    _propertyStore = _spectatorHelixManager.getHelixPropertyStore();
    _helixDataAccessor = _spectatorHelixManager.getHelixDataAccessor();
    _helixExternalViewBasedRouting = new HelixExternalViewBasedRouting(_propertyStore, _spectatorHelixManager,
        brokerConf.subset(ROUTING_TABLE_PARAMS_SUBSET_KEY));
    _tableQueryQuotaManager = new TableQueryQuotaManager(_spectatorHelixManager);
    _liveInstanceChangeHandler = new LiveInstanceChangeHandler(_spectatorHelixManager);
    _brokerServerBuilder = startBroker(_brokerConf);
    _metricsRegistry = _brokerServerBuilder.getMetricsRegistry();

    // Initialize cluster change mediator
    Map<ChangeType, List<ClusterChangeHandler>> changeHandlersMap = new HashMap<>();
    List<ClusterChangeHandler> externalViewChangeHandlers = new ArrayList<>();
    externalViewChangeHandlers.add(_helixExternalViewBasedRouting);
    externalViewChangeHandlers.add(_tableQueryQuotaManager);
    externalViewChangeHandlers.addAll(getCustomExternalViewChangeHandlers(_spectatorHelixManager));
    changeHandlersMap.put(ChangeType.EXTERNAL_VIEW, externalViewChangeHandlers);
    List<ClusterChangeHandler> instanceConfigChangeHandlers = new ArrayList<>();
    instanceConfigChangeHandlers.add(_helixExternalViewBasedRouting);
    instanceConfigChangeHandlers.addAll(getCustomInstanceConfigChangeHandlers(_spectatorHelixManager));
    changeHandlersMap.put(ChangeType.INSTANCE_CONFIG, instanceConfigChangeHandlers);
    List<ClusterChangeHandler> liveInstanceChangeHandler = new ArrayList<>();
    liveInstanceChangeHandler.add(_liveInstanceChangeHandler);
    liveInstanceChangeHandler.addAll(getCustomLiveInstanceChangeHandlers(_spectatorHelixManager));
    changeHandlersMap.put(ChangeType.LIVE_INSTANCE, liveInstanceChangeHandler);
    _clusterChangeMediator = new ClusterChangeMediator(changeHandlersMap, _brokerServerBuilder.getBrokerMetrics());
    _clusterChangeMediator.start();
    _spectatorHelixManager.addExternalViewChangeListener(_clusterChangeMediator);
    _spectatorHelixManager.addInstanceConfigChangeListener(_clusterChangeMediator);
    _spectatorHelixManager.addLiveInstanceChangeListener(_clusterChangeMediator);

    // Connect participant Helix manager.
    _participantHelixManager =
        HelixManagerFactory.getZKHelixManager(helixClusterName, brokerId, InstanceType.PARTICIPANT, zkServers);
    StateMachineEngine stateMachineEngine = _participantHelixManager.getStateMachineEngine();
    StateModelFactory<?> stateModelFactory =
        new BrokerResourceOnlineOfflineStateModelFactory(_propertyStore, _helixDataAccessor,
            _helixExternalViewBasedRouting, _tableQueryQuotaManager);
    stateMachineEngine
        .registerStateModelFactory(BrokerResourceOnlineOfflineStateModelFactory.getStateModelDef(), stateModelFactory);
    _participantHelixManager.connect();
    _tbiMessageHandler = new TimeboundaryRefreshMessageHandlerFactory(_helixExternalViewBasedRouting, _brokerConf
        .getLong(CommonConstants.Broker.CONFIG_OF_BROKER_REFRESH_TIMEBOUNDARY_INFO_SLEEP_INTERVAL,
            CommonConstants.Broker.DEFAULT_BROKER_REFRESH_TIMEBOUNDARY_INFO_SLEEP_INTERVAL_MS));
    _participantHelixManager.getMessagingService()
        .registerMessageHandlerFactory(Message.MessageType.USER_DEFINE_MSG.toString(), _tbiMessageHandler);

    addInstanceTagIfNeeded(helixClusterName, brokerId);

    // Register the service status handler
    double minResourcePercentForStartup = _brokerConf
        .getDouble(CommonConstants.Broker.CONFIG_OF_BROKER_MIN_RESOURCE_PERCENT_FOR_START,
            CommonConstants.Broker.DEFAULT_BROKER_MIN_RESOURCE_PERCENT_FOR_START);
    ServiceStatus.setServiceStatusCallback(new ServiceStatus.MultipleCallbackServiceStatusCallback(ImmutableList
        .of(new ServiceStatus.IdealStateAndCurrentStateMatchServiceStatusCallback(_participantHelixManager,
                helixClusterName, brokerId, minResourcePercentForStartup),
            new ServiceStatus.IdealStateAndExternalViewMatchServiceStatusCallback(_participantHelixManager,
                helixClusterName, brokerId, minResourcePercentForStartup))));

    _brokerServerBuilder.getBrokerMetrics()
        .addCallbackGauge("helix.connected", () -> _participantHelixManager.isConnected() ? 1L : 0L);

    _participantHelixManager.addPreConnectCallback(() -> _brokerServerBuilder.getBrokerMetrics()
        .addMeteredGlobalValue(BrokerMeter.HELIX_ZOOKEEPER_RECONNECTS, 1L));
  }

  private void setupHelixSystemProperties() {
    // NOTE: Helix will disconnect the manager and disable the instance if it detects flapping (too frequent disconnect
    // from ZooKeeper). Setting flapping time window to a small value can avoid this from happening. Helix ignores the
    // non-positive value, so set the default value as 1.
    System.setProperty(SystemPropertyKeys.FLAPPING_TIME_WINDOW, _brokerConf
        .getString(CommonConstants.Helix.CONFIG_OF_BROKER_FLAPPING_TIME_WINDOW_MS,
            CommonConstants.Helix.DEFAULT_FLAPPING_TIME_WINDOW_MS));
  }

  private void addInstanceTagIfNeeded(String clusterName, String instanceName) {
    InstanceConfig instanceConfig =
        _helixDataAccessor.getProperty(_helixDataAccessor.keyBuilder().instanceConfig(instanceName));
    List<String> instanceTags = instanceConfig.getTags();
    if (instanceTags == null || instanceTags.isEmpty()) {
      if (ZKMetadataProvider.getClusterTenantIsolationEnabled(_propertyStore)) {
        _helixAdmin.addInstanceTag(clusterName, instanceName,
            TagNameUtils.getBrokerTagForTenant(TagNameUtils.DEFAULT_TENANT_NAME));
      } else {
        _helixAdmin.addInstanceTag(clusterName, instanceName, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
      }
    }
  }

  private BrokerServerBuilder startBroker(Configuration config) {
    BrokerServerBuilder brokerServerBuilder = new BrokerServerBuilder(config, _helixExternalViewBasedRouting,
        _helixExternalViewBasedRouting.getTimeBoundaryService(), _liveInstanceChangeHandler, _tableQueryQuotaManager);
    _accessControlFactory = brokerServerBuilder.getAccessControlFactory();
    _helixExternalViewBasedRouting.setBrokerMetrics(brokerServerBuilder.getBrokerMetrics());
    _tableQueryQuotaManager.setBrokerMetrics(brokerServerBuilder.getBrokerMetrics());
    brokerServerBuilder.start();

    LOGGER.info("Pinot broker ready and listening on port {} for API requests",
        config.getProperty("pinot.broker.client.queryPort"));

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          brokerServerBuilder.stop();
        } catch (final Exception e) {
          LOGGER.error("Caught exception while running shutdown hook", e);
        }
      }
    });
    return brokerServerBuilder;
  }

  /**
   * To be overridden to plug in custom external view change handlers.
   * <p>NOTE: all change handlers will be run in a single thread, so any slow change handler can block other change
   * handlers from running. For slow change handler, make it asynchronous.
   *
   * @param spectatorHelixManager Spectator Helix manager
   * @return List of custom external view change handlers to plug in
   */
  @SuppressWarnings("unused")
  protected List<ClusterChangeHandler> getCustomExternalViewChangeHandlers(HelixManager spectatorHelixManager) {
    return Collections.emptyList();
  }

  /**
   * To be overridden to plug in custom instance config change handlers.
   * <p>NOTE: all change handlers will be run in a single thread, so any slow change handler can block other change
   * handlers from running. For slow change handler, make it asynchronous.
   *
   * @param spectatorHelixManager Spectator Helix manager
   * @return List of custom instance config change handlers to plug in
   */
  @SuppressWarnings("unused")
  protected List<ClusterChangeHandler> getCustomInstanceConfigChangeHandlers(HelixManager spectatorHelixManager) {
    return Collections.emptyList();
  }

  /**
   * To be overridden to plug in custom live instance change handlers.
   * <p>NOTE: all change handlers will be run in a single thread, so any slow change handler can block other change
   * handlers from running. For slow change handler, make it asynchronous.
   *
   * @param spectatorHelixManager Spectator Helix manager
   * @return List of custom live instance change handlers to plug in
   */
  @SuppressWarnings("unused")
  protected List<ClusterChangeHandler> getCustomLiveInstanceChangeHandlers(HelixManager spectatorHelixManager) {
    return Collections.emptyList();
  }

  public AccessControlFactory getAccessControlFactory() {
    return _accessControlFactory;
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

  public static HelixBrokerStarter startDefault()
      throws Exception {
    Configuration brokerConf = new BaseConfiguration();
    int port = 5001;
    brokerConf.addProperty(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT, port);
    brokerConf.addProperty(CommonConstants.Broker.CONFIG_OF_BROKER_TIMEOUT_MS, 60 * 1000L);
    return new HelixBrokerStarter(null, "quickstart", "localhost:2122", brokerConf);
  }

  public void shutdown() {
    LOGGER.info("Shutting down");

    if (_participantHelixManager != null) {
      LOGGER.info("Disconnecting participant Helix manager");
      _participantHelixManager.disconnect();
    }

    if (_spectatorHelixManager != null) {
      LOGGER.info("Disconnecting spectator Helix manager");
      _spectatorHelixManager.disconnect();
    }

    if (_tbiMessageHandler != null) {
      LOGGER.info("Shutting down timeboundary info refresh message handler");
      _tbiMessageHandler.shutdown();
    }

    _clusterChangeMediator.stop();
  }

  public MetricsRegistry getMetricsRegistry() {
    return _metricsRegistry;
  }

  public static void main(String[] args)
      throws Exception {
    startDefault();
  }
}
