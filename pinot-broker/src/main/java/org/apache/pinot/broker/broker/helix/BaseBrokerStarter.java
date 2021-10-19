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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.PinotMetricUtils;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.helix.TableCache;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.core.transport.TlsConfig;
import org.apache.pinot.core.util.ListenerConfigUtil;
import org.apache.pinot.core.util.TlsUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.spi.services.ServiceStartable;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.sql.parsers.rewriter.QueryRewriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base class for broker startable implementations
 */
@SuppressWarnings("unused")
public abstract class BaseBrokerStarter implements ServiceStartable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseBrokerStarter.class);

  protected PinotConfiguration _brokerConf;
  protected List<ListenerConfig> _listenerConfigs;
  protected String _clusterName;
  protected String _zkServers;
  protected String _hostname;
  protected int _port;
  protected String _instanceId;
  protected final List<ClusterChangeHandler> _externalViewChangeHandlers = new ArrayList<>();
  protected final List<ClusterChangeHandler> _instanceConfigChangeHandlers = new ArrayList<>();
  protected final List<ClusterChangeHandler> _liveInstanceChangeHandlers = new ArrayList<>();
  // Spectator Helix manager handles the custom change listeners, properties read/write
  protected HelixManager _spectatorHelixManager;
  protected HelixAdmin _helixAdmin;
  protected ZkHelixPropertyStore<ZNRecord> _propertyStore;
  protected HelixDataAccessor _helixDataAccessor;
  protected PinotMetricsRegistry _metricsRegistry;
  protected BrokerMetrics _brokerMetrics;
  protected RoutingManager _routingManager;
  protected AccessControlFactory _accessControlFactory;
  protected BrokerRequestHandler _brokerRequestHandler;
  protected BrokerAdminApiApplication _brokerAdminApplication;
  protected ClusterChangeMediator _clusterChangeMediator;
  // Participant Helix manager handles Helix functionality such as state transitions and messages
  protected HelixManager _participantHelixManager;

  @Override
  public void init(PinotConfiguration brokerConf)
      throws Exception {
    _brokerConf = brokerConf;
    _listenerConfigs = ListenerConfigUtil.buildBrokerConfigs(brokerConf);
    setupHelixSystemProperties();

    _clusterName = brokerConf.getProperty(Helix.CONFIG_OF_CLUSTER_NAME);

    // Remove all white-spaces from the list of zkServers (if any).
    _zkServers = brokerConf.getProperty(Helix.CONFIG_OF_ZOOKEEPR_SERVER).replaceAll("\\s+", "");
    _hostname = brokerConf.getProperty(Broker.CONFIG_OF_BROKER_HOSTNAME);
    if (_hostname == null) {
      _hostname =
          _brokerConf.getProperty(Helix.SET_INSTANCE_ID_TO_HOSTNAME_KEY, false) ? NetUtils.getHostnameOrAddress()
              : NetUtils.getHostAddress();
    }
    _port = _listenerConfigs.get(0).getPort();

    _instanceId = _brokerConf.getProperty(Helix.Instance.INSTANCE_ID_KEY);
    if (_instanceId != null) {
      // NOTE: Force all instances to have the same prefix in order to derive the instance type based on the instance id
      Preconditions.checkState(_instanceId.startsWith(Helix.PREFIX_OF_BROKER_INSTANCE),
          "Instance id must have prefix '%s', got '%s'", Helix.PREFIX_OF_BROKER_INSTANCE, _instanceId);
    } else {
      _instanceId = Helix.PREFIX_OF_BROKER_INSTANCE + _hostname + "_" + _port;
    }

    _brokerConf.setProperty(Broker.CONFIG_OF_BROKER_ID, _instanceId);
  }

  private void setupHelixSystemProperties() {
    // NOTE: Helix will disconnect the manager and disable the instance if it detects flapping (too frequent disconnect
    // from ZooKeeper). Setting flapping time window to a small value can avoid this from happening. Helix ignores the
    // non-positive value, so set the default value as 1.
    System.setProperty(SystemPropertyKeys.FLAPPING_TIME_WINDOW,
        _brokerConf.getProperty(Helix.CONFIG_OF_BROKER_FLAPPING_TIME_WINDOW_MS, Helix.DEFAULT_FLAPPING_TIME_WINDOW_MS));
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

  @Override
  public ServiceRole getServiceRole() {
    return ServiceRole.BROKER;
  }

  @Override
  public String getInstanceId() {
    return _instanceId;
  }

  @Override
  public PinotConfiguration getConfig() {
    return _brokerConf;
  }

  @Override
  public void start()
      throws Exception {
    LOGGER.info("Starting Pinot broker");
    Utils.logVersions();

    LOGGER.info("Connecting spectator Helix manager");
    _spectatorHelixManager =
        HelixManagerFactory.getZKHelixManager(_clusterName, _instanceId, InstanceType.SPECTATOR, _zkServers);
    _spectatorHelixManager.connect();
    _helixAdmin = _spectatorHelixManager.getClusterManagmentTool();
    _propertyStore = _spectatorHelixManager.getHelixPropertyStore();
    _helixDataAccessor = _spectatorHelixManager.getHelixDataAccessor();

    // Fetch cluster level config from ZK
    HelixConfigScope helixConfigScope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(_clusterName).build();
    Map<String, String> configMap = _helixAdmin.getConfig(helixConfigScope, Arrays
        .asList(Helix.ENABLE_CASE_INSENSITIVE_KEY, Helix.DEPRECATED_ENABLE_CASE_INSENSITIVE_KEY,
            Broker.CONFIG_OF_ENABLE_QUERY_LIMIT_OVERRIDE, Helix.DEFAULT_HYPERLOGLOG_LOG2M_KEY,
            Helix.ENABLE_DISTINCT_COUNT_BITMAP_OVERRIDE_KEY));

    boolean caseInsensitive = Boolean.parseBoolean(configMap.get(Helix.ENABLE_CASE_INSENSITIVE_KEY)) || Boolean
        .parseBoolean(configMap.get(Helix.DEPRECATED_ENABLE_CASE_INSENSITIVE_KEY));

    String log2mStr = configMap.get(Helix.DEFAULT_HYPERLOGLOG_LOG2M_KEY);
    if (log2mStr != null) {
      try {
        _brokerConf.setProperty(Helix.DEFAULT_HYPERLOGLOG_LOG2M_KEY, Integer.parseInt(log2mStr));
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid config of '{}': '{}', using: {} as the default log2m for HyperLogLog",
            Helix.DEFAULT_HYPERLOGLOG_LOG2M_KEY, log2mStr, Helix.DEFAULT_HYPERLOGLOG_LOG2M);
      }
    }

    if (Boolean.parseBoolean(configMap.get(Broker.CONFIG_OF_ENABLE_QUERY_LIMIT_OVERRIDE))) {
      _brokerConf.setProperty(Broker.CONFIG_OF_ENABLE_QUERY_LIMIT_OVERRIDE, true);
    }

    if (Boolean.parseBoolean(configMap.get(Helix.ENABLE_DISTINCT_COUNT_BITMAP_OVERRIDE_KEY))) {
      _brokerConf.setProperty(Helix.ENABLE_DISTINCT_COUNT_BITMAP_OVERRIDE_KEY, true);
    }

    LOGGER.info("Setting up broker request handler");
    // Set up metric registry and broker metrics
    PinotConfiguration metricsConfiguration = _brokerConf.subset(Broker.METRICS_CONFIG_PREFIX);
    PinotMetricUtils.init(metricsConfiguration);
    _metricsRegistry = PinotMetricUtils.getPinotMetricsRegistry();
    _brokerMetrics = new BrokerMetrics(
        _brokerConf.getProperty(Broker.CONFIG_OF_METRICS_NAME_PREFIX, Broker.DEFAULT_METRICS_NAME_PREFIX),
        _metricsRegistry,
        _brokerConf.getProperty(Broker.CONFIG_OF_ENABLE_TABLE_LEVEL_METRICS, Broker.DEFAULT_ENABLE_TABLE_LEVEL_METRICS),
        _brokerConf.getProperty(Broker.CONFIG_OF_ALLOWED_TABLES_FOR_EMITTING_METRICS, Collections.emptyList()));
    _brokerMetrics.initializeGlobalMeters();
    // Set up request handling classes
    _routingManager = new RoutingManager(_brokerMetrics);
    _routingManager.init(_spectatorHelixManager);
    _accessControlFactory = AccessControlFactory.loadFactory(_brokerConf.subset(Broker.ACCESS_CONTROL_CONFIG_PREFIX));
    HelixExternalViewBasedQueryQuotaManager queryQuotaManager =
        new HelixExternalViewBasedQueryQuotaManager(_brokerMetrics, _instanceId);
    queryQuotaManager.init(_spectatorHelixManager);
    // Initialize QueryRewriterFactory
    LOGGER.info("Initializing QueryRewriterFactory");
    QueryRewriterFactory.init(_brokerConf.getProperty(Broker.CONFIG_OF_BROKER_QUERY_REWRITER_CLASS_NAMES));
    // Initialize FunctionRegistry before starting the broker request handler
    FunctionRegistry.init();
    TableCache tableCache = new TableCache(_propertyStore, caseInsensitive);
    // Configure TLS for netty connection to server
    TlsConfig tlsDefaults = TlsUtils.extractTlsConfig(_brokerConf, Broker.BROKER_TLS_PREFIX);

    if (_brokerConf.getProperty(Broker.BROKER_NETTYTLS_ENABLED, false)) {
      _brokerRequestHandler =
          new SingleConnectionBrokerRequestHandler(_brokerConf, _routingManager, _accessControlFactory,
              queryQuotaManager, tableCache, _brokerMetrics, tlsDefaults);
    } else {
      _brokerRequestHandler =
          new SingleConnectionBrokerRequestHandler(_brokerConf, _routingManager, _accessControlFactory,
              queryQuotaManager, tableCache, _brokerMetrics, null);
    }

    LOGGER.info("Starting broker admin application on: {}", ListenerConfigUtil.toString(_listenerConfigs));
    _brokerAdminApplication =
        new BrokerAdminApiApplication(_routingManager, _brokerRequestHandler, _brokerMetrics, _brokerConf);
    _brokerAdminApplication.start(_listenerConfigs);

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
    _instanceConfigChangeHandlers.add(queryQuotaManager);
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
        HelixManagerFactory.getZKHelixManager(_clusterName, _instanceId, InstanceType.PARTICIPANT, _zkServers);
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
    updateInstanceConfigIfNeeded();
    _brokerMetrics
        .addCallbackGauge(Helix.INSTANCE_CONNECTED_METRIC_NAME, () -> _participantHelixManager.isConnected() ? 1L : 0L);
    _participantHelixManager
        .addPreConnectCallback(() -> _brokerMetrics.addMeteredGlobalValue(BrokerMeter.HELIX_ZOOKEEPER_RECONNECTS, 1L));

    // Register the service status handler
    registerServiceStatusHandler();

    LOGGER.info("Finish starting Pinot broker");
  }

  private void updateInstanceConfigIfNeeded() {
    InstanceConfig instanceConfig = HelixHelper.getInstanceConfig(_participantHelixManager, _instanceId);
    boolean updated = HelixHelper.updateHostnamePort(instanceConfig, _hostname, _port);
    updated |= HelixHelper.addDefaultTags(instanceConfig, () -> {
      if (ZKMetadataProvider.getClusterTenantIsolationEnabled(_propertyStore)) {
        return Collections.singletonList(TagNameUtils.getBrokerTagForTenant(null));
      } else {
        return Collections.singletonList(Helix.UNTAGGED_BROKER_INSTANCE);
      }
    });
    if (updated) {
      HelixHelper.updateInstanceConfig(_participantHelixManager, instanceConfig);
    }
  }

  /**
   * Fetches the resources to monitor and registers the
   * {@link org.apache.pinot.common.utils.ServiceStatus.ServiceStatusCallback}s
   */
  private void registerServiceStatusHandler() {
    List<String> resourcesToMonitor = new ArrayList<>(1);
    IdealState brokerResourceIdealState =
        _helixAdmin.getResourceIdealState(_clusterName, Helix.BROKER_RESOURCE_INSTANCE);
    if (brokerResourceIdealState != null && brokerResourceIdealState.isEnabled()) {
      for (String partitionName : brokerResourceIdealState.getPartitionSet()) {
        if (brokerResourceIdealState.getInstanceSet(partitionName).contains(_instanceId)) {
          resourcesToMonitor.add(Helix.BROKER_RESOURCE_INSTANCE);
          break;
        }
      }
    }

    double minResourcePercentForStartup = _brokerConf
        .getProperty(Broker.CONFIG_OF_BROKER_MIN_RESOURCE_PERCENT_FOR_START,
            Broker.DEFAULT_BROKER_MIN_RESOURCE_PERCENT_FOR_START);

    LOGGER.info("Registering service status handler");
    ServiceStatus.setServiceStatusCallback(_instanceId, new ServiceStatus.MultipleCallbackServiceStatusCallback(
        ImmutableList.of(new ServiceStatus.IdealStateAndCurrentStateMatchServiceStatusCallback(_participantHelixManager,
                _clusterName, _instanceId, resourcesToMonitor, minResourcePercentForStartup),
            new ServiceStatus.IdealStateAndExternalViewMatchServiceStatusCallback(_participantHelixManager,
                _clusterName, _instanceId, resourcesToMonitor, minResourcePercentForStartup))));
  }

  @Override
  public void stop() {
    LOGGER.info("Shutting down Pinot broker");

    LOGGER.info("Disconnecting participant Helix manager");
    _participantHelixManager.disconnect();

    LOGGER.info("Stopping cluster change mediator");
    _clusterChangeMediator.stop();

    // Delay shutdown of request handler so that the pending queries can be finished. The participant Helix manager has
    // been disconnected, so instance should disappear from ExternalView soon and stop getting new queries.
    long delayShutdownTimeMs =
        _brokerConf.getProperty(Broker.CONFIG_OF_DELAY_SHUTDOWN_TIME_MS, Broker.DEFAULT_DELAY_SHUTDOWN_TIME_MS);
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

    LOGGER.info("Deregistering service status handler");
    ServiceStatus.removeServiceStatusCallback(_instanceId);
    LOGGER.info("Shutdown Broker Metrics Registry");
    _metricsRegistry.shutdown();
    LOGGER.info("Finish shutting down Pinot broker for {}", _instanceId);
  }

  public HelixManager getSpectatorHelixManager() {
    return _spectatorHelixManager;
  }

  public PinotMetricsRegistry getMetricsRegistry() {
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
}
