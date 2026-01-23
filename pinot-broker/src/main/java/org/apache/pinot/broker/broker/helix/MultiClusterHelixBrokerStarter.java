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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.pinot.broker.routing.manager.MultiClusterRoutingManager;
import org.apache.pinot.broker.routing.manager.RemoteClusterBrokerRoutingManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.config.provider.ZkTableCache;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.core.routing.MultiClusterRoutingContext;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Multi-cluster broker starter that extends the base Helix broker functionality
 * to support federation across multiple Pinot clusters.
 *
 * This class handles:
 * - Connection to remote clusters via separate ZooKeeper instances
 * - Federated routing across primary and remote clusters
 * - Cross-cluster query federation
 * - Cluster change monitoring for remote clusters
 */
@SuppressWarnings("unused")
public class MultiClusterHelixBrokerStarter extends BaseBrokerStarter {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiClusterHelixBrokerStarter.class);

  // Remote cluster configuration
  protected List<String> _remoteClusterNames;
  protected Map<String, String> _remoteZkServers;
  protected String _remoteInstanceId;

  // Remote cluster Helix managers and routing
  protected Map<String, HelixManager> _remoteSpectatorHelixManager;
  protected Map<String, RemoteClusterBrokerRoutingManager> _remoteRoutingManagers;
  protected MultiClusterRoutingManager _multiClusterRoutingManager;
  protected MultiClusterRoutingContext _multiClusterRoutingContext;
  protected Map<String, ClusterChangeMediator> _remoteClusterChangeMediator;

  // Tracks clusters that failed to connect (for adding warnings to query responses)
  protected Set<String> _unavailableClusters;

  public MultiClusterHelixBrokerStarter() {
  }

  @Override
  public void init(PinotConfiguration brokerConf)
      throws Exception {
    super.init(brokerConf);
    _remoteInstanceId = _instanceId + "_remote";
    initRemoteClusterNamesAndZk(brokerConf);
  }

  @Override
  public void start()
      throws Exception {
    LOGGER.info("[multi-cluster] Starting multi-cluster broker");
    super.start();
    // build routing tables for remote clusters
    initRemoteClusterRouting();
    LOGGER.info("[multi-cluster] Multi-cluster broker started successfully");
  }

  @Override
  protected void initSpectatorHelixManager() throws Exception {
    super.initSpectatorHelixManager();
    try {
      initRemoteClusterSpectatorHelixManagers();
    } catch (Exception e) {
      LOGGER.error("[multi-cluster] Failed to initialize remote cluster spectator Helix managers", e);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.MULTI_CLUSTER_BROKER_STARTUP_FAILURE, 1);
    }
  }

  @Override
  protected void initRoutingManager() throws Exception {
    super.initRoutingManager();
    try {
      initRemoteClusterFederatedRoutingManager();
    } catch (Exception e) {
      LOGGER.error("[multi-cluster] Failed to initialize remote cluster federated routing manager", e);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.MULTI_CLUSTER_BROKER_STARTUP_FAILURE, 1);
    }
  }

  @Override
  protected void initClusterChangeMediator() throws Exception {
    super.initClusterChangeMediator();
    try {
      initRemoteClusterChangeMediator();
    } catch (Exception e) {
      LOGGER.error("[multi-cluster] Failed to initialize remote cluster change mediator", e);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.MULTI_CLUSTER_BROKER_STARTUP_FAILURE, 1);
    }
  }

  private void initRemoteClusterSpectatorHelixManagers() throws Exception {
    _unavailableClusters = new HashSet<>();
    if (_remoteZkServers == null || _remoteZkServers.isEmpty()) {
      LOGGER.info("[multi-cluster] No remote ZK servers configured - skipping spectator Helix manager init");
      return;
    }

    LOGGER.info("[multi-cluster] Initializing spectator Helix managers for {} remote clusters",
      _remoteZkServers.size());
    _remoteSpectatorHelixManager = new HashMap<>();

    for (Map.Entry<String, String> entry : _remoteZkServers.entrySet()) {
      String clusterName = entry.getKey();
      String zkServers = entry.getValue();
      try {
        HelixManager helixManager = HelixManagerFactory.getZKHelixManager(
          clusterName, _instanceId, InstanceType.SPECTATOR, zkServers);
        helixManager.connect();
        _remoteSpectatorHelixManager.put(clusterName, helixManager);
        LOGGER.info("[multi-cluster] Connected to remote cluster '{}' at ZK: {}", clusterName, zkServers);
      } catch (Exception e) {
        LOGGER.error("[multi-cluster] Failed to connect to cluster '{}' at ZK: {}", clusterName, zkServers, e);
        _unavailableClusters.add(clusterName);
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.MULTI_CLUSTER_BROKER_STARTUP_FAILURE, 1);
      }
    }

    if (_remoteSpectatorHelixManager.isEmpty()) {
      LOGGER.warn("[multi-cluster] Failed to connect to any remote clusters - "
        + "multi-cluster will not be functional");
    } else {
      LOGGER.info("[multi-cluster] Connected to {}/{} remote clusters: {}", _remoteSpectatorHelixManager.size(),
        _remoteZkServers.size(), _remoteSpectatorHelixManager.keySet());
    }
    if (!_unavailableClusters.isEmpty()) {
      LOGGER.warn("[multi-cluster] The following clusters are unavailable and will generate warnings "
          + "in query responses: {}", _unavailableClusters);
    }
  }

  protected void stopRemoteClusterComponents() {
    if (_remoteClusterChangeMediator != null) {
      _remoteClusterChangeMediator.values().forEach(ClusterChangeMediator::stop);
    }
    if (_remoteRoutingManagers != null) {
      for (RemoteClusterBrokerRoutingManager routingManager : _remoteRoutingManagers.values()) {
        routingManager.shutdown();
      }
    }
    if (_remoteSpectatorHelixManager != null) {
      _remoteSpectatorHelixManager.values().forEach(HelixManager::disconnect);
    }
  }

  private void initRemoteClusterNamesAndZk(PinotConfiguration brokerConf) {
    LOGGER.info("[multi-cluster] Initializing remote cluster configuration");
    String remoteClusterNames = brokerConf.getProperty(Helix.CONFIG_OF_REMOTE_CLUSTER_NAMES);

    if (remoteClusterNames == null || remoteClusterNames.trim().isEmpty()) {
      LOGGER.info("[multi-cluster] No remote cluster configured - multi-cluster mode disabled");
      return;
    }

    _remoteClusterNames = Arrays.asList(remoteClusterNames.replaceAll("\\s+", "").split(","));
    if (_remoteClusterNames.isEmpty()) {
      LOGGER.warn("[multi-cluster] Remote cluster names list is empty after parsing");
      return;
    }
    LOGGER.info("[multi-cluster] Configured remote cluster names: {}", _remoteClusterNames);

    _remoteZkServers = new HashMap<>();
    for (String name : _remoteClusterNames) {
      String zkConfig = String.format(Helix.CONFIG_OF_REMOTE_ZOOKEEPER_SERVERS, name);
      String zkServers = brokerConf.getProperty(zkConfig);

      if (zkServers == null || zkServers.trim().isEmpty()) {
        LOGGER.error("[multi-cluster] Missing ZooKeeper configuration for cluster '{}', expected: {}", name, zkConfig);
        continue;
      }
      _remoteZkServers.put(name, zkServers.replaceAll("\\s+", ""));
    }

    if (_remoteZkServers.isEmpty()) {
      LOGGER.error("[multi-cluster] No valid ZooKeeper configurations found - multi-cluster will not be functional");
      _remoteClusterNames = null;
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.MULTI_CLUSTER_BROKER_STARTUP_FAILURE, 1);
    } else {
      LOGGER.info("[multi-cluster] Initialized {} remote cluster(s): {}", _remoteZkServers.size(),
          _remoteZkServers.keySet());
    }
  }

  private void initRemoteClusterFederatedRoutingManager() {
    if (_remoteSpectatorHelixManager == null || _remoteSpectatorHelixManager.isEmpty()) {
      LOGGER.info("[multi-cluster] No remote spectator Helix managers - skipping federated routing manager init");
      return;
    }

    LOGGER.info("[multi-cluster] Initializing federated routing manager for {} clusters",
        _remoteSpectatorHelixManager.size());
    _remoteRoutingManagers = new HashMap<>();

    for (Map.Entry<String, HelixManager> entry : _remoteSpectatorHelixManager.entrySet()) {
      String clusterName = entry.getKey();
      try {
        RemoteClusterBrokerRoutingManager routingManager =
            new RemoteClusterBrokerRoutingManager(clusterName, _brokerMetrics, _serverRoutingStatsManager, _brokerConf);
        routingManager.init(entry.getValue());
        _remoteRoutingManagers.put(clusterName, routingManager);
      } catch (Exception e) {
        LOGGER.error("[multi-cluster] Failed to initialize routing manager for cluster '{}'", clusterName, e);
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.MULTI_CLUSTER_BROKER_STARTUP_FAILURE, 1);
      }
    }

    if (_remoteRoutingManagers.isEmpty()) {
      LOGGER.error("[multi-cluster] Failed to initialize any routing managers - federated routing unavailable");
    } else {
      _multiClusterRoutingManager = new MultiClusterRoutingManager(_routingManager,
          new ArrayList<>(_remoteRoutingManagers.values()));
      LOGGER.info("[multi-cluster] Created federated routing manager with {}/{} remote clusters",
          _remoteRoutingManagers.size(), _remoteSpectatorHelixManager.size());
    }
  }

  private void initRemoteClusterFederationProvider(TableCache primaryTableCache, boolean caseInsensitive) {
    if (_multiClusterRoutingManager == null) {
      LOGGER.info("[multi-cluster] Federation is not enabled - FederationProvider will be null");
      _multiClusterRoutingContext = null;
      return;
    }

    Map<String, TableCache> tableCacheMap = new HashMap<>();
    tableCacheMap.put(_clusterName, primaryTableCache);

    if (_remoteSpectatorHelixManager == null || _remoteSpectatorHelixManager.isEmpty()) {
      LOGGER.info("[multi-cluster] No remote spectator Helix managers - "
          + "creating provider with primary cluster only");
      _multiClusterRoutingContext = null;
      return;
    }

    LOGGER.info("[multi-cluster] Initializing federation provider with {} remote clusters",
        _remoteSpectatorHelixManager.size());

    for (Map.Entry<String, HelixManager> entry : _remoteSpectatorHelixManager.entrySet()) {
      String clusterName = entry.getKey();
      try {
        TableCache remoteCache = new ZkTableCache(entry.getValue().getHelixPropertyStore(), caseInsensitive);
        tableCacheMap.put(clusterName, remoteCache);
      } catch (Exception e) {
        LOGGER.error("[multi-cluster] Failed to create table cache for cluster '{}'", clusterName, e);
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.MULTI_CLUSTER_BROKER_STARTUP_FAILURE, 1);
      }
    }

    _multiClusterRoutingContext = new MultiClusterRoutingContext(tableCacheMap, _routingManager,
        _multiClusterRoutingManager, _unavailableClusters);
    LOGGER.info("[multi-cluster] Created federation provider with {}/{} clusters (1 primary + {} remote), "
            + "{} unavailable",
        tableCacheMap.size(), _remoteSpectatorHelixManager.size() + 1, tableCacheMap.size() - 1,
        _unavailableClusters.size());
  }

  private void initRemoteClusterRouting() {
    if (_remoteRoutingManagers == null || _remoteRoutingManagers.isEmpty()) {
      LOGGER.info("[multi-cluster] No remote routing managers - skipping routing table initialization");
      return;
    }

    LOGGER.info("[multi-cluster] Initializing routing tables for {} remote clusters",
        _remoteRoutingManagers.size());
    int initialized = 0;

    for (Map.Entry<String, RemoteClusterBrokerRoutingManager> entry : _remoteRoutingManagers.entrySet()) {
      try {
        entry.getValue().determineRoutingChangeForTables();
        initialized++;
      } catch (Exception e) {
        LOGGER.error("[multi-cluster] Failed to initialize routing tables for cluster '{}'", entry.getKey(), e);
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.MULTI_CLUSTER_BROKER_STARTUP_FAILURE, 1);
      }
    }

    LOGGER.info("[multi-cluster] Initialized routing tables for {}/{} remote clusters",
        initialized, _remoteRoutingManagers.size());
  }

  private void initRemoteClusterChangeMediator() throws Exception {
    if (_remoteSpectatorHelixManager == null || _remoteSpectatorHelixManager.isEmpty()) {
      LOGGER.info("[multi-cluster] No remote spectator Helix managers - skipping cluster change mediator init");
      return;
    }

    if (_remoteRoutingManagers == null || _remoteRoutingManagers.isEmpty()) {
      LOGGER.error("[multi-cluster] Remote routing managers not initialized - "
          + "cannot create cluster change mediators");
      return;
    }

    LOGGER.info("[multi-cluster] Initializing cluster change mediators for {} remote clusters",
        _remoteSpectatorHelixManager.size());
    _remoteClusterChangeMediator = new HashMap<>();

    for (String clusterName : _remoteSpectatorHelixManager.keySet()) {
      RemoteClusterBrokerRoutingManager routingManager = _remoteRoutingManagers.get(clusterName);
      if (routingManager == null) {
        LOGGER.error("[multi-cluster] Routing manager not found for cluster '{}' - skipping mediator setup",
            clusterName);
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.MULTI_CLUSTER_BROKER_STARTUP_FAILURE, 1);
        continue;
      }

      try {
        Map<ChangeType, List<ClusterChangeHandler>> handlers = new HashMap<>();
        handlers.put(ChangeType.CLUSTER_CONFIG, new ArrayList<>());
        handlers.put(ChangeType.IDEAL_STATE, Collections.singletonList(routingManager));
        handlers.put(ChangeType.EXTERNAL_VIEW, Collections.singletonList(routingManager));
        handlers.put(ChangeType.INSTANCE_CONFIG, Collections.singletonList(routingManager));

        ClusterChangeMediator mediator = new ClusterChangeMediator(handlers, _brokerMetrics);
        mediator.start();
        _remoteClusterChangeMediator.put(clusterName, mediator);

        HelixManager helixManager = _remoteSpectatorHelixManager.get(clusterName);
        helixManager.addIdealStateChangeListener(mediator);
        helixManager.addExternalViewChangeListener(mediator);
        helixManager.addInstanceConfigChangeListener(mediator);
        helixManager.addClusterfigChangeListener(mediator);
      } catch (Exception e) {
        LOGGER.error("[multi-cluster] Failed to initialize cluster change mediator for cluster '{}'", clusterName, e);
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.MULTI_CLUSTER_BROKER_STARTUP_FAILURE, 1);
      }
    }

    LOGGER.info("[multi-cluster] Initialized {}/{} cluster change mediators", _remoteClusterChangeMediator.size(),
        _remoteSpectatorHelixManager.size());
  }

  @Override
  protected MultiClusterRoutingContext getMultiClusterRoutingContext() {
    initRemoteClusterFederationProvider(_tableCache,
      _brokerConf.getProperty(Helix.ENABLE_CASE_INSENSITIVE_KEY, Helix.DEFAULT_ENABLE_CASE_INSENSITIVE));
    return _multiClusterRoutingContext;
  }

  @Override
  public void stop() {
    LOGGER.info("[multi-cluster] Shutting down multi-cluster broker");
    super.stop();
    stopRemoteClusterComponents();
    LOGGER.info("[multi-cluster] Multi-cluster broker shut down successfully");
  }
}
