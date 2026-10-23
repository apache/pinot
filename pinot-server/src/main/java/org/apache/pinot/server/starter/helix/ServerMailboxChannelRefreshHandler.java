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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.HelixAdmin;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Listens for instance config changes and refreshes the GRPC mailbox channel to a server when it completes startup
/// (IS_SHUTDOWN_IN_PROGRESS transitions from true to false).
///
/// When a server restarts (gracefully or after a crash), the startup sequence is:
///
/// 1. updateInstanceConfigIfNeeded sets IS_SHUTDOWN_IN_PROGRESS=true (even after crash)
/// 2. Segments load, startup checks run
/// 3. Query/gRPC server starts listening
/// 4. IS_SHUTDOWN_IN_PROGRESS set to false (readiness signal)
///
/// This handler detects the transition in step 4 and refreshes the channel to that server: the channel resolves the
/// server's hostname again before the next send, and connects without waiting for its reconnect backoff. Resolving
/// again matters because the restarted server may have a new IP address. The channel last resolved the hostname when
/// the old connection was lost, while DNS could still return the old address, and it would reconnect there. When
/// another server took over that address and listens on the same port, the connection succeeds and the data goes to
/// the wrong server.
///
/// It also shuts down the channel to the old mailbox address of a server that moved to another address (for example a
/// new port after a restart) or left the cluster, since nothing will use it again.
@BatchMode(enabled = false)
@PreFetch(enabled = false)
public class ServerMailboxChannelRefreshHandler implements InstanceConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerMailboxChannelRefreshHandler.class);

  private final HelixAdmin _helixAdmin;
  private final String _clusterName;
  private final String _selfInstanceId;
  private final MailboxService _mailboxService;

  // Tracks servers that have IS_SHUTDOWN_IN_PROGRESS=true. When a server transitions
  // from being in this set to having IS_SHUTDOWN_IN_PROGRESS=false, we refresh its channel.
  private final Set<String> _shuttingDownServers = new HashSet<>();
  // Last known mailbox address of each server, to retire the channel of an address a server has left
  private final Map<String, Pair<String, Integer>> _mailboxAddresses = new HashMap<>();

  public ServerMailboxChannelRefreshHandler(HelixAdmin helixAdmin, String clusterName, String selfInstanceId,
      MailboxService mailboxService) {
    _helixAdmin = helixAdmin;
    _clusterName = clusterName;
    _selfInstanceId = selfInstanceId;
    _mailboxService = mailboxService;
  }

  @Override
  public synchronized void onInstanceConfigChange(List<InstanceConfig> instanceConfigs,
      NotificationContext context) {
    // Only process INIT (listener registration) and CALLBACK (ZK data/child change).
    // Ignore FINALIZE (listener unregistration) and other types.

    // INIT: first callback when the listener is registered (full cluster snapshot).
    // isChildChange: an instance ZK node was added or removed under /CONFIGS/PARTICIPANT.
    // Both require a full scan to rebuild _shuttingDownServers from the current cluster state.
    NotificationContext.Type type = context.getType();
    if (type == NotificationContext.Type.INIT || context.getIsChildChange()) {
      handleFullScan();
    } else if (type == NotificationContext.Type.CALLBACK) {
      // An existing instance's config changed (e.g. IS_SHUTDOWN_IN_PROGRESS toggled).
      // pathChanged is the ZK path of the specific instance that changed.
      String pathChanged = context.getPathChanged();
      String instanceName = pathChanged.substring(pathChanged.lastIndexOf('/') + 1);
      handleSingleInstanceChange(instanceName);
    }
  }

  /// Full cluster scan: used on INIT (first callback) and when instances are added/removed.
  ///
  /// It records the new address of every server before it retires any old one. Otherwise, when one server left an
  /// address and a server read later in the scan took it over, the scan would shut down the channel the second server
  /// uses.
  private void handleFullScan() {
    Set<String> currentShuttingDown = new HashSet<>();
    Set<String> currentServers = new HashSet<>();
    Map<String, Pair<String, Integer>> previousAddresses = new HashMap<>();
    for (String instance : _helixAdmin.getInstancesInCluster(_clusterName)) {
      if (!InstanceTypeUtils.isServer(instance) || instance.equals(_selfInstanceId)) {
        continue;
      }
      // Before reading the config: a server whose config cannot be read now has not left the cluster
      currentServers.add(instance);
      InstanceConfig config = getInstanceConfig(instance);
      if (config == null) {
        continue;
      }
      Pair<String, Integer> previousAddress = recordMailboxAddress(instance, config);
      if (previousAddress != null) {
        previousAddresses.put(instance, previousAddress);
      }
      if (isShutdownInProgress(config)) {
        currentShuttingDown.add(instance);
      } else if (_shuttingDownServers.contains(instance)) {
        refreshChannelToServer(instance, config);
      }
    }
    // Servers that left the cluster
    for (String instance : new HashSet<>(_mailboxAddresses.keySet())) {
      if (!currentServers.contains(instance)) {
        previousAddresses.put(instance, _mailboxAddresses.remove(instance));
      }
    }
    for (Map.Entry<String, Pair<String, Integer>> entry : previousAddresses.entrySet()) {
      if (!entry.getValue().equals(_mailboxAddresses.get(entry.getKey()))) {
        retireChannel(entry.getKey(), entry.getValue());
      }
    }
    _shuttingDownServers.clear();
    _shuttingDownServers.addAll(currentShuttingDown);
  }

  /// Handles a single instance config change by reading only the changed instance's config
  /// from ZooKeeper, rather than scanning the entire cluster.
  private void handleSingleInstanceChange(String instance) {
    if (!InstanceTypeUtils.isServer(instance) || instance.equals(_selfInstanceId)) {
      return;
    }
    InstanceConfig config = getInstanceConfig(instance);
    if (config == null) {
      return;
    }
    updateMailboxAddress(instance, config);
    if (isShutdownInProgress(config)) {
      _shuttingDownServers.add(instance);
    } else if (_shuttingDownServers.remove(instance)) {
      refreshChannelToServer(instance, config);
    }
  }

  @Nullable
  private InstanceConfig getInstanceConfig(String instance) {
    try {
      return _helixAdmin.getInstanceConfig(_clusterName, instance);
    } catch (Exception e) {
      LOGGER.warn("Failed to get instance config for: {}, skipping", instance, e);
      return null;
    }
  }

  private static boolean isShutdownInProgress(InstanceConfig config) {
    return Boolean.parseBoolean(
        config.getRecord().getStringField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, "false"));
  }

  /// Records the server's current mailbox address, and retires the channel to its previous one if it moved.
  private void updateMailboxAddress(String instanceId, InstanceConfig config) {
    Pair<String, Integer> previousAddress = recordMailboxAddress(instanceId, config);
    if (previousAddress != null && !previousAddress.equals(_mailboxAddresses.get(instanceId))) {
      retireChannel(instanceId, previousAddress);
    }
  }

  /// Records the server's current mailbox address, and returns its previous one.
  @Nullable
  private Pair<String, Integer> recordMailboxAddress(String instanceId, InstanceConfig config) {
    Pair<String, Integer> address = getMailboxAddress(instanceId, config);
    return address != null ? _mailboxAddresses.put(instanceId, address) : _mailboxAddresses.remove(instanceId);
  }

  private void retireChannel(String instanceId, Pair<String, Integer> address) {
    // Another server may have taken over the address
    if (_mailboxAddresses.containsValue(address)) {
      return;
    }
    if (_mailboxService.retireChannel(address.getLeft(), address.getRight())) {
      LOGGER.info("Server {} left mailbox address {}:{}, shut down the channel to it", instanceId, address.getLeft(),
          address.getRight());
    }
  }

  @Nullable
  private static Pair<String, Integer> getMailboxAddress(String instanceId, InstanceConfig config) {
    int mailboxPort = config.getRecord().getIntField(
        CommonConstants.Helix.Instance.MULTI_STAGE_QUERY_ENGINE_MAILBOX_PORT_KEY, -1);
    if (mailboxPort <= 0) {
      return null;
    }
    try {
      return Pair.of(ServerInstance.extractHostnameFromConfig(config), mailboxPort);
    } catch (Exception e) {
      LOGGER.warn("Could not extract hostname for server: {}", instanceId, e);
      return null;
    }
  }

  private void refreshChannelToServer(String instanceId, InstanceConfig config) {
    Pair<String, Integer> address = getMailboxAddress(instanceId, config);
    if (address == null) {
      LOGGER.debug("Server {} has no mailbox address, skipping channel refresh", instanceId);
      return;
    }
    if (_mailboxService.refreshChannel(address.getLeft(), address.getRight())) {
      LOGGER.info("Server {} completed startup, refreshed mailbox channel to {}:{}", instanceId, address.getLeft(),
          address.getRight());
    }
  }
}
