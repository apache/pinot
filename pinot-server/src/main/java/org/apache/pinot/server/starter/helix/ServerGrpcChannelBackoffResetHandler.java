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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
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


/**
 * Listens for instance config changes and resets GRPC mailbox channel backoff when a server
 * completes startup (IS_SHUTDOWN_IN_PROGRESS transitions from true to false).
 *
 * <p>When a server restarts (gracefully or after a crash), the startup sequence is:
 * <ol>
 *   <li>updateInstanceConfigIfNeeded sets IS_SHUTDOWN_IN_PROGRESS=true (even after crash)</li>
 *   <li>Segments load, startup checks run</li>
 *   <li>Query/gRPC server starts listening</li>
 *   <li>IS_SHUTDOWN_IN_PROGRESS set to false (readiness signal)</li>
 * </ol>
 *
 * <p>This handler detects the transition in step 4 and resets the GRPC channel backoff for
 * that specific server, but only if the channel is currently in TRANSIENT_FAILURE state.
 * Channels in other states (READY, IDLE, CONNECTING) are left untouched.</p>
 */
@BatchMode(enabled = false)
@PreFetch(enabled = false)
public class ServerGrpcChannelBackoffResetHandler implements InstanceConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerGrpcChannelBackoffResetHandler.class);

  private final HelixAdmin _helixAdmin;
  private final String _clusterName;
  private final String _selfInstanceId;
  private final MailboxService _mailboxService;

  // Tracks servers that have IS_SHUTDOWN_IN_PROGRESS=true. When a server transitions
  // from being in this set to having IS_SHUTDOWN_IN_PROGRESS=false, we reset its channel backoff.
  private final Set<String> _shuttingDownServers = new HashSet<>();

  public ServerGrpcChannelBackoffResetHandler(HelixAdmin helixAdmin, String clusterName, String selfInstanceId,
      MailboxService mailboxService) {
    _helixAdmin = helixAdmin;
    _clusterName = clusterName;
    _selfInstanceId = selfInstanceId;
    _mailboxService = mailboxService;
  }

  @Override
  public synchronized void onInstanceConfigChange(List<InstanceConfig> instanceConfigs,
      NotificationContext context) {
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
      if (pathChanged == null) {
        // Shouldn't happen, but be defensive.
        return;
      }
      String instanceName = pathChanged.substring(pathChanged.lastIndexOf('/') + 1);
      handleSingleInstanceChange(instanceName);
    }
  }

  /**
   * Full cluster scan: used on INIT (first callback) and when instances are added/removed.
   */
  private void handleFullScan() {
    Set<String> currentShuttingDown = new HashSet<>();
    for (String instance : _helixAdmin.getInstancesInCluster(_clusterName)) {
      if (!InstanceTypeUtils.isServer(instance) || instance.equals(_selfInstanceId)) {
        continue;
      }
      InstanceConfig config = getInstanceConfig(instance);
      if (config == null) {
        continue;
      }
      if (isShutdownInProgress(config)) {
        currentShuttingDown.add(instance);
      } else if (_shuttingDownServers.contains(instance)) {
        resetBackoffForServer(instance, config);
      }
    }
    _shuttingDownServers.clear();
    _shuttingDownServers.addAll(currentShuttingDown);
  }

  /**
   * Handles a single instance config change by reading only the changed instance's config
   * from ZooKeeper, rather than scanning the entire cluster.
   */
  private void handleSingleInstanceChange(String instance) {
    if (!InstanceTypeUtils.isServer(instance) || instance.equals(_selfInstanceId)) {
      return;
    }
    InstanceConfig config = getInstanceConfig(instance);
    if (config == null) {
      return;
    }
    if (isShutdownInProgress(config)) {
      _shuttingDownServers.add(instance);
    } else if (_shuttingDownServers.remove(instance)) {
      resetBackoffForServer(instance, config);
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


  private void resetBackoffForServer(String instanceId, InstanceConfig config) {
    int mailboxPort = config.getRecord().getIntField(
        CommonConstants.Helix.Instance.MULTI_STAGE_QUERY_ENGINE_MAILBOX_PORT_KEY, -1);
    if (mailboxPort <= 0) {
      LOGGER.debug("Server {} has no mailbox port configured, skipping backoff reset", instanceId);
      return;
    }
    String hostname;
    try {
      hostname = ServerInstance.extractHostnameFromConfig(config);
    } catch (Exception e) {
      LOGGER.warn("Could not extract hostname for server: {}, skipping backoff reset", instanceId, e);
      return;
    }
    if (_mailboxService.resetConnectBackoff(hostname, mailboxPort)) {
      LOGGER.info("Server {} completed startup, reset mailbox channel backoff to {}:{}", instanceId, hostname,
          mailboxPort);
    }
  }
}
