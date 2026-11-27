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
package org.apache.pinot.common.utils.helix;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;
import javax.annotation.Nullable;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Tracks a homogeneous version snapshot of the live brokers and servers. Register both Helix listeners so that
/// decommissioned instance configs cannot hold the gate closed. Config data changes read only the changed instance;
/// live topology changes retain unchanged configs and read new members. Query threads read one volatile flag.
/// Callbacks serialize snapshot updates and publish only after a complete refresh; transient failures retain the last
/// complete version snapshot, but disable the gate until a complete refresh succeeds. FINALIZE discards the snapshot
/// and disables the gate until a new INIT completes.
@BatchMode(enabled = false)
@PreFetch(enabled = false)
public class ClusterVersionTracker
    implements InstanceConfigChangeListener, LiveInstanceChangeListener, BooleanSupplier {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterVersionTracker.class);

  private final HelixManager _manager;
  private final String _version;
  private final boolean _supportedVersion;
  private final boolean _requireBrokerAndServer;
  private Map<String, String> _versionsByInstance = Map.of();
  private boolean _active;
  private boolean _initialized;
  private volatile boolean _supported;

  public ClusterVersionTracker(HelixManager manager, @Nullable String version, boolean allowSnapshotVersion,
      boolean requireBrokerAndServer) {
    _manager = manager;
    _version = version;
    _supportedVersion = version != null && !PinotVersion.UNKNOWN.equals(version)
        && (allowSnapshotVersion || !version.contains("SNAPSHOT"));
    _requireBrokerAndServer = requireBrokerAndServer;
    if (!_supportedVersion) {
      LOGGER.info("Cluster compatibility gate disabled for local version: {}", version);
    }
  }

  @Override
  public boolean getAsBoolean() {
    return _supported;
  }

  @Override
  public synchronized void onInstanceConfigChange(List<InstanceConfig> configs, NotificationContext context) {
    if (finalizeOrIgnore(context) || !_supportedVersion) {
      return;
    }
    try {
      if (context.getType() == NotificationContext.Type.INIT || context.getIsChildChange() || !_initialized) {
        refreshTopology(context.getType() == NotificationContext.Type.INIT || !_initialized);
        _initialized = true;
      } else {
        String path = context.getPathChanged();
        if (path == null) {
          throw new IllegalStateException("Missing changed instance config path");
        }
        String instance = path.substring(path.lastIndexOf('/') + 1);
        if (_versionsByInstance.containsKey(instance)) {
          Map<String, String> versions = new HashMap<>(_versionsByInstance);
          versions.put(instance, readVersion(instance));
          publish(versions);
        }
      }
    } catch (Exception e) {
      disableUntilRefresh(e);
    }
  }

  @Override
  public synchronized void onLiveInstanceChange(List<LiveInstance> instances, NotificationContext context) {
    if (finalizeOrIgnore(context) || !_supportedVersion) {
      return;
    }
    try {
      refreshTopology(context.getType() == NotificationContext.Type.INIT || !_initialized);
      _initialized = true;
    } catch (Exception e) {
      disableUntilRefresh(e);
    }
  }

  private void disableUntilRefresh(Exception e) {
    _supported = false;
    _initialized = false;
    LOGGER.warn("Failed to refresh instance versions; compatibility gate disabled until a complete refresh. "
        + "Retaining cached versions: {}", _versionsByInstance, e);
  }

  private boolean finalizeOrIgnore(NotificationContext context) {
    NotificationContext.Type type = context.getType();
    if (type == NotificationContext.Type.FINALIZE) {
      _versionsByInstance = Map.of();
      _active = false;
      _initialized = false;
      _supported = false;
      LOGGER.info("Cluster version tracking finalized; compatibility gate disabled");
      return true;
    }
    if (type == NotificationContext.Type.INIT) {
      _active = true;
      return false;
    }
    return type != NotificationContext.Type.CALLBACK || !_active;
  }

  private void refreshTopology(boolean refreshExisting) {
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    List<String> instances = Objects.requireNonNull(accessor.getChildNames(accessor.keyBuilder().liveInstances()),
        "Live instance snapshot is unavailable");
    Map<String, String> versions = new HashMap<>();
    for (String instance : instances) {
      if (InstanceTypeUtils.isBroker(instance) || InstanceTypeUtils.isServer(instance)) {
        String version = !refreshExisting && _versionsByInstance.containsKey(instance)
            ? _versionsByInstance.get(instance) : readVersion(instance);
        versions.put(instance, version);
      }
    }
    publish(versions);
  }

  @Nullable
  private String readVersion(String instance) {
    InstanceConfig config = _manager.getClusterManagmentTool().getInstanceConfig(_manager.getClusterName(), instance);
    return config != null ? config.getRecord().getStringField(CommonConstants.Helix.Instance.PINOT_VERSION_KEY, null)
        : null;
  }

  private void publish(Map<String, String> versions) {
    boolean foundBroker = false;
    boolean foundServer = false;
    Map<String, String> blockers = new TreeMap<>();
    for (Map.Entry<String, String> entry : versions.entrySet()) {
      foundBroker |= InstanceTypeUtils.isBroker(entry.getKey());
      foundServer |= InstanceTypeUtils.isServer(entry.getKey());
      if (entry.getValue() == null || PinotVersion.UNKNOWN.equals(entry.getValue())
          || !entry.getValue().equals(_version)) {
        blockers.put(entry.getKey(), entry.getValue());
      }
    }
    boolean supported = (!_requireBrokerAndServer || foundBroker && foundServer) && blockers.isEmpty();
    boolean changed = !_initialized || _supported != supported || !_versionsByInstance.equals(versions);
    _versionsByInstance = versions;
    _supported = supported;
    if (changed) {
      LOGGER.info("Cluster version compatibility {} (expected version: {}, live broker: {}, "
              + "live server: {}, blocking instances: {})", supported ? "enabled" : "disabled", _version,
          foundBroker, foundServer, blockers);
    }
  }
}
