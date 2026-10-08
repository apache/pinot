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
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Tracks whether live brokers and servers report the local Pinot version. Register both listener interfaces.
/// Config data changes read only the changed live participant; membership changes read only new, restarted or
/// unreadable participants. Membership updates read all live records to detect coalesced same-name restarts, but reuse
/// cached participant configs. INIT and config child changes refresh the complete live snapshot. Missing versions and
/// read failures fail closed until a subsequent notification refreshes them. Synchronized callbacks protect the cache;
/// query threads read a snapshot published once after each update, without locking or consulting Helix.
@BatchMode(enabled = false)
@PreFetch(enabled = false)
public class ClusterVersionTracker implements InstanceConfigChangeListener, LiveInstanceChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterVersionTracker.class);
  private static final Snapshot UNKNOWN = new Snapshot(false, false, Map.of());

  private final HelixManager _manager;
  private final String _version;
  private Map<String, InstanceVersion> _versionsById = new HashMap<>();
  private boolean _initialized;
  private boolean _stopped;
  private volatile Snapshot _snapshot = UNKNOWN;

  public ClusterVersionTracker(HelixManager manager) {
    this(manager, PinotVersion.VERSION);
  }

  public ClusterVersionTracker(HelixManager manager, String version) {
    _manager = manager;
    _version = version;
  }

  public boolean isSameVersion() {
    return _snapshot._sameVersion;
  }

  public boolean isSameVersionWithBrokerAndServer() {
    return _snapshot._sameVersionWithBrokerAndServer;
  }

  /// Permanently disables the tracker if its listeners could not both be registered.
  public synchronized void stop() {
    _stopped = true;
    reset();
  }

  @Override
  public synchronized void onInstanceConfigChange(List<InstanceConfig> configs, NotificationContext context) {
    if (_stopped) {
      return;
    }
    if (!isUpdate(context)) {
      reset();
      return;
    }
    try {
      if (!_initialized || context.getType() == NotificationContext.Type.INIT || context.getIsChildChange()) {
        refreshLiveInstances(configs, true, null);
      } else {
        String path = context.getPathChanged();
        if (path == null) {
          throw new IllegalArgumentException("Missing changed instance config path");
        }
        String instance = path.substring(path.lastIndexOf('/') + 1);
        InstanceVersion previous = _versionsById.get(instance);
        if (previous != null) {
          _versionsById.put(instance, new InstanceVersion(previous._sessionId, getVersion(instance, configs)));
        }
      }
      publish();
    } catch (Exception e) {
      failClosed(e);
    }
  }

  @Override
  @PreFetch(enabled = false)
  public synchronized void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext context) {
    if (_stopped) {
      return;
    }
    if (!isUpdate(context)) {
      reset();
      return;
    }
    try {
      Objects.requireNonNull(liveInstances, "Missing live instance snapshot");
      // Keep the read inside this failure boundary: Helix prefetch failures would bypass the listener entirely.
      refreshLiveInstances(List.of(), false, liveInstances.isEmpty() ? null : liveInstances);
      publish();
    } catch (Exception e) {
      failClosed(e);
    }
  }

  private static boolean isUpdate(NotificationContext context) {
    return context.getType() == NotificationContext.Type.INIT || context.getType() == NotificationContext.Type.CALLBACK;
  }

  private void refreshLiveInstances(List<InstanceConfig> configs, boolean refreshVersions,
      @Nullable List<LiveInstance> liveInstances) {
    if (liveInstances == null) {
      HelixDataAccessor accessor = _manager.getHelixDataAccessor();
      liveInstances = Objects.requireNonNull(accessor.getChildValues(accessor.keyBuilder().liveInstances(), true),
          "Cannot read live instances");
    }
    Map<String, InstanceVersion> versions = new HashMap<>();
    for (LiveInstance liveInstance : liveInstances) {
      String instance = liveInstance.getInstanceName();
      InstanceType type = InstanceTypeUtils.getInstanceType(instance);
      if (type == InstanceType.BROKER || type == InstanceType.SERVER) {
        InstanceVersion previous = _versionsById.get(instance);
        String session = liveInstance.getEphemeralOwner();
        String version = !refreshVersions && previous != null && Objects.equals(session, previous._sessionId)
            ? previous._version : null;
        versions.put(instance, new InstanceVersion(session, version != null ? version : getVersion(instance, configs)));
      }
    }
    _versionsById = versions;
    _initialized = true;
  }

  @Nullable
  private String getVersion(String instance, List<InstanceConfig> configs) {
    try {
      InstanceConfig config = null;
      for (InstanceConfig prefetched : configs) {
        if (instance.equals(prefetched.getInstanceName())) {
          config = prefetched;
          break;
        }
      }
      if (config == null) {
        config = _manager.getClusterManagmentTool().getInstanceConfig(_manager.getClusterName(), instance);
      }
      return config.getRecord().getStringField(CommonConstants.Helix.Instance.PINOT_VERSION_KEY, null);
    } catch (Exception e) {
      LOGGER.warn("Cannot read version for live instance {}; disabling version-dependent features", instance, e);
      return null;
    }
  }

  private void publish() {
    boolean sameVersion = _version != null && !_version.equals(PinotVersion.UNKNOWN);
    boolean foundBroker = false;
    boolean foundServer = false;
    Map<String, String> incompatibleVersions = new HashMap<>();
    for (Map.Entry<String, InstanceVersion> entry : _versionsById.entrySet()) {
      String version = entry.getValue()._version;
      if (!Objects.equals(_version, version)) {
        sameVersion = false;
        incompatibleVersions.put(entry.getKey(), version);
      }
      InstanceType type = InstanceTypeUtils.getInstanceType(entry.getKey());
      foundBroker |= type == InstanceType.BROKER;
      foundServer |= type == InstanceType.SERVER;
    }
    boolean changed = _snapshot == UNKNOWN || _snapshot._sameVersion != sameVersion
        || !_snapshot._incompatibleVersions.equals(incompatibleVersions);
    _snapshot = new Snapshot(sameVersion, sameVersion && foundBroker && foundServer, incompatibleVersions);
    if (changed) {
      LOGGER.info("Live broker/server versions match {}: {} (incompatible versions: {})", _version, sameVersion,
          incompatibleVersions);
    }
  }

  private void failClosed(Exception e) {
    reset();
    LOGGER.warn("Cannot refresh live broker/server versions; disabling version-dependent features", e);
  }

  private void reset() {
    _initialized = false;
    _versionsById = new HashMap<>();
    _snapshot = UNKNOWN;
  }

  private static class Snapshot {
    private final boolean _sameVersion;
    private final boolean _sameVersionWithBrokerAndServer;
    private final Map<String, String> _incompatibleVersions;

    Snapshot(boolean sameVersion, boolean sameVersionWithBrokerAndServer, Map<String, String> incompatibleVersions) {
      _sameVersion = sameVersion;
      _sameVersionWithBrokerAndServer = sameVersionWithBrokerAndServer;
      _incompatibleVersions = incompatibleVersions;
    }
  }

  private static class InstanceVersion {
    private final String _sessionId;
    @Nullable
    private final String _version;

    InstanceVersion(String sessionId, @Nullable String version) {
      _sessionId = sessionId;
      _version = version;
    }
  }
}
