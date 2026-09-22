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
package org.apache.pinot.query.service.dispatch;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Decides whether a multi-stage query ships its leaf-stage segment lists as native protobuf fields of the worker
/// metadata, or as the legacy JSON custom property.
///
/// Only servers decode those fields, and a server that predates them finds no segments, treats the worker as an
/// intermediate-stage worker and fails the leaf stage. The mode, set by
/// [CommonConstants.Broker#CONFIG_OF_MSE_PROTO_SEGMENT_LIST], picks how that risk is handled:
///
/// - [Mode#SAFE] (the default): proto only while every server of the cluster reports exactly this broker's Pinot
///   version in its Helix instance config, so the encoding turns itself on when a rolling upgrade completes and off
///   again as soon as an older server joins, with no operator involved. It fails closed: a server with a missing,
///   `UNKNOWN` or unreadable version counts as outdated, the encoding stays off until the first instance-config
///   delivery, and multi-cluster queries always use the legacy encoding because the servers of remote clusters are
///   not watched. Any version other than this broker's counts as outdated, including a newer one: telling which
///   versions understand the fields would mean parsing Pinot version strings, so a heterogeneous cluster
///   conservatively stays on the legacy encoding until it becomes homogeneous again.
/// - [Mode#ALWAYS]: proto unconditionally. For clusters that do not publish versions, forks that carry the fields
///   under a different version string, and tests.
/// - [Mode#NEVER]: legacy JSON unconditionally. The kill switch.
///
/// The mode is read from cluster config, falling back to the static broker config and then to
/// [CommonConstants.Broker#DEFAULT_MSE_PROTO_SEGMENT_LIST]. Cluster config wins and is applied to the next query, so
/// an operator can switch the encoding off without restarting the brokers; clearing the key restores the static
/// broker config. A value that is not a mode is ignored with a warning, leaving the current mode in place.
///
/// Modeled on [org.apache.pinot.query.runtime.SendStatsPredicate], which gates the MSE stats on the same signal, with
/// two deliberate differences: only servers are checked, since brokers never decode the fields, and an unreadable
/// instance config counts as outdated rather than current, since the cost of a wrong answer here is a failed query
/// rather than missing stats.
///
/// Every instance that is not a controller, broker or minion counts as a server, so an instance config left behind
/// by a decommissioned old server keeps [Mode#SAFE] on the legacy encoding until it is removed; the outdated servers
/// are logged whenever the encoding switches.
///
/// Thread-safety: [#isEnabled] reads two `volatile` fields and is lock-free on the request path. Instance-config and
/// cluster-config deliveries are serialized on `this`, which also publishes the Helix handles set by
/// [#watchInstanceConfigs].
@ThreadSafe
@BatchMode(enabled = false)
@PreFetch(enabled = false)
public class ProtoSegmentListPredicate implements InstanceConfigChangeListener, PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProtoSegmentListPredicate.class);
  private static final String KEY = CommonConstants.Broker.CONFIG_OF_MSE_PROTO_SEGMENT_LIST;
  /// Cap on the outdated servers named in one log line, so that a large rolling upgrade does not log the whole fleet.
  private static final int MAX_LOGGED_SERVERS = 10;

  public enum Mode {
    NEVER, SAFE, ALWAYS
  }

  /// The mode of the static broker config, used until cluster config says otherwise and again when the cluster-config
  /// key is cleared.
  private final Mode _staticMode;
  /// The mode in force, which cluster config can change at runtime.
  private volatile Mode _mode;
  private final String _currentVersion;
  /// SAFE only: servers whose version is not [#_currentVersion], mapped to the version they report. Guarded by `this`.
  private final Map<String, String> _outdatedServers = new HashMap<>();
  /// SAFE only: `false` until the first instance-config delivery shows that every server is current.
  private volatile boolean _allServersCurrent;
  /// SAFE only: set by [#watchInstanceConfigs]. Guarded by `this`.
  @Nullable
  private HelixAdmin _helixAdmin;
  @Nullable
  private String _clusterName;

  public ProtoSegmentListPredicate(Mode mode) {
    this(mode, PinotVersion.VERSION);
  }

  @VisibleForTesting
  ProtoSegmentListPredicate(Mode mode, String currentVersion) {
    _staticMode = mode;
    _mode = mode;
    _currentVersion = currentVersion;
  }

  /// Reads the mode from the static broker configuration, failing fast on a value that is not a mode.
  public static ProtoSegmentListPredicate create(PinotConfiguration brokerConf) {
    String value = brokerConf.getProperty(KEY, CommonConstants.Broker.DEFAULT_MSE_PROTO_SEGMENT_LIST);
    Mode mode;
    try {
      mode = Mode.valueOf(value.trim().toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value '" + value + "' for " + KEY + ", expected one of NEVER, SAFE, ALWAYS", e);
    }
    LOGGER.info("Initialized {} with mode: {}", KEY, mode);
    return new ProtoSegmentListPredicate(mode);
  }

  public Mode getMode() {
    return _mode;
  }

  /// Whether a query uses the proto encoding. `multiClusterQuery` is whether the query routes to other clusters,
  /// whose server versions this predicate cannot see.
  public boolean isEnabled(boolean multiClusterQuery) {
    switch (_mode) {
      case ALWAYS:
        return true;
      case SAFE:
        return !multiClusterQuery && _allServersCurrent;
      default:
        return false;
    }
  }

  /// Starts watching the server versions of the cluster. Registered whatever the current mode is, because cluster
  /// config can switch the mode to [Mode#SAFE] at runtime. `helixManager` must already be connected. A registration
  /// failure is logged rather than thrown: it leaves [Mode#SAFE] on the legacy encoding, which is always correct, and
  /// an optimization must not fail broker startup.
  public void watchInstanceConfigs(HelixManager helixManager) {
    try {
      // Published under the monitor that deliveries synchronize on, but registered outside it: Helix may deliver the
      // initial notification on another thread while registration is still in progress.
      synchronized (this) {
        _helixAdmin = helixManager.getClusterManagmentTool();
        _clusterName = helixManager.getClusterName();
      }
      helixManager.addInstanceConfigChangeListener(this);
      LOGGER.info("Watching server versions for {}", KEY);
    } catch (Exception e) {
      LOGGER.error("Failed to watch server versions for {}, leaving the legacy segment list encoding on", KEY, e);
    }
  }

  /// Applies a mode set in cluster config, which wins over the static broker config and takes effect on the next
  /// query. Clearing the key restores the static broker config.
  @Override
  public synchronized void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (!changedConfigs.contains(KEY)) {
      return;
    }
    String value = clusterConfigs.get(KEY);
    Mode mode;
    if (value == null || value.isBlank()) {
      mode = _staticMode;
    } else {
      try {
        mode = Mode.valueOf(value.trim().toUpperCase(Locale.ENGLISH));
      } catch (IllegalArgumentException e) {
        // Keep the mode in force: a typo must not silently move a cluster off the encoding an operator chose.
        LOGGER.warn("Ignoring invalid value '{}' for {}, expected one of NEVER, SAFE, ALWAYS, staying on: {}", value,
            KEY, _mode);
        return;
      }
    }
    Mode previous = _mode;
    if (mode == previous) {
      return;
    }
    _mode = mode;
    LOGGER.info("Updated {} from: {} to: {}", KEY, previous, mode);
  }

  @Override
  public synchronized void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
    NotificationContext.Type type = context.getType();
    if (type != NotificationContext.Type.INIT && type != NotificationContext.Type.CALLBACK) {
      return;
    }
    HelixAdmin helixAdmin = _helixAdmin;
    if (helixAdmin == null) {
      LOGGER.warn("Ignoring instance config change delivered before {} was watched", KEY);
      return;
    }
    String pathChanged = context.getPathChanged();
    if (type == NotificationContext.Type.INIT || context.getIsChildChange() || pathChanged == null) {
      // Instances were added or removed, this is the first delivery, or the changed path is unknown: rebuild the
      // whole view.
      Map<String, String> serverVersions = new HashMap<>();
      for (String instanceId : helixAdmin.getInstancesInCluster(_clusterName)) {
        if (isServer(instanceId)) {
          serverVersions.put(instanceId, readVersion(helixAdmin, instanceId));
        }
      }
      refreshAllServers(serverVersions);
    } else {
      // A single instance config changed, e.g. a server restarted on a new version and republished it.
      String instanceId = pathChanged.substring(pathChanged.lastIndexOf('/') + 1);
      if (isServer(instanceId)) {
        refreshServer(instanceId, readVersion(helixAdmin, instanceId));
      }
    }
  }

  /// Replaces the whole view with the versions reported by `serverVersions`, keyed by server instance id.
  @VisibleForTesting
  synchronized void refreshAllServers(Map<String, String> serverVersions) {
    _outdatedServers.clear();
    for (Map.Entry<String, String> entry : serverVersions.entrySet()) {
      if (isOutdated(entry.getValue())) {
        _outdatedServers.put(entry.getKey(), String.valueOf(entry.getValue()));
      }
    }
    updateAllServersCurrent();
  }

  /// Updates the view for one server whose instance config changed.
  @VisibleForTesting
  synchronized void refreshServer(String instanceId, @Nullable String version) {
    if (isOutdated(version)) {
      _outdatedServers.put(instanceId, String.valueOf(version));
    } else {
      _outdatedServers.remove(instanceId);
    }
    updateAllServersCurrent();
  }

  private void updateAllServersCurrent() {
    boolean allServersCurrent = _outdatedServers.isEmpty();
    if (allServersCurrent == _allServersCurrent) {
      return;
    }
    _allServersCurrent = allServersCurrent;
    if (allServersCurrent) {
      LOGGER.info("Every server reports version {}, enabling the proto segment list encoding", _currentVersion);
    } else {
      LOGGER.info("{} server(s) do not report version {}, using the legacy segment list encoding: {}",
          _outdatedServers.size(), _currentVersion, describeOutdatedServers());
    }
  }

  private String describeOutdatedServers() {
    return _outdatedServers.entrySet().stream().limit(MAX_LOGGED_SERVERS)
        .map(entry -> entry.getKey() + "=" + entry.getValue())
        .collect(Collectors.joining(", ", "[", _outdatedServers.size() > MAX_LOGGED_SERVERS ? ", ...]" : "]"));
  }

  private boolean isOutdated(@Nullable String version) {
    return version == null || version.equals(PinotVersion.UNKNOWN) || !version.equals(_currentVersion);
  }

  private static boolean isServer(String instanceId) {
    return InstanceTypeUtils.getInstanceType(instanceId) == InstanceType.SERVER;
  }

  /// The version the instance publishes in its instance config, or `null` when it publishes none or its config
  /// cannot be read. Both read as outdated.
  @Nullable
  private String readVersion(HelixAdmin helixAdmin, String instanceId) {
    try {
      InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(_clusterName, instanceId);
      return instanceConfig != null
          ? instanceConfig.getRecord().getStringField(CommonConstants.Helix.Instance.PINOT_VERSION_KEY, null) : null;
    } catch (Exception e) {
      LOGGER.warn("Failed to read the instance config of server: {}, treating it as outdated", instanceId, e);
      return null;
    }
  }
}
