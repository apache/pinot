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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Tracks the capabilities that the instances of one type advertise, see [InstanceCapability]. It tracks the
/// capabilities of [InstanceCapability#values()].
///
/// The tracker is a Helix [InstanceConfigChangeListener]. Register it with
/// `HelixManager.addInstanceConfigChangeListener`. Each instance advertises its capabilities in the map field
/// `capabilities` of its instance config ([CommonConstants.Helix.Instance#CAPABILITIES_KEY]). For each capability,
/// [#getMinRevision] returns the minimum revision across all the instances of the tracked type in the cluster. A
/// missing map, a missing key or a malformed value counts as revision 0 (not supported). A malformed value is logged
/// as a warning. Instances of other types are ignored.
///
/// The minimum covers every instance config of the tracked type in the cluster, including the instances that are
/// disabled, not live, or serve other tenants. Such an instance can lower the minimum although it never receives the
/// query. This is fail-safe: a lower minimum only keeps the plan that every version can run.
///
/// Rollback guard: the map also holds the reserved entry `pinotVersion`
/// ([CommonConstants.Helix.Instance#CAPABILITIES_PINOT_VERSION_KEY]), the Pinot version that wrote the capabilities.
/// The tracker reads the capabilities of an instance only when that entry is string-equal to the simple field
/// `pinotVersion` ([CommonConstants.Helix.Instance#PINOT_VERSION_KEY]) of the same instance config. Otherwise it treats
/// the instance as supporting no capability (revision 0). Every Pinot version rewrites the simple field at startup,
/// but a version that predates the capabilities does not clear the map. So after a rollback to such a version the two
/// values differ, and the stale capabilities are ignored. This is an equality check between two fields of one instance
/// config. It does not order versions and it does not compare the versions of different instances.
/// Residual gap: `PinotHelixResourceManager.updateInstance` reads the instance config and writes the full record back
/// without a version check. When an instance restarts on another version between that read and that write, the write
/// restores the previous simple field `pinotVersion` and the previous map, so stale capabilities can be trusted. The
/// simple field `pinotVersion` has the same problem today. A follow-up can make the controller keep the fields that
/// the instance owns.
///
/// [#getMinRevision] returns 0 before the first notification and while the cluster has no instance of the tracked
/// type. Helix sends the `INIT` notification when the listener is registered.
///
/// When the instance config of one instance cannot be read on a `CALLBACK` notification (for example because of a
/// transient ZooKeeper error), the instance counts as revision 0 until its instance config changes again or a new
/// session sends `INIT`. This is fail-safe.
///
/// When the instances below the revision that this version supports ([InstanceCapability#getRevision()]) change, the
/// tracker logs their ids at INFO, at most [#MAX_LOGGED_INSTANCES] per list. It lists separately the instances that
/// advertise no or an older revision and the instances whose capabilities are ignored because the version stamp does
/// not match. It never logs on the query path.
///
/// Thread safety: Helix callback threads call [#onInstanceConfigChange] and query threads call [#getMinRevision]. The
/// notification handling is `synchronized` and owns the per-instance bookkeeping. It publishes an immutable snapshot
/// of the minimum revisions through a `volatile` field, so readers never block and see the result of the last
/// processed notification.
@BatchMode(enabled = false)
@PreFetch(enabled = false)
public class InstanceCapabilityTracker implements InstanceConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceCapabilityTracker.class);
  /// The maximum number of instance ids that the tracker logs for each capability.
  static final int MAX_LOGGED_INSTANCES = 20;

  private final HelixManager _helixManager;
  private final String _clusterName;
  private final InstanceType _instanceType;
  private final List<InstanceCapability> _capabilities;
  // Guarded by this
  private final Map<String, Map<InstanceCapability, Integer>> _revisionsByInstance = new HashMap<>();
  // Guarded by this. The instances whose capabilities are ignored because the version stamp does not match.
  private final Set<String> _staleInstances = new HashSet<>();
  // Guarded by this. For each capability, the sorted ids of the instances below InstanceCapability.getRevision(),
  // and the sorted ids of the stale instances, as last logged.
  private Map<InstanceCapability, List<String>> _laggingInstances = Map.of();
  private List<String> _loggedStaleInstances = List.of();

  private HelixAdmin _helixAdmin;
  private volatile Map<InstanceCapability, Integer> _minRevisions = Map.of();

  /// Tracks the instances of `instanceType`. The Helix manager does not need to be connected yet. It must be connected
  /// before the tracker is registered.
  public InstanceCapabilityTracker(HelixManager helixManager, InstanceType instanceType) {
    this(helixManager, instanceType, InstanceCapability.values());
  }

  /// Tracks the given capabilities instead of [InstanceCapability#values()].
  @VisibleForTesting
  InstanceCapabilityTracker(HelixManager helixManager, InstanceType instanceType,
      Collection<InstanceCapability> capabilities) {
    _helixManager = helixManager;
    _clusterName = helixManager.getClusterName();
    _instanceType = instanceType;
    _capabilities = List.copyOf(capabilities);
  }

  /// The Helix manager of the tracked cluster.
  public HelixManager getHelixManager() {
    return _helixManager;
  }

  /// The name of the tracked cluster.
  public String getClusterName() {
    return _clusterName;
  }

  /// The minimum revision of the capability across all the instances of the tracked type, 0 when one of them does not
  /// support it or when the cluster has no such instance. Never blocks.
  public int getMinRevision(InstanceCapability capability) {
    return _minRevisions.getOrDefault(capability, 0);
  }

  @Override
  public synchronized void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
    if (_helixAdmin == null) {
      _helixAdmin = _helixManager.getClusterManagmentTool();
    }
    NotificationContext.Type type = context.getType();
    if (type != NotificationContext.Type.INIT && type != NotificationContext.Type.CALLBACK) {
      LOGGER.warn("Ignoring notification type: {} for instance config change", type);
      return;
    }
    if (type == NotificationContext.Type.INIT || context.getIsChildChange()) {
      // Instances may have been added or removed: read them all.
      _revisionsByInstance.clear();
      _staleInstances.clear();
      for (String instance : _helixAdmin.getInstancesInCluster(_clusterName)) {
        if (isTrackedType(instance)) {
          try {
            putInstance(instance, _helixAdmin.getInstanceConfig(_clusterName, instance));
          } catch (Exception e) {
            LOGGER.warn("Failed to get instance config for instance: {}, treating it as supporting no capability",
                instance, e);
            putInstance(instance, null);
          }
        }
      }
    } else {
      String pathChanged = context.getPathChanged();
      String instanceName = pathChanged.substring(pathChanged.lastIndexOf('/') + 1);
      if (isTrackedType(instanceName)) {
        try {
          putInstance(instanceName, _helixAdmin.getInstanceConfig(_clusterName, instanceName));
        } catch (Exception e) {
          // Keep the instance as supporting no capability, as the INIT path does: a read failure must not raise the
          // minimum. When the instance was removed, the child change notification removes it.
          LOGGER.warn("Failed to get instance config for instance: {}, treating it as supporting no capability",
              instanceName, e);
          putInstance(instanceName, null);
        }
      }
    }
    Map<InstanceCapability, Integer> minRevisions = computeMinRevisions();
    if (!minRevisions.equals(_minRevisions)) {
      LOGGER.info("Minimum capability revisions across {} {} instances of cluster: {} changed from {} to {}",
          _revisionsByInstance.size(), _instanceType, _clusterName, _minRevisions, minRevisions);
      _minRevisions = minRevisions;
    }
    logLaggingInstancesIfChanged();
  }

  /// The sorted ids of the tracked instances whose capabilities are ignored because the version stamp does not match.
  @VisibleForTesting
  synchronized List<String> getStaleInstances() {
    return _staleInstances.stream().filter(_revisionsByInstance::containsKey).sorted().collect(Collectors.toList());
  }

  /// Stores the revisions of one instance, and whether its capabilities are ignored because of the version stamp. A
  /// null instance config (it could not be read) counts as supporting no capability.
  private void putInstance(String instanceName, @Nullable InstanceConfig instanceConfig) {
    _revisionsByInstance.put(instanceName, readRevisions(instanceConfig, _capabilities));
    if (instanceConfig != null && hasStaleCapabilities(instanceConfig)) {
      _staleInstances.add(instanceName);
    } else {
      _staleInstances.remove(instanceName);
    }
  }

  /// Logs at INFO, when they changed since the last log, the instances below the revision that this version supports,
  /// in two lists: the instances that advertise no or an older revision, and the instances whose capabilities are
  /// ignored because the version stamp does not match.
  private void logLaggingInstancesIfChanged() {
    Map<InstanceCapability, List<String>> laggingInstances = computeLaggingInstances();
    List<String> staleInstances = getStaleInstances();
    if (laggingInstances.equals(_laggingInstances) && staleInstances.equals(_loggedStaleInstances)) {
      return;
    }
    _laggingInstances = laggingInstances;
    _loggedStaleInstances = staleInstances;
    for (InstanceCapability capability : _capabilities) {
      if (capability.getInstanceType() != _instanceType) {
        continue;
      }
      List<String> olderInstances = new ArrayList<>();
      List<String> ignoredInstances = new ArrayList<>();
      for (String instance : laggingInstances.getOrDefault(capability, List.of())) {
        (_staleInstances.contains(instance) ? ignoredInstances : olderInstances).add(instance);
      }
      LOGGER.info("{} {} instances of cluster: {} are below revision {} of capability {}. With no or an older revision "
              + "({}): {}. With capabilities ignored because of a version stamp mismatch ({}): {}",
          olderInstances.size() + ignoredInstances.size(), _instanceType, _clusterName, capability.getRevision(),
          capability, olderInstances.size(), truncate(olderInstances), ignoredInstances.size(),
          truncate(ignoredInstances));
    }
  }

  private static String truncate(List<String> instances) {
    if (instances.size() <= MAX_LOGGED_INSTANCES) {
      return instances.toString();
    }
    return instances.subList(0, MAX_LOGGED_INSTANCES) + " ...";
  }

  /// For each capability of the tracked type, the sorted ids of the instances below the revision that this version
  /// supports. A capability without such instances has no entry.
  private Map<InstanceCapability, List<String>> computeLaggingInstances() {
    Map<InstanceCapability, List<String>> laggingInstances = new HashMap<>();
    Map<String, Map<InstanceCapability, Integer>> sortedRevisions = new TreeMap<>(_revisionsByInstance);
    for (InstanceCapability capability : _capabilities) {
      if (capability.getInstanceType() != _instanceType) {
        continue;
      }
      List<String> instances = new ArrayList<>();
      for (Map.Entry<String, Map<InstanceCapability, Integer>> entry : sortedRevisions.entrySet()) {
        if (entry.getValue().getOrDefault(capability, 0) < capability.getRevision()) {
          instances.add(entry.getKey());
        }
      }
      if (!instances.isEmpty()) {
        laggingInstances.put(capability, List.copyOf(instances));
      }
    }
    return Map.copyOf(laggingInstances);
  }

  private Map<InstanceCapability, Integer> computeMinRevisions() {
    if (_revisionsByInstance.isEmpty()) {
      return Map.of();
    }
    Map<InstanceCapability, Integer> minRevisions = new HashMap<>();
    for (InstanceCapability capability : _capabilities) {
      int minRevision = Integer.MAX_VALUE;
      for (Map<InstanceCapability, Integer> revisions : _revisionsByInstance.values()) {
        minRevision = Math.min(minRevision, revisions.getOrDefault(capability, 0));
      }
      minRevisions.put(capability, minRevision);
    }
    return Map.copyOf(minRevisions);
  }

  /// Reads the revisions of the given capabilities that an instance advertises. Returns an empty map (revision 0 for
  /// every capability) when the instance config has no capabilities, or when the capabilities were written by another
  /// Pinot version than the one in the simple field `pinotVersion`. See the rollback guard in the class Javadoc.
  @VisibleForTesting
  static Map<InstanceCapability, Integer> readRevisions(@Nullable InstanceConfig instanceConfig,
      Collection<InstanceCapability> capabilities) {
    if (instanceConfig == null) {
      return Map.of();
    }
    ZNRecord record = instanceConfig.getRecord();
    Map<String, String> mapField = record.getMapField(CommonConstants.Helix.Instance.CAPABILITIES_KEY);
    if (mapField == null) {
      return Map.of();
    }
    if (hasStaleCapabilities(instanceConfig)) {
      return Map.of();
    }
    Map<InstanceCapability, Integer> revisions = new HashMap<>();
    for (InstanceCapability capability : capabilities) {
      String value = mapField.get(capability.name());
      if (value == null) {
        continue;
      }
      int revision;
      try {
        revision = Integer.parseInt(value.trim());
      } catch (NumberFormatException e) {
        revision = -1;
      }
      if (revision < 0) {
        LOGGER.warn("Malformed revision: '{}' of capability: {} for instance: {}, treating it as 0", value,
            capability, instanceConfig.getInstanceName());
        continue;
      }
      revisions.put(capability, revision);
    }
    return revisions;
  }

  /// Whether the instance config has a capabilities map that was not written by the Pinot version in its simple field
  /// `pinotVersion`: the version stamp is missing or not string-equal. Not an ordering of versions.
  private static boolean hasStaleCapabilities(InstanceConfig instanceConfig) {
    ZNRecord record = instanceConfig.getRecord();
    Map<String, String> mapField = record.getMapField(CommonConstants.Helix.Instance.CAPABILITIES_KEY);
    if (mapField == null) {
      return false;
    }
    String writtenByVersion = mapField.get(CommonConstants.Helix.Instance.CAPABILITIES_PINOT_VERSION_KEY);
    return writtenByVersion == null
        || !writtenByVersion.equals(record.getSimpleField(CommonConstants.Helix.Instance.PINOT_VERSION_KEY));
  }

  private boolean isTrackedType(String instanceName) {
    return InstanceTypeUtils.getInstanceType(instanceName) == _instanceType;
  }
}
