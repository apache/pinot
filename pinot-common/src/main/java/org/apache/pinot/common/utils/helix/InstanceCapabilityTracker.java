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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Computes, for each capability, the minimum revision across a set of instances of one type, see
/// [InstanceCapability].
///
/// The tracker does not read ZooKeeper and does not listen to Helix. Its owner calls [#update] with the instance
/// config records that it already reads, for example the broker routing manager on each instance config change. Each
/// call replaces the whole set: the tracker keeps the records of the tracked type and ignores the others. The owner
/// decides which instances count. A wider set is fail-safe: an instance that never receives the work can only lower
/// the minimum, and a lower minimum only keeps the plan that every version can run.
///
/// Each instance advertises its capabilities in the map field `capabilities` of its instance config
/// ([CommonConstants.Helix.Instance#CAPABILITIES_KEY]). The tracker reads every entry of the map, so it also tracks
/// the capabilities that an extension defines (see [InstanceCapability]). A missing map, a missing key or a malformed
/// value counts as revision 0 (not supported). [#getMinRevision] returns 0 before the first [#update] and while the
/// set has no instance of the tracked type.
///
/// Rollback guard: the map also holds the reserved entry `pinotVersion`
/// ([CommonConstants.Helix.Instance#CAPABILITIES_PINOT_VERSION_KEY]), the Pinot version that wrote the capabilities.
/// The tracker reads the capabilities of an instance only when that entry is string-equal to the simple field
/// `pinotVersion` ([CommonConstants.Helix.Instance#PINOT_VERSION_KEY]) of the same instance config. Otherwise it treats
/// the instance as supporting no capability (revision 0). Every Pinot version rewrites the simple field at startup,
/// but a version that predates the capabilities does not clear the map. So after a rollback to such a version the two
/// values differ, and the stale capabilities are ignored. This is an equality check between two fields of one instance
/// config. It does not order versions, and it cannot detect a rollback between two builds with the same version
/// string. Residual gap: `PinotHelixResourceManager.updateInstance` reads the instance config and writes the full
/// record back without a version check. When an instance restarts on another version between that read and that
/// write, the write restores the previous simple field `pinotVersion` and the previous map, so stale capabilities can
/// be trusted. The simple field `pinotVersion` has the same problem today.
///
/// Logging: when the parsed capabilities of the set change, the tracker logs at INFO the instances below the revision
/// that this version supports, for the capabilities given to the constructor, at most [#MAX_LOGGED_INSTANCES] per
/// list. It lists separately the instances whose capabilities are ignored because the version stamp does not match.
/// It logs a malformed value at WARN when the capabilities of that instance change. A call that changes nothing logs
/// nothing.
///
/// Thread safety: [#update] is `synchronized`. It publishes an immutable snapshot of the minimum revisions through a
/// `volatile` field, so [#getMinRevision] never blocks and sees the result of the last [#update].
public class InstanceCapabilityTracker {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceCapabilityTracker.class);
  /// The maximum number of instance ids that the tracker logs for each list.
  static final int MAX_LOGGED_INSTANCES = 20;
  /// The revision stored for a malformed value. Counts as 0.
  private static final int MALFORMED_REVISION = -1;

  private final String _clusterName;
  private final InstanceType _instanceType;
  private final List<InstanceCapability> _loggedCapabilities;
  // Guarded by this. For each tracked instance, the revision of each capability that it advertises.
  private Map<String, Map<String, Integer>> _revisionsByInstance = Map.of();
  // Guarded by this. The instances whose capabilities are ignored because the version stamp does not match.
  private Set<String> _staleInstances = Set.of();

  private volatile Map<String, Integer> _minRevisions = Map.of();

  /// Tracks the instances of `instanceType` in the given cluster. Logs the lagging instances for the capabilities of
  /// [InstanceCapability#values()].
  public InstanceCapabilityTracker(String clusterName, InstanceType instanceType) {
    this(clusterName, instanceType, InstanceCapability.values());
  }

  /// Same as [#InstanceCapabilityTracker(String, InstanceType)], but logs the lagging instances for the given
  /// capabilities, for example the capabilities of Pinot and those of an extension. Every capability that the
  /// instances advertise is tracked either way.
  public InstanceCapabilityTracker(String clusterName, InstanceType instanceType,
      Collection<InstanceCapability> loggedCapabilities) {
    _clusterName = clusterName;
    _instanceType = instanceType;
    List<InstanceCapability> capabilities = new ArrayList<>();
    for (InstanceCapability capability : loggedCapabilities) {
      if (capability.instanceType() == instanceType) {
        capabilities.add(capability);
      }
    }
    _loggedCapabilities = List.copyOf(capabilities);
  }

  /// The minimum revision of the capability across the tracked instances, 0 when one of them does not support it or
  /// when there is no tracked instance. Never blocks.
  public int getMinRevision(InstanceCapability capability) {
    return _minRevisions.getOrDefault(capability.name(), 0);
  }

  /// Replaces the set of instances with the given instance config records. Keeps the records of the tracked type and
  /// ignores the others. Never throws because of the content of a record.
  public synchronized void update(Collection<ZNRecord> instanceConfigRecords) {
    Map<String, Map<String, Integer>> revisionsByInstance = new HashMap<>();
    Set<String> staleInstances = new TreeSet<>();
    for (ZNRecord record : instanceConfigRecords) {
      String instanceId = record.getId();
      if (InstanceTypeUtils.getInstanceType(instanceId) != _instanceType) {
        continue;
      }
      if (hasStaleCapabilities(record)) {
        staleInstances.add(instanceId);
      }
      Map<String, Integer> revisions = readRevisions(record);
      revisionsByInstance.put(instanceId, revisions);
      if (revisions.containsValue(MALFORMED_REVISION) && !revisions.equals(_revisionsByInstance.get(instanceId))) {
        LOGGER.warn("Malformed capability revisions for instance: {} of cluster: {}, treating them as 0: {}",
            instanceId, _clusterName, record.getMapField(CommonConstants.Helix.Instance.CAPABILITIES_KEY));
      }
    }
    if (revisionsByInstance.equals(_revisionsByInstance) && staleInstances.equals(_staleInstances)) {
      return;
    }
    _revisionsByInstance = revisionsByInstance;
    _staleInstances = staleInstances;
    Map<String, Integer> minRevisions = computeMinRevisions(revisionsByInstance);
    if (!minRevisions.equals(_minRevisions)) {
      LOGGER.info("Minimum capability revisions across {} {} instances of cluster: {} changed from {} to {}",
          revisionsByInstance.size(), _instanceType, _clusterName, _minRevisions, minRevisions);
      _minRevisions = minRevisions;
    }
    logLaggingInstances();
  }

  /// The sorted ids of the tracked instances whose capabilities are ignored because the version stamp does not match.
  @VisibleForTesting
  synchronized List<String> getStaleInstances() {
    return List.copyOf(_staleInstances);
  }

  /// For each capability that some instance advertises, the minimum revision across all the instances. A capability
  /// with minimum 0 has no entry.
  private static Map<String, Integer> computeMinRevisions(Map<String, Map<String, Integer>> revisionsByInstance) {
    if (revisionsByInstance.isEmpty()) {
      return Map.of();
    }
    Map<String, Integer> minRevisions = new HashMap<>();
    // Every instance must advertise a capability, so the candidates are the capabilities of any one instance.
    for (String name : revisionsByInstance.values().iterator().next().keySet()) {
      int minRevision = Integer.MAX_VALUE;
      for (Map<String, Integer> revisions : revisionsByInstance.values()) {
        minRevision = Math.min(minRevision, revisions.getOrDefault(name, 0));
      }
      if (minRevision > 0) {
        minRevisions.put(name, minRevision);
      }
    }
    return Map.copyOf(minRevisions);
  }

  /// Logs at INFO, for each logged capability, the instances below the revision that this version supports, in two
  /// lists: the instances that advertise no or an older revision, and the instances whose capabilities are ignored
  /// because the version stamp does not match.
  private void logLaggingInstances() {
    Map<String, Map<String, Integer>> sortedRevisions = new TreeMap<>(_revisionsByInstance);
    for (InstanceCapability capability : _loggedCapabilities) {
      List<String> olderInstances = new ArrayList<>();
      List<String> ignoredInstances = new ArrayList<>();
      for (Map.Entry<String, Map<String, Integer>> entry : sortedRevisions.entrySet()) {
        if (entry.getValue().getOrDefault(capability.name(), 0) < capability.revision()) {
          (_staleInstances.contains(entry.getKey()) ? ignoredInstances : olderInstances).add(entry.getKey());
        }
      }
      if (olderInstances.isEmpty() && ignoredInstances.isEmpty()) {
        continue;
      }
      LOGGER.info("{} {} instances of cluster: {} are below revision {} of capability {}. With no or an older revision "
              + "({}): {}. With capabilities ignored because of a version stamp mismatch ({}): {}",
          olderInstances.size() + ignoredInstances.size(), _instanceType, _clusterName, capability.revision(),
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

  /// Reads the revision of every capability that an instance advertises, by name. Returns an empty map (revision 0
  /// for every capability) when the instance config has no capabilities, or when the capabilities were written by
  /// another Pinot version than the one in the simple field `pinotVersion`. See the rollback guard in the class
  /// Javadoc. A malformed value is stored as [#MALFORMED_REVISION], which counts as 0.
  @VisibleForTesting
  static Map<String, Integer> readRevisions(@Nullable ZNRecord record) {
    if (record == null) {
      return Map.of();
    }
    Map<String, String> mapField = record.getMapField(CommonConstants.Helix.Instance.CAPABILITIES_KEY);
    if (mapField == null || hasStaleCapabilities(record)) {
      return Map.of();
    }
    Map<String, Integer> revisions = new HashMap<>();
    for (Map.Entry<String, String> entry : mapField.entrySet()) {
      String name = entry.getKey();
      if (name.equals(CommonConstants.Helix.Instance.CAPABILITIES_PINOT_VERSION_KEY)) {
        continue;
      }
      int revision;
      try {
        revision = entry.getValue() != null ? Integer.parseInt(entry.getValue().trim()) : MALFORMED_REVISION;
      } catch (NumberFormatException e) {
        revision = MALFORMED_REVISION;
      }
      revisions.put(name, revision < 0 ? MALFORMED_REVISION : revision);
    }
    return revisions;
  }

  /// Whether the instance config has a capabilities map that was not written by the Pinot version in its simple field
  /// `pinotVersion`: the version stamp is missing or not string-equal. Not an ordering of versions.
  private static boolean hasStaleCapabilities(ZNRecord record) {
    Map<String, String> mapField = record.getMapField(CommonConstants.Helix.Instance.CAPABILITIES_KEY);
    if (mapField == null) {
      return false;
    }
    String writtenByVersion = mapField.get(CommonConstants.Helix.Instance.CAPABILITIES_PINOT_VERSION_KEY);
    return writtenByVersion == null
        || !writtenByVersion.equals(record.getSimpleField(CommonConstants.Helix.Instance.PINOT_VERSION_KEY));
  }
}
