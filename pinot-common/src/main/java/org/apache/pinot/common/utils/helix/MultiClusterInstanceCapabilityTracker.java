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
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Tracks the capabilities of the instances of one type across several Helix clusters, for example the local cluster
/// of a broker and the remote clusters that it federates. It holds one [InstanceCapabilityTracker] for each cluster
/// that it watches.
///
/// [#getMinRevision] returns the minimum revision across all the clusters. A cluster that the caller adds with
/// [#addUnavailableCluster] (for example a remote cluster that failed to connect), or whose tracker failed to
/// register, counts as revision 0. This is fail-safe: a lower minimum only keeps the plan that every version can run.
/// Without any cluster, the minimum is 0 too.
///
/// An unavailable cluster stays at revision 0 for the life of this instance: nothing retries the connection or the
/// registration. For a broker, a remote cluster that fails to connect at startup pins the minimum to 0 until the
/// broker restarts. Both cases are logged at WARN or ERROR with the cluster name.
///
/// Lifecycle: add the clusters with [#addCluster] and [#addUnavailableCluster] while the Helix managers are
/// connected, then share the instance. [#close] removes the Helix listeners. Call it before the Helix managers
/// disconnect.
///
/// Thread safety: the clusters are kept in copy-on-write collections, so query threads may call [#getMinRevision]
/// at any time, also while clusters are added. [#getMinRevision] never blocks.
public class MultiClusterInstanceCapabilityTracker {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiClusterInstanceCapabilityTracker.class);

  private final InstanceType _instanceType;
  private final List<InstanceCapability> _capabilities;
  private final List<InstanceCapabilityTracker> _trackers = new CopyOnWriteArrayList<>();
  private final Set<String> _unavailableClusters = new CopyOnWriteArraySet<>();

  /// Tracks the capabilities of [InstanceCapability#values()] for the instances of `instanceType`.
  public MultiClusterInstanceCapabilityTracker(InstanceType instanceType) {
    this(instanceType, InstanceCapability.values());
  }

  /// Tracks the given capabilities instead of [InstanceCapability#values()].
  @VisibleForTesting
  MultiClusterInstanceCapabilityTracker(InstanceType instanceType, Collection<InstanceCapability> capabilities) {
    _instanceType = instanceType;
    _capabilities = List.copyOf(capabilities);
  }

  /// Watches the cluster of the given connected Helix manager: creates an [InstanceCapabilityTracker] and registers it
  /// as an instance config change listener. When the registration fails, the cluster counts as revision 0.
  public void addCluster(HelixManager helixManager) {
    addCluster(new InstanceCapabilityTracker(helixManager, _instanceType, _capabilities));
  }

  @VisibleForTesting
  void addCluster(InstanceCapabilityTracker tracker) {
    String clusterName = tracker.getClusterName();
    try {
      tracker.getHelixManager().addInstanceConfigChangeListener(tracker);
      _trackers.add(tracker);
      LOGGER.info("Tracking the capabilities of the {} instances of cluster: {}", _instanceType, clusterName);
    } catch (Exception e) {
      LOGGER.error("Failed to register the InstanceCapabilityTracker for cluster: {}, treating the cluster as "
          + "supporting no capability until restart", clusterName, e);
      _unavailableClusters.add(clusterName);
    }
  }

  /// Adds a cluster that cannot be watched, for example a remote cluster that failed to connect. It counts as
  /// revision 0 for every capability.
  public void addUnavailableCluster(String clusterName) {
    LOGGER.warn("Cluster: {} is unavailable, treating it as supporting no capability until restart", clusterName);
    _unavailableClusters.add(clusterName);
  }

  /// The minimum revision of the capability across the instances of all the clusters. 0 when a cluster is
  /// unavailable, when an instance does not support the capability, or when no cluster is tracked. Never blocks.
  public int getMinRevision(InstanceCapability capability) {
    if (!_unavailableClusters.isEmpty() || _trackers.isEmpty()) {
      return 0;
    }
    int minRevision = Integer.MAX_VALUE;
    for (InstanceCapabilityTracker tracker : _trackers) {
      minRevision = Math.min(minRevision, tracker.getMinRevision(capability));
    }
    return minRevision;
  }

  /// Removes the Helix listeners of every tracker. Call it before the Helix managers disconnect.
  public void close() {
    for (InstanceCapabilityTracker tracker : _trackers) {
      HelixManager helixManager = tracker.getHelixManager();
      try {
        helixManager.removeListener(new PropertyKey.Builder(tracker.getClusterName()).instanceConfigs(), tracker);
      } catch (Exception e) {
        LOGGER.warn("Failed to remove the InstanceCapabilityTracker of cluster: {}", tracker.getClusterName(), e);
      }
    }
    _trackers.clear();
  }
}
