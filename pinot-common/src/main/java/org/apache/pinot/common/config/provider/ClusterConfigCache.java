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
package org.apache.pinot.common.config.provider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.config.provider.PinotClusterConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Backend-neutral cache of cluster config snapshots. All access and listener callbacks are serialized on this
/// instance, so registration delivers the current snapshot before any later change. Backends supply complete snapshots
/// in order; this cache does not establish backend revisions or watch consistency.
public class ClusterConfigCache implements PinotClusterConfigProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterConfigCache.class);

  private final List<PinotClusterConfigChangeListener> _listeners = new ArrayList<>();
  private Map<String, String> _clusterConfigs = Map.of();

  /// Replaces the current snapshot and notifies registered listeners, including when no keys changed. Null-valued
  /// entries are treated as absent. The supplied map is copied before publication.
  public synchronized void updateClusterConfigs(Map<String, String> clusterConfigs) {
    Map<String, String> snapshot = copyWithoutNullValues(clusterConfigs);
    Set<String> changedConfigs = getChangedProperties(_clusterConfigs, snapshot);
    LOGGER.info("Cluster configs changed: {}", changedConfigs);
    _clusterConfigs = snapshot;
    for (PinotClusterConfigChangeListener listener : _listeners) {
      listener.onChange(changedConfigs, snapshot);
    }
  }

  @Override
  public synchronized Map<String, String> getClusterConfigs() {
    return _clusterConfigs;
  }

  @Override
  public synchronized boolean registerClusterConfigChangeListener(PinotClusterConfigChangeListener listener) {
    LOGGER.info("Registering cluster config change listener: {}", listener.getClass().getName());
    _listeners.add(listener);
    // Treat every key as newly added so that the listener picks up the current values
    listener.onChange(_clusterConfigs.keySet(), _clusterConfigs);
    return true;
  }
}
