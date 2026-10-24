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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.config.provider.PinotClusterConfigProvider;


/// Delivers complete native cluster config snapshots with recovery tracked separately for each listener.
/// Registration, reads, and callbacks are serialized on this instance. Failed callbacks retain attempted keys because
/// they may have applied only part of a change; callers retry by supplying a complete snapshot again.
public final class RecoverableClusterConfigCache implements PinotClusterConfigProvider {
  private final List<Delivery> _listeners = new ArrayList<>();
  private Map<String, String> _clusterConfigs = Map.of();

  /// Publishes a detached snapshot, treating null values as absent, and delivers it to listeners in registration order.
  /// A failure stops delivery; the next update retries missed additions, changes, and removals for each listener.
  public synchronized void updateClusterConfigs(Map<String, String> clusterConfigs) {
    Map<String, String> snapshot = copyWithoutNullValues(clusterConfigs);
    _clusterConfigs = snapshot;
    for (Delivery delivery : _listeners) {
      delivery.deliver(snapshot);
    }
  }

  @Override
  public synchronized Map<String, String> getClusterConfigs() {
    return _clusterConfigs;
  }

  @Override
  public synchronized boolean registerClusterConfigChangeListener(PinotClusterConfigChangeListener listener) {
    Delivery delivery = new Delivery(Objects.requireNonNull(listener, "listener"));
    // Keep the listener registered if its initial callback fails, so a later complete snapshot can recover it.
    _listeners.add(delivery);
    delivery.deliver(_clusterConfigs);
    return true;
  }

  private final class Delivery {
    private final PinotClusterConfigChangeListener _listener;
    private Map<String, String> _delivered = Map.of();
    private final Set<String> _pending = new HashSet<>();

    private Delivery(PinotClusterConfigChangeListener listener) {
      _listener = listener;
    }

    // Accessed only under the enclosing cache's monitor, including the initial registration callback.
    private void deliver(Map<String, String> configs) {
      _pending.addAll(getChangedProperties(_delivered, configs));
      // Retain attempted keys on failure: the callback may have applied some of them before throwing.
      _listener.onChange(Set.copyOf(_pending), configs);
      _delivered = configs;
      _pending.clear();
    }
  }
}
