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
package org.apache.pinot.common.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.api.listeners.ClusterConfigChangeListener;
import org.apache.helix.model.ClusterConfig;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.config.provider.PinotClusterConfigProvider;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Bridges Helix cluster config callbacks to [PinotClusterConfigChangeListener]s. Instance configs captured at
/// construction override cluster configs in the effective view exposed to listeners and [#getClusterConfigs()]. All
/// access is serialized on this instance, so a listener sees the snapshot handed to it at registration and every later
/// change in order.
@BatchMode(enabled = false)
public class DefaultClusterConfigChangeHandler implements ClusterConfigChangeListener, PinotClusterConfigProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultClusterConfigChangeHandler.class);
  private static final Comparator<String> CONFIG_KEY_COMPARATOR =
      Comparator.comparing(DefaultClusterConfigChangeHandler::normalizeConfigKey);

  private final List<PinotClusterConfigChangeListener> _listeners = new ArrayList<>();
  private final Map<String, String> _instanceConfigs;
  private Map<String, String> _effectiveConfigs;

  public DefaultClusterConfigChangeHandler() {
    this(Map.of());
  }

  /// Snapshots the instance config at construction time. Callers must construct the handler before folding cluster
  /// configs into the instance config.
  /// @param instanceConfig instance config to snapshot
  public DefaultClusterConfigChangeHandler(PinotConfiguration instanceConfig) {
    this(snapshot(instanceConfig));
  }

  DefaultClusterConfigChangeHandler(Map<String, String> instanceConfigs) {
    _instanceConfigs = immutableConfigMap(instanceConfigs);
    _effectiveConfigs = _instanceConfigs;
  }

  @Override
  public synchronized void onClusterConfigChange(ClusterConfig clusterConfig, NotificationContext context) {
    Map<String, String> clusterConfigs = copyWithoutNullValues(clusterConfig.getRecord().getSimpleFields());
    Map<String, String> effectiveConfigs = getEffectiveConfigs(clusterConfigs);
    Set<String> changedConfigs = getChangedProperties(_effectiveConfigs, effectiveConfigs);
    LOGGER.info("Cluster configs changed: {}", changedConfigs);
    _effectiveConfigs = effectiveConfigs;
    for (PinotClusterConfigChangeListener listener : _listeners) {
      listener.onChange(changedConfigs, effectiveConfigs);
    }
  }

  @Override
  public synchronized Map<String, String> getClusterConfigs() {
    return _effectiveConfigs;
  }

  @Override
  public synchronized boolean registerClusterConfigChangeListener(PinotClusterConfigChangeListener listener) {
    LOGGER.info("Registering cluster config change listener: {}", listener.getClass().getName());
    _listeners.add(listener);
    // Treat every key as newly added so that the listener picks up the current values
    listener.onChange(_effectiveConfigs.keySet(), _effectiveConfigs);
    return true;
  }

  private Map<String, String> getEffectiveConfigs(Map<String, String> clusterConfigs) {
    Map<String, String> effectiveConfigs = new TreeMap<>(CONFIG_KEY_COMPARATOR);
    effectiveConfigs.putAll(_instanceConfigs);
    for (Map.Entry<String, String> entry : clusterConfigs.entrySet()) {
      effectiveConfigs.putIfAbsent(entry.getKey(), entry.getValue());
    }
    return Collections.unmodifiableMap(effectiveConfigs);
  }

  private static Map<String, String> snapshot(PinotConfiguration instanceConfig) {
    Map<String, String> snapshot = new TreeMap<>(CONFIG_KEY_COMPARATOR);
    for (String key : instanceConfig.getKeys()) {
      String value = instanceConfig.getProperty(key);
      if (value != null) {
        snapshot.put(key, value);
      }
    }
    return snapshot;
  }

  private static Map<String, String> immutableConfigMap(Map<String, String> configs) {
    Map<String, String> copy = new TreeMap<>(CONFIG_KEY_COMPARATOR);
    for (Map.Entry<String, String> entry : configs.entrySet()) {
      if (entry.getValue() != null) {
        copy.put(entry.getKey(), entry.getValue());
      }
    }
    return Collections.unmodifiableMap(copy);
  }

  // PinotConfiguration applies this normalization to instance config keys. Using the same ordering here lets a
  // normalized snapshot override the original spelling of a ZK key and preserves relaxed lookups for listeners.
  private static String normalizeConfigKey(String key) {
    return key.replace("-", "").replace("_", "").toLowerCase();
  }
}
