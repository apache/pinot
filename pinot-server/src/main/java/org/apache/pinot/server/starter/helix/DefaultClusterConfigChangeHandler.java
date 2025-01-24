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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.api.listeners.ClusterConfigChangeListener;
import org.apache.helix.model.ClusterConfig;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.config.provider.PinotClusterConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@BatchMode(enabled = false)
public class DefaultClusterConfigChangeHandler implements ClusterConfigChangeListener, PinotClusterConfigProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultClusterConfigChangeHandler.class);

  private volatile Map<String, String> _properties;
  private final List<PinotClusterConfigChangeListener> _clusterConfigChangeListeners;

  public DefaultClusterConfigChangeHandler() {
    _properties = null;
    _clusterConfigChangeListeners = new ArrayList<>();
  }

  @Override
  public void onClusterConfigChange(ClusterConfig clusterConfig, NotificationContext notificationContext) {
    LOGGER.info("Handling Cluster ConfigChanges: CALLBACK START");
    process(clusterConfig.getRecord().getSimpleFields());
    LOGGER.info("Handling Cluster ConfigChanges: CALLBACK DONE");
  }

  private synchronized void process(Map<String, String> properties) {
    Set<String> changedProperties = getChangedProperties(_properties, properties);
    _properties = properties;
    _clusterConfigChangeListeners.forEach(l -> l.onChange(changedProperties, _properties));
  }

  @Override
  public Map<String, String> getClusterConfigs() {
    return _properties;
  }

  @Override
  public boolean registerClusterConfigChangeListener(PinotClusterConfigChangeListener clusterConfigChangeListener) {
    _clusterConfigChangeListeners.add(clusterConfigChangeListener);
    LOGGER.info("Registering clusterConfigChangeListener: {}", clusterConfigChangeListener.getClass().getName());
    // On registration, we want all keys to be treated as newly added, so pass changed properties as the keySet()
    clusterConfigChangeListener.onChange(_properties.keySet(), _properties);
    return true;
  }
}
