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
package org.apache.pinot.spi.config.provider;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * Interface for ZK cluster config providers. Will be registered with Helix to listen on cluster config changes and
 * will propagate changes to all registered listeners
 */
public interface PinotClusterConfigProvider {
  /**
   * Get the cluster configs
   * @return map of cluster configs
   */
  Map<String, String> getClusterConfigs();

  /**
   * Register cluster config change listener
   * @param clusterConfigChangeListener change listener to be registered to obtain cluster config changes
   * @return returns 'true' if the registration was successful
   */
  boolean registerClusterConfigChangeListener(PinotClusterConfigChangeListener clusterConfigChangeListener);

  /**
   * Calculates the set of keys that changed in ZK cluster configs between the old and new
   * @param oldProperties map of previously cached ZK cluster configs
   * @param newProperties map of newly fetched ZK cluster configs
   * @return set of changed (added/deleted/updated) cluster config keys
   */
  default Set<String> getChangedProperties(Map<String, String> oldProperties, Map<String, String> newProperties) {
    if (oldProperties == null || oldProperties.isEmpty()) {
      return newProperties.keySet();
    }

    if (newProperties == null || newProperties.isEmpty()) {
      return oldProperties.keySet();
    }

    Set<String> changedProperties = new HashSet<>();
    // Add all properties that are newly added or whose value changed
    for (Map.Entry<String, String> entry : newProperties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (!oldProperties.containsKey(key) || !oldProperties.get(key).equals(value)) {
        changedProperties.add(key);
      }
    }

    // Add all properties that were deleted
    Set<String> originalPropertyKeys = oldProperties.keySet();
    originalPropertyKeys.removeAll(changedProperties);
    changedProperties.addAll(originalPropertyKeys);

    return changedProperties;
  }
}
