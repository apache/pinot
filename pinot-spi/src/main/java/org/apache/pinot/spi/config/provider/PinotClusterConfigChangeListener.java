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

import java.util.Map;
import java.util.Set;


/**
 * Change listener for obtaining ZK cluster config changes. Must be registered with {@link PinotClusterConfigProvider}
 */
public interface PinotClusterConfigChangeListener {
  /**
   * On change callback to handle changes to the cluster configs
   * @param changedConfigs set of configs that were changed (added/deleted/modified)
   * @param clusterConfigs map of all the cluster configs
   */
  void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs);
}
