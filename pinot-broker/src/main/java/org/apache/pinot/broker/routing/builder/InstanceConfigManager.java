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
package org.apache.pinot.broker.routing.builder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.CommonConstants.Helix;
import org.apache.pinot.common.utils.HashUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code InstanceConfigManager} class manages instance configs during routing table creation.
 */
public class InstanceConfigManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceConfigManager.class);

  private final Map<String, InstanceConfig> _instanceConfigMap;

  public InstanceConfigManager(List<InstanceConfig> instanceConfigs) {
    _instanceConfigMap = new HashMap<>(HashUtil.getHashMapCapacity(instanceConfigs.size()));
    for (InstanceConfig instanceConfig : instanceConfigs) {
      _instanceConfigMap.put(instanceConfig.getInstanceName(), instanceConfig);
    }
  }

  /**
   * Returns the instance config for the given instance if it is active, or {@code null} otherwise.
   * <p>Instance is active iff:
   * <ul>
   *   <li>Instance exists in the instance config map</li>
   *   <li>Instance is enabled in Helix</li>
   *   <li>Instance does not have {@link Helix#IS_SHUTDOWN_IN_PROGRESS} or {@link Helix#QUERIES_DISABLED} set</li>
   * </ul>
   */
  @Nullable
  public InstanceConfig getActiveInstanceConfig(String instanceName) {
    InstanceConfig instanceConfig = _instanceConfigMap.get(instanceName);
    if (instanceConfig == null) {
      LOGGER.warn("Instance config for instance '{}' does not exist", instanceName);
      return null;
    }

    if (!instanceConfig.getInstanceEnabled()) {
      LOGGER.info("Instance '{}' is disabled", instanceName);
      return null;
    }

    if (Boolean.parseBoolean(instanceConfig.getRecord().getSimpleField(Helix.IS_SHUTDOWN_IN_PROGRESS))) {
      LOGGER.info("Instance '{}' is shutting down", instanceName);
      return null;
    }

    if (Boolean.parseBoolean(instanceConfig.getRecord().getSimpleField(Helix.QUERIES_DISABLED))) {
      LOGGER.info("Instance '{}' has disabled queries", instanceName);
      return null;
    }

    return instanceConfig;
  }
}
