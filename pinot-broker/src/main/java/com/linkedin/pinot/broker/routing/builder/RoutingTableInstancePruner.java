/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.broker.routing.builder;

import com.linkedin.pinot.common.utils.CommonConstants;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Routing table instance pruner, which is used to remove inactive and disabled instances during routing table creation.
 */
public class RoutingTableInstancePruner {
  private static final Logger LOGGER = LoggerFactory.getLogger(RoutingTableInstancePruner.class);

  private final Map<String, InstanceConfig> _instanceConfigMap = new HashMap<>();

  public RoutingTableInstancePruner(List<InstanceConfig> instanceConfigs) {
    for (InstanceConfig instanceConfig : instanceConfigs) {
      _instanceConfigMap.put(instanceConfig.getInstanceName(), instanceConfig);
    }
  }

  /**
   * Returns True iff:
   * - The given instance is disabled in Helix.
   * - The instance is being shutdown.
   * False otherwise
   *
   * @param instanceName Name of instance to check.
   * @return True if instance is disabled in helix, or is being shutdown, False otherwise.
   */
  public boolean isInactive(String instanceName) {
    InstanceConfig instanceConfig = _instanceConfigMap.get(instanceName);
    if (instanceConfig == null) {
      LOGGER.error("Instance config for instance '{}' does not exist", instanceName);
      return true;
    }

    if (!instanceConfig.getInstanceEnabled()) {
      LOGGER.info("Instance '{}' is disabled", instanceName);
      return true;
    }

    if (Boolean.parseBoolean(
        instanceConfig.getRecord().getSimpleField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS))) {
      LOGGER.info("Instance '{}' is shutting down", instanceName);
      return true;
    }

    return false;
  }
}
