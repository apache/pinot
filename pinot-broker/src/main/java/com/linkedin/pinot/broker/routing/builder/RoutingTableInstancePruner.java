/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

  private Map<String, InstanceConfig> instanceConfigMap;

  public RoutingTableInstancePruner(List<InstanceConfig> instanceConfigList) {
    instanceConfigMap = new HashMap<String, InstanceConfig>();
    for (InstanceConfig config : instanceConfigList) {
      instanceConfigMap.put(config.getInstanceName(), config);
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
    if (!instanceConfigMap.containsKey(instanceName)) {
      return true;
    }

    // If the instance is not enabled, return false.
    InstanceConfig instanceConfig = instanceConfigMap.get(instanceName);
    if (!instanceConfig.getInstanceEnabled()) {
      LOGGER.info("Instance '{}' is disabled in the config map.", instanceName);
      return true;
    }

    boolean status = false;
    if (instanceConfig.getRecord().getSimpleField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS) != null) {
      try {
        if (instanceConfig.getRecord() == null) {
          LOGGER.info("Config record not found for instance '{}'.", instanceName);
          return true;
        }
        if (instanceConfig.getRecord()
            .getSimpleField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS) != null) {
          status =
              Boolean.valueOf(instanceConfig.getRecord()
                  .getSimpleField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS));
          if (status == true) {
            LOGGER.info("found an instance : '{}' in shutting down state", instanceName);
          }
        }
      } catch (Exception e) {
        LOGGER.error("unknown value found while parsing boolean isShuttingDownField ", e);
      }
    }

    return status;
  }
}
