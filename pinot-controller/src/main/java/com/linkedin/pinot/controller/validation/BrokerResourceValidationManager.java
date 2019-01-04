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
package com.linkedin.pinot.controller.validation;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import java.util.List;
import java.util.Set;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Rebuilds the broker resource if the instance set has changed
 */
public class BrokerResourceValidationManager extends ControllerPeriodicTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerResourceValidationManager.class);

  private List<InstanceConfig> _instanceConfigs;

  public BrokerResourceValidationManager(ControllerConf config, PinotHelixResourceManager pinotHelixResourceManager) {
    super("BrokerResourceValidationManager", config.getBrokerResourceValidationFrequencyInSeconds(),
        pinotHelixResourceManager);
  }

  @Override
  protected void preprocess() {
    // Cache instance configs to reduce ZK access
    _instanceConfigs = _pinotHelixResourceManager.getAllHelixInstanceConfigs();
  }

  @Override
  protected void processTable(String tableNameWithType) {
    try {
      TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
      if (tableConfig == null) {
        LOGGER.warn("Failed to find table config for table: {}, skipping broker resource validation", tableNameWithType);
        return;
      }

      // Rebuild broker resource
      Set<String> brokerInstances = _pinotHelixResourceManager.getAllInstancesForBrokerTenant(_instanceConfigs,
          tableConfig.getTenantConfig().getBroker());
      _pinotHelixResourceManager.rebuildBrokerResource(tableNameWithType, brokerInstances);
    } catch (Exception e) {
      LOGGER.warn("Caught exception while validating broker resource for table: {}", tableNameWithType, e);
    }
  }


  @Override
  protected void postprocess() {

  }

  @Override
  protected void initTask() {

  }

  @Override
  public void stopTask() {
  }
}
