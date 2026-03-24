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
package org.apache.pinot.controller.validation;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.core.periodictask.PeriodicTask;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Rebuilds the broker resource if the instance set has changed
 */
public class BrokerResourceValidationManager extends ControllerPeriodicTask<BrokerResourceValidationManager.Context> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerResourceValidationManager.class);

  public BrokerResourceValidationManager(ControllerConf config, PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ControllerMetrics controllerMetrics) {
    super("BrokerResourceValidationManager", config.getBrokerResourceValidationFrequencyInSeconds(),
        config.getBrokerResourceValidationInitialDelayInSeconds(), pinotHelixResourceManager, leadControllerManager,
        controllerMetrics);
  }

  @Override
  protected List<String> getTablesToProcess(Properties periodicTaskProperties) {
    List<String> tables = super.getTablesToProcess(periodicTaskProperties);
    if (periodicTaskProperties.get(PeriodicTask.PROPERTY_KEY_TABLE_NAME) != null) {
      return tables;
    }
    List<String> combined = new ArrayList<>(tables);
    combined.addAll(_pinotHelixResourceManager.getBrokerResourceLogicalTables());
    return combined;
  }

  @Override
  protected Context preprocess(Properties periodicTaskProperties) {
    Context context = new Context();
    context._instanceConfigs = _pinotHelixResourceManager.getAllHelixInstanceConfigs();
    return context;
  }

  @Override
  protected void processTable(String tableNameWithType, Context context) {
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    if (tableConfig != null) {
      Set<String> brokerInstances = _pinotHelixResourceManager
          .getAllInstancesForBrokerTenant(context._instanceConfigs, tableConfig.getTenantConfig().getBroker());
      _pinotHelixResourceManager.rebuildBrokerResource(tableNameWithType, brokerInstances);
      return;
    }
    LogicalTableConfig logicalTableConfig = _pinotHelixResourceManager.getLogicalTableConfig(tableNameWithType);
    if (logicalTableConfig != null) {
      Set<String> brokerInstances = _pinotHelixResourceManager
          .getAllInstancesForBrokerTenant(context._instanceConfigs, logicalTableConfig.getBrokerTenant());
      _pinotHelixResourceManager.rebuildBrokerResource(tableNameWithType, brokerInstances);
      return;
    }
    LOGGER.warn("No table config or logical table config found for: {}, skipping broker resource validation",
        tableNameWithType);
  }

  public static final class Context {
    private List<InstanceConfig> _instanceConfigs;
  }
}
