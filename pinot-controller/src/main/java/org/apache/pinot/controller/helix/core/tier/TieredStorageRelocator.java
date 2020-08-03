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
package org.apache.pinot.controller.helix.core.tier;

import com.google.common.base.Preconditions;
import java.util.concurrent.ExecutorService;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.pinot.common.assignment.InstanceAssignmentConfigUtils;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfigConstants;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalancer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Periodic task to run rebalancer in background to relocate segments to storage tiers
 * TODO: we could potentially get rid of tagOverrideConfig and rely on this relocator for moving COMPLETED segments
 */
public class TieredStorageRelocator extends ControllerPeriodicTask<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TieredStorageRelocator.class);

  private final ExecutorService _executorService;

  public TieredStorageRelocator(PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ControllerConf config, ControllerMetrics controllerMetrics,
      ExecutorService executorService) {
    super(TieredStorageRelocator.class.getSimpleName(), config.getTieredStorageRelocatorFrequencyInSeconds(),
        config.getTieredStorageRelocatorInitialDelayInSeconds(), pinotHelixResourceManager, leadControllerManager,
        controllerMetrics);
    _executorService = executorService;
  }

  @Override
  protected void processTable(String tableNameWithType) {

    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: {}", tableNameWithType);

    // Tiered storage doesn't apply to HLC
    if (TableNameBuilder.isRealtimeTableResource(tableNameWithType) && new StreamConfig(tableNameWithType,
        tableConfig.getIndexingConfig().getStreamConfigs()).hasHighLevelConsumerType()) {
      return;
    }

    if (!InstanceAssignmentConfigUtils.shouldRelocateToTiers(tableConfig)) {
      LOGGER.debug("Relocation of segments to storage tiers not enabled for table: {}", tableNameWithType);
      return;
    }

    LOGGER.info("Relocating segments table: {} to storage tiers", tableNameWithType);
    // Allow at most one replica unavailable during relocation
    Configuration rebalanceConfig = new BaseConfiguration();
    rebalanceConfig.addProperty(RebalanceConfigConstants.MIN_REPLICAS_TO_KEEP_UP_FOR_NO_DOWNTIME, -1);
    // Run rebalance asynchronously
    _executorService.submit(() -> {
      try {
        RebalanceResult rebalance =
            new TableRebalancer(_pinotHelixResourceManager.getHelixZkManager()).rebalance(tableConfig, rebalanceConfig);
        switch (rebalance.getStatus()) {
          case NO_OP:
            LOGGER.info("All segments are already relocated to storage tiers for table: {}", tableNameWithType);
            break;
          case DONE:
            LOGGER.info("Finished relocating segments to storage tiers for table: {}", tableNameWithType);
            break;
          default:
            LOGGER.error("Relocation to storage tiers failed for table: {}", tableNameWithType);
        }
      } catch (Throwable t) {
        LOGGER.error("Caught exception/error while rebalancing table: {}", tableNameWithType, t);
      }
    });
  }
}
