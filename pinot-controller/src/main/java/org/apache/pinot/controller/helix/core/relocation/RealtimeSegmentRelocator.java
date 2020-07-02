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
package org.apache.pinot.controller.helix.core.relocation;

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
 * Periodic task to run rebalancer in background to relocate COMPLETED segments for LLC real-time table. Allow at most
 * one replica unavailable during rebalance.
 */
public class RealtimeSegmentRelocator extends ControllerPeriodicTask<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeSegmentRelocator.class);

  private final ExecutorService _executorService;

  public RealtimeSegmentRelocator(PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ControllerConf config, ControllerMetrics controllerMetrics,
      ExecutorService executorService) {
    super("RealtimeSegmentRelocator", getRunFrequencySeconds(config.getRealtimeSegmentRelocatorFrequency()),
        config.getRealtimeSegmentRelocationInitialDelayInSeconds(), pinotHelixResourceManager, leadControllerManager,
        controllerMetrics);
    _executorService = executorService;
  }

  @Override
  protected void processTable(String tableNameWithType) {
    // Only relocate segments for LLC real-time table
    if (!TableNameBuilder.isRealtimeTableResource(tableNameWithType)) {
      return;
    }
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: {}", tableNameWithType);
    if (new StreamConfig(tableNameWithType, tableConfig.getIndexingConfig().getStreamConfigs())
        .hasHighLevelConsumerType()) {
      return;
    }
    if (!InstanceAssignmentConfigUtils.shouldRelocateCompletedSegments(tableConfig)) {
      LOGGER.debug("No need to relocate COMPLETED segments for table: {}", tableNameWithType);
      return;
    }

    LOGGER.info("Relocating COMPLETED segments for table: {}", tableNameWithType);
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
            LOGGER.info("All COMPLETED segments are already relocated for table: {}", tableNameWithType);
            break;
          case DONE:
            LOGGER.info("Finished relocating COMPLETED segments for table: {}", tableNameWithType);
            break;
          default:
            LOGGER.error("Relocation failed for table: {}", tableNameWithType);
        }
      } catch (Throwable t) {
        LOGGER.error("Caught exception/error while rebalancing table: {}", tableNameWithType, t);
      }
    });
  }

  private static long getRunFrequencySeconds(String timeStr) {
    long seconds;
    try {
      Long millis = TimeUtils.convertPeriodToMillis(timeStr);
      seconds = millis / 1000;
    } catch (Exception e) {
      throw new RuntimeException("Invalid time spec '" + timeStr + "' (Valid examples: '3h', '4h30m', '30m')", e);
    }
    return seconds;
  }
}
