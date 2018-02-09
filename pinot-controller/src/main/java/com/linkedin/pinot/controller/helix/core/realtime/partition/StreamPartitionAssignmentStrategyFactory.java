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

package com.linkedin.pinot.controller.helix.core.realtime.partition;

import com.linkedin.pinot.common.config.StreamConsumptionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory class to construct the {@link StreamPartitionAssignmentStrategy} depending on the {@link StreamConsumptionConfig} defined in the table config
 */
public class StreamPartitionAssignmentStrategyFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamPartitionAssignmentStrategyFactory.class);

  // TODO: We default to Uniform strategy, until we resolve
  // the race conditions that we might encounter in Balanced strategy,
  // when trying to change partition assignment of multiple tables at once
  private static final StreamPartitionAssignmentStrategyEnum DEFAULT_STRATEGY =
      StreamPartitionAssignmentStrategyEnum.UniformStreamPartitionAssignment;

  /**
   * Given a table config, returns the {@link StreamPartitionAssignmentStrategy} that should be used for the partitions of that table's stream
   * @param tableConfig
   * @return
   */
  public static StreamPartitionAssignmentStrategy getStreamPartitionAssignmentStrategy(TableConfig tableConfig) {
    StreamPartitionAssignmentStrategy streamPartitionAssignmentStrategy = null;

    String partitionAssignmentStrategyName = null;
    StreamConsumptionConfig consumptionConfig = tableConfig.getIndexingConfig().getStreamConsumptionConfig();
    if (consumptionConfig != null) {
      partitionAssignmentStrategyName = consumptionConfig.getStreamPartitionAssignmentStrategy();
    }

    StreamPartitionAssignmentStrategyEnum partitionAssignmentStrategyEnum = DEFAULT_STRATEGY;
    if (partitionAssignmentStrategyName != null) {
      try {
        partitionAssignmentStrategyEnum =
            StreamPartitionAssignmentStrategyEnum.valueOf(partitionAssignmentStrategyName);
      } catch (Exception e) {
        LOGGER.error("Unable to create stream partition assignment strategy with name:{} for table:{}",
            partitionAssignmentStrategyName, tableConfig.getTableName());
      }
    }

    switch (partitionAssignmentStrategyEnum) {

      case UniformStreamPartitionAssignment:
        streamPartitionAssignmentStrategy = new UniformStreamPartitionAssignmentStrategy();
        break;
      case BalancedStreamPartitionAssignment:
        streamPartitionAssignmentStrategy = new BalancedStreamPartitionAssignmentStrategy();
        break;
      default:

    }
    return streamPartitionAssignmentStrategy;
  }
}
