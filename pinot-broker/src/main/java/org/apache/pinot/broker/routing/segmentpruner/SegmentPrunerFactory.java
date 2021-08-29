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
package org.apache.pinot.broker.routing.segmentpruner;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.broker.routing.segmentmetadata.PartitionInfo.getPartitionColumnFromConfig;


public class SegmentPrunerFactory {
  private SegmentPrunerFactory() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPrunerFactory.class);

  public static final String LEGACY_PARTITION_AWARE_OFFLINE_ROUTING = "PartitionAwareOffline";
  public static final String LEGACY_PARTITION_AWARE_REALTIME_ROUTING = "PartitionAwareRealtime";

  public static List<SegmentPruner> getSegmentPruners(TableConfig tableConfig,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    RoutingConfig routingConfig = tableConfig.getRoutingConfig();
    List<SegmentPruner> segmentPruners = new ArrayList<>();

    if (routingConfig != null) {
      List<String> segmentPrunerTypes = routingConfig.getSegmentPrunerTypes();
      if (segmentPrunerTypes != null) {
        List<SegmentPruner> configuredSegmentPruners = new ArrayList<>(segmentPrunerTypes.size());
        for (String segmentPrunerType : segmentPrunerTypes) {
          if (RoutingConfig.PARTITION_SEGMENT_PRUNER_TYPE.equalsIgnoreCase(segmentPrunerType)) {
            PartitionSegmentPruner partitionSegmentPruner = getPartitionSegmentPruner(tableConfig, propertyStore);
            if (partitionSegmentPruner != null) {
              configuredSegmentPruners.add(partitionSegmentPruner);
            }
          }

          if (RoutingConfig.TIME_SEGMENT_PRUNER_TYPE.equalsIgnoreCase(segmentPrunerType)) {
            TimeSegmentPruner timeSegmentPruner = getTimeSegmentPruner(tableConfig, propertyStore);
            if (timeSegmentPruner != null) {
              configuredSegmentPruners.add(timeSegmentPruner);
            }
          }
        }
        segmentPruners.addAll(sortSegmentPruners(configuredSegmentPruners));
      } else {
        // Handle legacy configs for backward-compatibility
        TableType tableType = tableConfig.getTableType();
        String routingTableBuilderName = routingConfig.getRoutingTableBuilderName();
        if ((tableType == TableType.OFFLINE && LEGACY_PARTITION_AWARE_OFFLINE_ROUTING
            .equalsIgnoreCase(routingTableBuilderName)) || (tableType == TableType.REALTIME
            && LEGACY_PARTITION_AWARE_REALTIME_ROUTING.equalsIgnoreCase(routingTableBuilderName))) {
          PartitionSegmentPruner partitionSegmentPruner = getPartitionSegmentPruner(tableConfig, propertyStore);
          if (partitionSegmentPruner != null) {
            segmentPruners.add(getPartitionSegmentPruner(tableConfig, propertyStore));
          }
        }
      }
    }
    // EmptySegmentPruner tries to create a copy of all the lists, in some cases if the
    // segments has 10k+ items in it, it may waste a lot of CPU cycle for several empty segment.
    // Moving it to the end may help some scenario in performance.
    segmentPruners.add(new EmptySegmentPruner(tableConfig, propertyStore));
    return segmentPruners;
  }

  @Nullable
  private static PartitionSegmentPruner getPartitionSegmentPruner(TableConfig tableConfig,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    String tableNameWithType = tableConfig.getTableName();
    Set<String> partitionColumns = getPartitionColumnFromConfig(tableConfig);
    if (partitionColumns == null) {
      LOGGER.warn("Cannot enable partition pruning without segment partition config for table: {}", tableNameWithType);
      return null;
    }
    if (partitionColumns.size() != 1) {
      LOGGER.warn("Cannot enable partition pruning with other than exact one partition column for table: {}",
          tableNameWithType);
      return null;
    }
    String partitionColumn = partitionColumns.iterator().next();
    LOGGER
        .info("Using PartitionSegmentPruner on partition column: {} for table: {}", partitionColumn, tableNameWithType);
    return new PartitionSegmentPruner(tableNameWithType, partitionColumn, propertyStore);
  }

  @Nullable
  private static TimeSegmentPruner getTimeSegmentPruner(TableConfig tableConfig,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    String tableNameWithType = tableConfig.getTableName();
    SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
    if (validationConfig == null) {
      LOGGER.warn("Cannot enable time range pruning without validation config for table: {}", tableNameWithType);
      return null;
    }
    String timeColumn = validationConfig.getTimeColumnName();
    if (timeColumn == null) {
      LOGGER.warn("Cannot enable time range pruning without time column for table: {}", tableNameWithType);
      return null;
    }

    LOGGER.info("Using TimeRangePruner on time column: {} for table: {}", timeColumn, tableNameWithType);
    return new TimeSegmentPruner(tableConfig, propertyStore);
  }

  private static List<SegmentPruner> sortSegmentPruners(List<SegmentPruner> pruners) {
    // If there's multiple pruners, move time range pruners to the front。
    // Partition pruner run time is proportional to input # of segments while time range pruner is not,
    // Prune based on time range first will have a smaller input size for partition pruners, so have better performance.
    List<SegmentPruner> sortedPruners = new ArrayList<>();
    for (SegmentPruner pruner : pruners) {
      if (pruner instanceof TimeSegmentPruner) {
        sortedPruners.add(pruner);
      }
    }
    for (SegmentPruner pruner : pruners) {
      if (!(pruner instanceof TimeSegmentPruner)) {
        sortedPruners.add(pruner);
      }
    }
    return sortedPruners;
  }
}
