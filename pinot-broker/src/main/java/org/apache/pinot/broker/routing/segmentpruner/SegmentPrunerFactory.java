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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.ColumnPartitionConfig;
import org.apache.pinot.common.config.RoutingConfig;
import org.apache.pinot.common.config.SegmentPartitionConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentPrunerFactory {
  private SegmentPrunerFactory() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPrunerFactory.class);

  public static final String LEGACY_PARTITION_AWARE_OFFLINE_ROUTING = "PartitionAwareOffline";
  public static final String LEGACY_PARTITION_AWARE_REALTIME_ROUTING = "PartitionAwareRealtime";

  public static List<SegmentPruner> getSegmentPruners(TableConfig tableConfig,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    RoutingConfig routingConfig = tableConfig.getRoutingConfig();
    if (routingConfig != null) {
      List<String> segmentPrunerTypes = routingConfig.getSegmentPrunerTypes();
      if (segmentPrunerTypes != null) {
        List<SegmentPruner> segmentPruners = new ArrayList<>(segmentPrunerTypes.size());
        for (String segmentPrunerType : segmentPrunerTypes) {
          if (RoutingConfig.PARTITION_SEGMENT_PRUNER_TYPE.equalsIgnoreCase(segmentPrunerType)) {
            PartitionSegmentPruner partitionSegmentPruner = getPartitionSegmentPruner(tableConfig, propertyStore);
            if (partitionSegmentPruner != null) {
              segmentPruners.add(partitionSegmentPruner);
            }
          }
        }
        return segmentPruners;
      } else {
        // Handle legacy configs for backward-compatibility
        TableType tableType = tableConfig.getTableType();
        String routingTableBuilderName = routingConfig.getRoutingTableBuilderName();
        if ((tableType == TableType.OFFLINE && LEGACY_PARTITION_AWARE_OFFLINE_ROUTING
            .equalsIgnoreCase(routingTableBuilderName)) || (tableType == TableType.REALTIME
            && LEGACY_PARTITION_AWARE_REALTIME_ROUTING.equalsIgnoreCase(routingTableBuilderName))) {
          PartitionSegmentPruner partitionSegmentPruner = getPartitionSegmentPruner(tableConfig, propertyStore);
          if (partitionSegmentPruner != null) {
            return Collections.singletonList(getPartitionSegmentPruner(tableConfig, propertyStore));
          }
        }
      }
    }
    return Collections.emptyList();
  }

  @Nullable
  private static PartitionSegmentPruner getPartitionSegmentPruner(TableConfig tableConfig,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    String tableNameWithType = tableConfig.getTableName();
    SegmentPartitionConfig segmentPartitionConfig = tableConfig.getIndexingConfig().getSegmentPartitionConfig();
    if (segmentPartitionConfig == null) {
      LOGGER.warn("Cannot enable partition pruning without segment partition config for table: {}", tableNameWithType);
      return null;
    }
    Map<String, ColumnPartitionConfig> columnPartitionMap = segmentPartitionConfig.getColumnPartitionMap();
    if (columnPartitionMap.size() != 1) {
      LOGGER.warn("Cannot enable partition pruning with other than exact one partition column for table: {}",
          tableNameWithType);
      return null;
    } else {
      String partitionColumn = columnPartitionMap.keySet().iterator().next();
      LOGGER.info("Using PartitionSegmentPruner on partition column: {} for table: {}", partitionColumn,
          tableNameWithType);
      return new PartitionSegmentPruner(tableNameWithType, partitionColumn, propertyStore);
    }
  }
}
