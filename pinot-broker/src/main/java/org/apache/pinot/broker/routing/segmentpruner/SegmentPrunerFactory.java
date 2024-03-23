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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections4.MapUtils;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.Schema;
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
    List<SegmentPruner> segmentPruners = new ArrayList<>();
    boolean needsEmptySegment = TableConfigUtils.needsEmptySegmentPruner(tableConfig);
    if (needsEmptySegment) {
      // Add EmptySegmentPruner if needed
      segmentPruners.add(new EmptySegmentPruner(tableConfig));
    }

    RoutingConfig routingConfig = tableConfig.getRoutingConfig();
    if (routingConfig != null) {
      List<String> segmentPrunerTypes = routingConfig.getSegmentPrunerTypes();
      if (segmentPrunerTypes != null) {
        List<SegmentPruner> configuredSegmentPruners = new ArrayList<>(segmentPrunerTypes.size());
        for (String segmentPrunerType : segmentPrunerTypes) {
          if (RoutingConfig.PARTITION_SEGMENT_PRUNER_TYPE.equalsIgnoreCase(segmentPrunerType)) {
            SegmentPruner partitionSegmentPruner = getPartitionSegmentPruner(tableConfig, propertyStore);
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
        // Sort all segment pruners in order of: empty -> time -> partition. We are trying to sort them in a this order
        // for improving the performance, this order may not be the optimal case -- we need move the pruner that will
        // potentially prune the most segments to front)
        segmentPruners.addAll(sortSegmentPruners(configuredSegmentPruners));
      } else {
        // Handle legacy configs for backward-compatibility
        TableType tableType = tableConfig.getTableType();
        String routingTableBuilderName = routingConfig.getRoutingTableBuilderName();
        if ((tableType == TableType.OFFLINE && LEGACY_PARTITION_AWARE_OFFLINE_ROUTING.equalsIgnoreCase(
            routingTableBuilderName)) || (tableType == TableType.REALTIME
            && LEGACY_PARTITION_AWARE_REALTIME_ROUTING.equalsIgnoreCase(routingTableBuilderName))) {
          SegmentPruner partitionSegmentPruner = getPartitionSegmentPruner(tableConfig, propertyStore);
          if (partitionSegmentPruner != null) {
            segmentPruners.add(partitionSegmentPruner);
          }
        }
      }
    }
    return segmentPruners;
  }

  @Nullable
  private static SegmentPruner getPartitionSegmentPruner(TableConfig tableConfig,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    String tableNameWithType = tableConfig.getTableName();
    SegmentPartitionConfig segmentPartitionConfig = tableConfig.getIndexingConfig().getSegmentPartitionConfig();
    if (segmentPartitionConfig == null) {
      LOGGER.warn("Cannot enable partition pruning without segment partition config for table: {}", tableNameWithType);
      return null;
    }
    if (MapUtils.isEmpty(segmentPartitionConfig.getColumnPartitionMap())) {
      LOGGER.warn("Cannot enable partition pruning without column partition config for table: {}", tableNameWithType);
      return null;
    }
    Map<String, ColumnPartitionConfig> columnPartitionMap = segmentPartitionConfig.getColumnPartitionMap();
    Set<String> partitionColumns = columnPartitionMap.keySet();
    LOGGER.info("Using PartitionSegmentPruner on partition columns: {} for table: {}", partitionColumns,
        tableNameWithType);
    return partitionColumns.size() == 1 ? new SinglePartitionColumnSegmentPruner(tableNameWithType,
        partitionColumns.iterator().next())
        : new MultiPartitionColumnsSegmentPruner(tableNameWithType, partitionColumns);
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
    return createTimeSegmentPruner(tableConfig, propertyStore);
  }

  @VisibleForTesting
  static TimeSegmentPruner createTimeSegmentPruner(TableConfig tableConfig,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    String tableNameWithType = tableConfig.getTableName();
    String timeColumn = tableConfig.getValidationConfig().getTimeColumnName();
    Preconditions.checkNotNull(timeColumn, "Time column must be configured in table config for table: %s",
        tableNameWithType);
    Schema schema = ZKMetadataProvider.getTableSchema(propertyStore, tableNameWithType);
    Preconditions.checkNotNull(schema, "Failed to find schema for table: %s", tableNameWithType);
    DateTimeFieldSpec dateTimeSpec = schema.getSpecForTimeColumn(timeColumn);
    Preconditions.checkNotNull(dateTimeSpec, "Field spec must be specified in schema for time column: %s of table: %s",
        timeColumn, tableNameWithType);
    DateTimeFormatSpec timeFormatSpec = dateTimeSpec.getFormatSpec();

    LOGGER.info("Using TimeRangePruner on time column: {} for table: {} with DateTimeFormatSpec: {}",
        timeColumn, tableNameWithType, timeFormatSpec);
    return new TimeSegmentPruner(tableConfig, timeColumn, timeFormatSpec);
  }

  private static List<SegmentPruner> sortSegmentPruners(List<SegmentPruner> pruners) {
    // If there's multiple pruners, always prune empty segments first. After that, pruned based on time range, and
    // followed by partition pruners.
    // Partition pruner run time is proportional to input # of segments while time range pruner is not,
    // Prune based on time range first will have a smaller input size for partition pruners, so have better performance.
    List<SegmentPruner> sortedPruners = new ArrayList<>();
    for (SegmentPruner pruner : pruners) {
      if (pruner instanceof EmptySegmentPruner) {
        sortedPruners.add(pruner);
      }
    }
    for (SegmentPruner pruner : pruners) {
      if (pruner instanceof TimeSegmentPruner) {
        sortedPruners.add(pruner);
      }
    }
    for (SegmentPruner pruner : pruners) {
      if (pruner instanceof SinglePartitionColumnSegmentPruner
          || pruner instanceof MultiPartitionColumnsSegmentPruner) {
        sortedPruners.add(pruner);
      }
    }
    return sortedPruners;
  }
}
