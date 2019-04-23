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
package org.apache.pinot.broker.routing;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ThreadSafe
public class HelixExternalViewBasedTimeBoundaryService implements TimeBoundaryService {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixExternalViewBasedTimeBoundaryService.class);

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final Map<String, TimeBoundaryInfo> _timeBoundaryInfoMap = new ConcurrentHashMap<>();

  public HelixExternalViewBasedTimeBoundaryService(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _propertyStore = propertyStore;
  }

  public void updateTimeBoundaryService(ExternalView externalView) {
    String tableNameWithType = externalView.getResourceName();

    // Skip real-time table, only use offline table to update the time boundary
    if (TableNameBuilder.getTableTypeFromTableName(tableNameWithType) == TableType.REALTIME) {
      return;
    }

    Set<String> offlineSegmentsServing = externalView.getPartitionSet();
    if (offlineSegmentsServing.isEmpty()) {
      LOGGER.warn("Skipping updating time boundary for table: '{}' with no offline segment", tableNameWithType);
      return;
    }

    // TODO: when we start using dateTime, pick the time column from the retention config, and use the DateTimeFieldSpec
    //       from the schema to determine the time unit
    // TODO: support SDF
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
    assert tableConfig != null;
    SegmentsValidationAndRetentionConfig retentionConfig = tableConfig.getValidationConfig();
    String timeColumn = retentionConfig.getTimeColumnName();
    TimeUnit tableTimeUnit = retentionConfig.getTimeType();
    if (timeColumn == null || tableTimeUnit == null) {
      LOGGER.error("Skipping updating time boundary for table: '{}' because time column/unit is not set",
          tableNameWithType);
      return;
    }

    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, tableNameWithType);
    assert schema != null;
    if (!timeColumn.equals(schema.getTimeColumnName())) {
      LOGGER.error("Time column does not match in table config: '{}' and schema: '{}'", timeColumn,
          schema.getTimeColumnName());
    }
    if (tableTimeUnit != schema.getOutgoingTimeUnit()) {
      LOGGER.error("Time unit does not match in table config: '{}' and schema: '{}'", tableTimeUnit,
          schema.getOutgoingTimeUnit());
    }

    // Bulk reading all segment ZK metadata is more efficient than reading one at a time
    List<OfflineSegmentZKMetadata> segmentZKMetadataList =
        ZKMetadataProvider.getOfflineSegmentZKMetadataListForTable(_propertyStore, tableNameWithType);

    long maxTimeValue = -1L;
    for (OfflineSegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
      String segmentName = segmentZKMetadata.getSegmentName();

      // Only consider segments in the external view
      if (!offlineSegmentsServing.contains(segmentName)) {
        LOGGER.warn("Skipping processing segment: '{}' for table: '{}' because it does not exist in the external view",
            segmentName, tableNameWithType);
        continue;
      }

      // Check if time unit in segment ZK metadata matches table time unit
      // NOTE: for now, time unit in segment ZK metadata should always match table time unit, but in the future we might
      //       want to always use MILLISECONDS as the time unit in segment ZK metadata
      TimeUnit segmentTimeUnit = segmentZKMetadata.getTimeUnit();
      if (segmentTimeUnit != tableTimeUnit) {
        LOGGER.warn("Time unit for table: '{}', segment: '{}' ZK metadata: {} does not match the table time unit: {}",
            tableNameWithType, segmentName, segmentTimeUnit, tableTimeUnit);
      }

      long segmentEndTime = segmentZKMetadata.getEndTime();
      if (segmentEndTime <= 0) {
        LOGGER
            .error("Skipping processing segment: '{}' for table: '{}' because the end time: {} is illegal", segmentName,
                tableNameWithType, segmentEndTime);
        continue;
      }

      // Convert segment end time into table time unit
      maxTimeValue = Math.max(maxTimeValue, tableTimeUnit.convert(segmentEndTime, segmentTimeUnit));
    }

    if (maxTimeValue == -1L) {
      LOGGER.error("Skipping updating time boundary for table: '{}' because no segment contains valid end time",
          tableNameWithType);
      return;
    }

    // For HOURLY push table with time unit other than DAYS, use (maxTimeValue - 1 HOUR) as the time boundary
    // Otherwise, use (maxTimeValue - 1 DAY)
    long timeBoundary;
    if ("HOURLY".equalsIgnoreCase(retentionConfig.getSegmentPushFrequency()) && tableTimeUnit != TimeUnit.DAYS) {
      timeBoundary = maxTimeValue - tableTimeUnit.convert(1L, TimeUnit.HOURS);
    } else {
      timeBoundary = maxTimeValue - tableTimeUnit.convert(1L, TimeUnit.DAYS);
    }

    LOGGER.info("Updated time boundary for table: '{}' to: {} {}", tableNameWithType, timeBoundary, tableTimeUnit);
    _timeBoundaryInfoMap.put(tableNameWithType, new TimeBoundaryInfo(timeColumn, Long.toString(timeBoundary)));
  }

  @Override
  public TimeBoundaryInfo getTimeBoundaryInfoFor(String table) {
    return _timeBoundaryInfoMap.get(table);
  }

  @Override
  public void remove(String tableName) {
    _timeBoundaryInfoMap.remove(tableName);
  }
}
