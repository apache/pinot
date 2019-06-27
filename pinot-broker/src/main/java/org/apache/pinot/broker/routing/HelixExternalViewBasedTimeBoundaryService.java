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
    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, tableNameWithType);
    assert schema != null;
    String timeColumn = schema.getTimeColumnName();
    TimeUnit tableTimeUnit = schema.getOutgoingTimeUnit();
    if (timeColumn == null || tableTimeUnit == null) {
      LOGGER.error("Skipping updating time boundary for table: '{}' because time column/unit is not set",
          tableNameWithType);
      return;
    }

    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
    assert tableConfig != null;
    SegmentsValidationAndRetentionConfig retentionConfig = tableConfig.getValidationConfig();
    if (!timeColumn.equals(retentionConfig.getTimeColumnName())) {
      LOGGER.error("Time column does not match in schema: '{}' and table config: '{}'", timeColumn,
          retentionConfig.getTimeColumnName());
    }
    if (tableTimeUnit != retentionConfig.getTimeType()) {
      LOGGER.error("Time unit does not match in schema: '{}' and table config: '{}'", tableTimeUnit,
          retentionConfig.getTimeType());
    }

    // Bulk reading all segment ZK metadata is more efficient than reading one at a time
    List<OfflineSegmentZKMetadata> segmentZKMetadataList =
        ZKMetadataProvider.getOfflineSegmentZKMetadataListForTable(_propertyStore, tableNameWithType);

    OfflineSegmentZKMetadata latestSegmentZKMetadata = null;
    long latestSegmentEndTimeMs = 0L;
    for (OfflineSegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
      String segmentName = segmentZKMetadata.getSegmentName();

      // Only consider segments in the external view
      if (!offlineSegmentsServing.contains(segmentName)) {
        LOGGER.warn("Skipping processing segment: '{}' for table: '{}' because it does not exist in the external view",
            segmentName, tableNameWithType);
        continue;
      }

      long segmentEndTime = segmentZKMetadata.getEndTime();
      if (segmentEndTime <= 0) {
        LOGGER
            .error("Skipping processing segment: '{}' for table: '{}' because the end time: {} is illegal", segmentName,
                tableNameWithType, segmentEndTime);
        continue;
      }

      long segmentEndTimeMs = segmentZKMetadata.getTimeUnit().toMillis(segmentEndTime);
      if (segmentEndTimeMs > latestSegmentEndTimeMs) {
        latestSegmentZKMetadata = segmentZKMetadata;
        latestSegmentEndTimeMs = segmentEndTimeMs;
      }
    }

    if (latestSegmentZKMetadata == null) {
      LOGGER.error("Skipping updating time boundary for table: '{}' because no segment contains valid end time",
          tableNameWithType);
      return;
    }

    // Convert segment end time into table time unit
    // NOTE: for now, time unit in segment ZK metadata should always match table time unit, but in the future we might
    //       want to always use MILLISECONDS as the time unit in segment ZK metadata
    long latestSegmentEndTime = latestSegmentZKMetadata.getEndTime();
    long timeBoundary = tableTimeUnit.convert(latestSegmentEndTime, latestSegmentZKMetadata.getTimeUnit());

    // When pushing multiple offline segments, the first pushed offline segment will push forward the time boundary
    // before the other offline segments arrived. If we directly push the time boundary to the segment end time, we
    // might get inconsistent result before all the segments arrived. In order to address this issue:
    // - Case 1: segment has the same start and end time, directly use the end time as the time boundary. It is OK to
    //   directly use the end time as the time boundary because all the records in the new segments have the same time
    //   value and the time filter will filter all of them out, so the result is consistent. The reason why we handle
    //   this case separately is because there are use cases with time unit other than DAYS, but have time value rounded
    //   to the start of the day for offline table (offline segments have the same start and end time), and un-rounded
    //   time value for real-time table (real-time segments have different start and end time). In order to get the
    //   correct result, must attach filter 'time < endTime' to the offline side and filter 'time >= endTime' to the
    //   real-time side.
    // - Case 2. segment has different start and end time, rewind the end time with the push interval and use it as the
    //   time boundary. The assumption we made here is that the push will finish in the push interval, and next push
    //   will push the time boundary forward again and make the records within the last push interval queryable.
    if (latestSegmentEndTime != latestSegmentZKMetadata.getStartTime()) {

      // For HOURLY push table with time unit other than DAYS, use {latestSegmentEndTime - (1 HOUR) + 1} as the time
      // boundary; otherwise, use {latestSegmentEndTime - (1 DAY) + 1} as the time boundary
      if ("HOURLY".equalsIgnoreCase(retentionConfig.getSegmentPushFrequency()) && tableTimeUnit != TimeUnit.DAYS) {
        timeBoundary = timeBoundary - tableTimeUnit.convert(1L, TimeUnit.HOURS) + 1;
      } else {
        timeBoundary = timeBoundary - tableTimeUnit.convert(1L, TimeUnit.DAYS) + 1;
      }
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
