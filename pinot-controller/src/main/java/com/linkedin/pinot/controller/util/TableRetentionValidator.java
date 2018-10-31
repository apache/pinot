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
package com.linkedin.pinot.controller.util;

import com.linkedin.pinot.common.config.SegmentsValidationAndRetentionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>TableRetentionValidator</code> class validates the retention policy in table config, and the start/end
 * timestamp in segment metadata.
 * <p>Will validate the followings:
 * <ul>
 *   <li>
 *     Table Config
 *     <ul>
 *       <li>"segmentsConfig" is set.</li>
 *       <li>"segmentPushType" is set to APPEND or REFRESH.</li>
 *       <li>Retention setting is valid for APPEND push type.</li>
 *     </ul>
 *   </li>
 *   <li>
 *     Segment Metadata
 *     <ul>
 *       <li>For APPEND push type, offline segment start/end time and time unit is valid.</li>
 *     </ul>
 *   </li>
 * </ul>
 */
public class TableRetentionValidator {
  public static final long DEFAULT_DURATION_IN_DAYS_THRESHOLD = 365;

  private static final Logger LOGGER = LoggerFactory.getLogger(TableRetentionValidator.class);

  private final String _clusterName;
  private final ZKHelixAdmin _helixAdmin;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;

  private String _tableNamePattern = null;
  private long _durationInDaysThreshold = DEFAULT_DURATION_IN_DAYS_THRESHOLD;

  public TableRetentionValidator(@Nonnull String zkAddress, @Nonnull String clusterName) {
    _clusterName = clusterName;
    _helixAdmin = new ZKHelixAdmin(zkAddress);
    _propertyStore = new ZkHelixPropertyStore<>(zkAddress, new ZNRecordSerializer(),
        PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, clusterName));
  }

  public void overrideDefaultSettings(@Nullable String tableNamePattern, long durationInDaysThreshold) {
    _tableNamePattern = tableNamePattern;
    _durationInDaysThreshold = durationInDaysThreshold;
  }

  public void run()
      throws Exception {
    // Get all resources in cluster
    List<String> resourcesInCluster = _helixAdmin.getResourcesInCluster(_clusterName);

    for (String tableName : resourcesInCluster) {
      // Skip non-table resources
      if (!TableNameBuilder.isTableResource(tableName)) {
        continue;
      }

      // Skip tables that do not match the defined name pattern
      if (_tableNamePattern != null && !tableName.matches(_tableNamePattern)) {
        continue;
      }

      // Get the retention config
      SegmentsValidationAndRetentionConfig retentionConfig = getTableConfig(tableName).getValidationConfig();
      if (retentionConfig == null) {
        LOGGER.error("Table: {}, \"segmentsConfig\" field is missing in table config", tableName);
        continue;
      }
      String segmentPushType = retentionConfig.getSegmentPushType();
      if (segmentPushType == null) {
        LOGGER.error("Table: {}, null push type", tableName);
        continue;
      } else if (segmentPushType.equalsIgnoreCase("REFRESH")) {
        continue;
      } else if (!segmentPushType.equalsIgnoreCase("APPEND")) {
        LOGGER.error("Table: {}, invalid push type: {}", tableName, segmentPushType);
        continue;
      }

      // APPEND use case

      // Get time unit
      String timeUnitString = retentionConfig.getRetentionTimeUnit();
      TimeUnit timeUnit;
      try {
        timeUnit = TimeUnit.valueOf(timeUnitString.toUpperCase());
      } catch (Exception e) {
        LOGGER.error("Table: {}, invalid time unit: {}", tableName, timeUnitString);
        continue;
      }

      // Get time duration in days
      String timeValueString = retentionConfig.getRetentionTimeValue();
      long durationInDays;
      try {
        durationInDays = timeUnit.toDays(Long.valueOf(timeValueString));
      } catch (Exception e) {
        LOGGER.error("Table: {}, invalid time value: {}", tableName, timeValueString);
        continue;
      }
      if (durationInDays <= 0) {
        LOGGER.error("Table: {}, invalid retention duration in days: {}", tableName, durationInDays);
        continue;
      }
      if (durationInDays > _durationInDaysThreshold) {
        LOGGER.warn("Table: {}, retention duration in days is too large: {}", tableName, durationInDays);
      }

      // Skip segments metadata check for realtime tables
      if (tableName.endsWith("REALTIME")) {
        continue;
      }

      // Check segments metadata (only for offline tables)
      List<String> segmentNames = getSegmentNames(tableName);
      if (segmentNames == null || segmentNames.isEmpty()) {
        LOGGER.warn("Table: {}, no segment metadata in property store", tableName);
        continue;
      }

      List<String> errorMessages = new ArrayList<>();
      for (String segmentName : segmentNames) {
        OfflineSegmentZKMetadata offlineSegmentMetadata = getOfflineSegmentMetadata(tableName, segmentName);
        TimeUnit segmentTimeUnit = offlineSegmentMetadata.getTimeUnit();
        if (segmentTimeUnit == null) {
          errorMessages.add("Segment: " + segmentName + " has null time unit");
          continue;
        }
        long startTimeInMillis = segmentTimeUnit.toMillis(offlineSegmentMetadata.getStartTime());
        if (!TimeUtils.timeValueInValidRange(startTimeInMillis)) {
          errorMessages.add("Segment: " + segmentName + " has invalid start time in millis: " + startTimeInMillis);
        }
        long endTimeInMillis = segmentTimeUnit.toMillis(offlineSegmentMetadata.getEndTime());
        if (!TimeUtils.timeValueInValidRange(endTimeInMillis)) {
          errorMessages.add("Segment: " + segmentName + " has invalid end time in millis: " + endTimeInMillis);
        }
      }

      if (!errorMessages.isEmpty()) {
        LOGGER.error("Table: {}, invalid segments: {}", tableName, errorMessages);
      }
    }
  }

  private TableConfig getTableConfig(String tableName)
      throws Exception {
    return TableConfig.fromZnRecord(
        _propertyStore.get(ZKMetadataProvider.constructPropertyStorePathForResourceConfig(tableName), null, 0));
  }

  private List<String> getSegmentNames(String tableName) {
    return _propertyStore.getChildNames(ZKMetadataProvider.constructPropertyStorePathForResource(tableName), 0);
  }

  private OfflineSegmentZKMetadata getOfflineSegmentMetadata(String tableName, String segmentName) {
    return new OfflineSegmentZKMetadata(
        _propertyStore.get(ZKMetadataProvider.constructPropertyStorePathForSegment(tableName, segmentName), null, 0));
  }
}
