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
package org.apache.pinot.controller.api.upload;

import java.util.Map;
import java.util.Set;
import javax.ws.rs.core.Response;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.validation.StorageQuotaChecker;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.utils.TimeUtils;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * SegmentValidationUtils provides utility methods to validate the segment during segment upload.
 */
public class SegmentValidationUtils {
  private SegmentValidationUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentValidationUtils.class);

  public static void validateTimeInterval(SegmentMetadata segmentMetadata, TableConfig tableConfig) {
    Interval timeInterval = segmentMetadata.getTimeInterval();
    if (timeInterval != null) {
      if (!TimeUtils.isValidTimeInterval(timeInterval)) {
        throw new ControllerApplicationException(LOGGER, String.format(
            "Invalid segment start/end time: %s (in millis: %d/%d) for segment: %s of table: %s, must be between: %s",
            timeInterval, timeInterval.getStartMillis(), timeInterval.getEndMillis(), segmentMetadata.getName(),
            tableConfig.getTableName(), TimeUtils.VALID_TIME_INTERVAL), Response.Status.FORBIDDEN);
      }
    } else {
      String timeColumn = tableConfig.getValidationConfig().getTimeColumnName();
      if (timeColumn != null) {
        throw new ControllerApplicationException(LOGGER,
            String.format("Failed to find time interval in segment: %s for table: %s with time column: %s",
                segmentMetadata.getName(), tableConfig.getTableName(), timeColumn), Response.Status.FORBIDDEN);
      }
    }
  }

  public static void validateUpsertSegmentPartitionMetadata(SegmentMetadata segmentMetadata, TableConfig tableConfig) {
    // Scope this upload guard to offline upsert. Realtime upsert upload flows need CONSUMING/COMPLETED
    // partition-column resolution, so avoid applying the OFFLINE lookup below to realtime tables.
    if (tableConfig.getTableType() != TableType.OFFLINE || !tableConfig.isUpsertEnabled()) {
      return;
    }

    String partitionColumn = getPartitionColumn(tableConfig);
    if (StringUtils.isEmpty(partitionColumn)) {
      return;
    }

    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(partitionColumn);
    Set<Integer> partitions = columnMetadata != null ? columnMetadata.getPartitions() : null;
    if (partitions == null || partitions.size() != 1) {
      throw new ControllerApplicationException(LOGGER,
          "Uploaded segment: " + segmentMetadata.getName() + " for offline upsert table: "
              + tableConfig.getTableName() + " must contain exactly one partition id for column: " + partitionColumn
              + ", got: " + partitions, Response.Status.BAD_REQUEST);
    }
  }

  private static String getPartitionColumn(TableConfig tableConfig) {
    if (!MapUtils.isEmpty(tableConfig.getInstanceAssignmentConfigMap())) {
      InstanceAssignmentConfig instanceAssignmentConfig =
          tableConfig.getInstanceAssignmentConfigMap().get(InstancePartitionsType.OFFLINE.name());
      String partitionColumn = getPartitionColumn(instanceAssignmentConfig);
      if (StringUtils.isNotEmpty(partitionColumn)) {
        return partitionColumn;
      }
    }

    //noinspection deprecation
    SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
        validationConfig != null ? validationConfig.getReplicaGroupStrategyConfig() : null;
    String partitionColumn =
        replicaGroupStrategyConfig != null ? replicaGroupStrategyConfig.getPartitionColumn() : null;
    if (StringUtils.isNotEmpty(partitionColumn)) {
      return partitionColumn;
    }

    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    SegmentPartitionConfig segmentPartitionConfig =
        indexingConfig != null ? indexingConfig.getSegmentPartitionConfig() : null;
    Map<String, ?> columnPartitionMap =
        segmentPartitionConfig != null ? segmentPartitionConfig.getColumnPartitionMap() : null;
    if (MapUtils.isNotEmpty(columnPartitionMap) && columnPartitionMap.size() == 1) {
      return columnPartitionMap.keySet().iterator().next();
    }
    return null;
  }

  private static String getPartitionColumn(InstanceAssignmentConfig instanceAssignmentConfig) {
    if (instanceAssignmentConfig == null) {
      return null;
    }
    InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig =
        instanceAssignmentConfig.getReplicaGroupPartitionConfig();
    return replicaGroupPartitionConfig != null ? replicaGroupPartitionConfig.getPartitionColumn() : null;
  }

  public static void checkStorageQuota(String segmentName, long tarSegmentSizeInBytes, long untarredSegmentSizeInBytes,
      TableConfig tableConfig,
      StorageQuotaChecker quotaChecker) {
    StorageQuotaChecker.QuotaCheckerResponse response;
    try {
      response = quotaChecker
          .isSegmentStorageWithinQuota(tableConfig, segmentName, tarSegmentSizeInBytes, untarredSegmentSizeInBytes);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Caught exception while checking the storage quota for segment: %s of table: %s", segmentName,
              tableConfig.getTableName()), Response.Status.INTERNAL_SERVER_ERROR);
    }
    if (!response._isSegmentWithinQuota) {
      throw new ControllerApplicationException(LOGGER,
          String.format("Storage quota check failed for segment: %s of table: %s, reason: %s", segmentName,
              tableConfig.getTableName(), response._reason), Response.Status.FORBIDDEN);
    }
  }
}
