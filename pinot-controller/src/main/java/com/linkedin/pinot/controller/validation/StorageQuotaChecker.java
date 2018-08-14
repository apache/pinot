/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.validation;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.linkedin.pinot.common.config.QuotaConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.exception.InvalidConfigException;
import com.linkedin.pinot.common.metrics.ControllerGauge;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.utils.DataSize;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.util.TableSizeReader;
import java.io.File;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to check if a new segment is within the configured storage quota for the table
 *
 */
public class StorageQuotaChecker {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageQuotaChecker.class);

  private final TableSizeReader _tableSizeReader;
  private final TableConfig _tableConfig;
  private final ControllerMetrics _controllerMetrics;
  private final PinotHelixResourceManager _pinotHelixResourceManager;

  public StorageQuotaChecker(TableConfig tableConfig, TableSizeReader tableSizeReader,
      ControllerMetrics controllerMetrics, PinotHelixResourceManager pinotHelixResourceManager) {
    _tableConfig = tableConfig;
    _tableSizeReader = tableSizeReader;
    _controllerMetrics = controllerMetrics;
    _pinotHelixResourceManager = pinotHelixResourceManager;
  }

  public static class QuotaCheckerResponse {
    public boolean isSegmentWithinQuota;
    public String reason;

    QuotaCheckerResponse(boolean isSegmentWithinQuota, String reason) {
      this.isSegmentWithinQuota = isSegmentWithinQuota;
      this.reason = reason;
    }
  }

  public static QuotaCheckerResponse success(String msg) {
    return new QuotaCheckerResponse(true, msg);
  }

  public static QuotaCheckerResponse failure(String msg) {
    return new QuotaCheckerResponse(false, msg);
  }

  /**
   * check if the segment represented by segmentFile is within the storage quota
   * @param segmentFile untarred segment. This should not be null.
   *                    segmentFile must exist on disk and must be a directory
   * @param tableNameWithType table name with type (OFFLINE/REALTIME) information
   * @param segmentName name of the segment being added
   * @param timeoutMsec timeout in milliseconds for reading table sizes from server
   *
   */
  public QuotaCheckerResponse isSegmentStorageWithinQuota(@Nonnull File segmentFile, @Nonnull String tableNameWithType,
      @Nonnull String segmentName, @Nonnegative int timeoutMsec) throws InvalidConfigException {
    Preconditions.checkNotNull(segmentFile);
    Preconditions.checkNotNull(tableNameWithType);
    Preconditions.checkNotNull(segmentName);
    Preconditions.checkArgument(timeoutMsec > 0, "Timeout value must be > 0, input: %s", timeoutMsec);
    Preconditions.checkArgument(segmentFile.exists(), "Segment file: %s does not exist", segmentFile);
    Preconditions.checkArgument(segmentFile.isDirectory(), "Segment file: %s is not a directory", segmentFile);

    // 1. Read table config
    // 2. read table size from all the servers
    // 3. update predicted segment sizes
    // 4. is the updated size within quota
    QuotaConfig quotaConfig = _tableConfig.getQuotaConfig();
    int numReplicas = _tableConfig.getValidationConfig().getReplicationNumber();
    final String tableName = _tableConfig.getTableName();

    if (quotaConfig == null || Strings.isNullOrEmpty(quotaConfig.getStorage())) {
      // no quota configuration...so ignore for backwards compatibility
      LOGGER.warn("Quota configuration not set for table: {}", tableNameWithType);
      return success("Quota configuration not set for table: " + tableNameWithType);
    }

    long allowedStorageBytes = numReplicas * quotaConfig.storageSizeBytes();
    if (allowedStorageBytes < 0) {
      LOGGER.warn("Storage quota is not configured for table: {}", tableNameWithType);
      return success("Storage quota is not configured for table: " + tableNameWithType);
    }
    _controllerMetrics.setValueOfTableGauge(tableName, ControllerGauge.TABLE_QUOTA, allowedStorageBytes);

    long incomingSegmentSizeBytes = FileUtils.sizeOfDirectory(segmentFile);

    // read table size
    TableSizeReader.TableSubTypeSizeDetails tableSubtypeSize = null;
    try {
      tableSubtypeSize = _tableSizeReader.getTableSubtypeSize(tableNameWithType, timeoutMsec);
    } catch (InvalidConfigException e) {
      LOGGER.error("Failed to get table size for table {}", tableNameWithType, e);
      throw e;
    }

    if (tableSubtypeSize.estimatedSizeInBytes == -1) {
      // don't fail the quota check in this case
      return success("Missing size reports from all servers. Bypassing storage quota check for " + tableNameWithType);
    }

    if (tableSubtypeSize.missingSegments > 0) {
      if (tableSubtypeSize.estimatedSizeInBytes > allowedStorageBytes) {
        return failure("Table " + tableNameWithType + " already over quota. Estimated size for all replicas is "
            + DataSize.fromBytes(tableSubtypeSize.estimatedSizeInBytes) + ". Configured size for " + numReplicas + " is "
            + DataSize.fromBytes(allowedStorageBytes));
      } else {
        return success( "Missing size report for " + tableSubtypeSize.missingSegments
            + " segments. Bypassing storage quota check for " + tableNameWithType);
      }
    }

    // If the segment exists(refresh), get the existing size
    TableSizeReader.SegmentSizeDetails sizeDetails = tableSubtypeSize.segments.get(segmentName);
    long existingSegmentSizeBytes = sizeDetails != null ? sizeDetails.estimatedSizeInBytes : 0;

    // Since tableNameWithType comes with the table type(OFFLINE), thus we guarantee that
    // tableSubtypeSize.estimatedSizeInBytes is the offline table size.
    _controllerMetrics.setValueOfTableGauge(tableName, ControllerGauge.OFFLINE_TABLE_ESTIMATED_SIZE,
        tableSubtypeSize.estimatedSizeInBytes);

    LOGGER.info("Table {}'s estimatedSizeInBytes is {}. ReportedSizeInBytes (actual reports from servers) is {}",
        tableName, tableSubtypeSize.estimatedSizeInBytes, tableSubtypeSize.reportedSizeInBytes);

    // Only emit the real percentage of storage quota usage by lead controller, otherwise emit 0L.
    if (_pinotHelixResourceManager.isLeader() && allowedStorageBytes != 0L) {
      long existingStorageQuotaUtilization = tableSubtypeSize.estimatedSizeInBytes  * 100 / allowedStorageBytes;
      _controllerMetrics.setValueOfTableGauge(tableName, ControllerGauge.TABLE_STORAGE_QUOTA_UTILIZATION,
          existingStorageQuotaUtilization);
    } else {
      _controllerMetrics.setValueOfTableGauge(tableName, ControllerGauge.TABLE_STORAGE_QUOTA_UTILIZATION, 0L);
    }

    // Note: incomingSegmentSizeBytes is uncompressed data size for just 1 replica,
    // while estimatedFinalSizeBytes is for all replicas of all segments put together.
    long totalIncomingSegmentSizeBytes = incomingSegmentSizeBytes * numReplicas;
    long estimatedFinalSizeBytes =
        tableSubtypeSize.estimatedSizeInBytes - existingSegmentSizeBytes + totalIncomingSegmentSizeBytes;
    if (estimatedFinalSizeBytes <= allowedStorageBytes) {
      String message;
      if (sizeDetails == null) {
        // append use case
        message = String.format("Appending Segment %s of Table %s is within quota. Total allowed storage size: %s ( = configured quota: %s * number replicas: %d). New estimated table size of all replicas: %s. Current table size of all replicas: %s. Incoming uncompressed segment size of all replicas: %s ( = single incoming uncompressed segment size: %s * number replicas: %d). Formula: New estimated size = current table size + incoming segment size",
            segmentName, tableName, DataSize.fromBytes(allowedStorageBytes), DataSize.fromBytes(quotaConfig.storageSizeBytes()), numReplicas, DataSize.fromBytes(estimatedFinalSizeBytes), DataSize.fromBytes(tableSubtypeSize.estimatedSizeInBytes), DataSize.fromBytes(totalIncomingSegmentSizeBytes), DataSize.fromBytes(incomingSegmentSizeBytes), numReplicas);
      } else  {
        // refresh use case
        message = String.format("Refreshing Segment %s of Table %s is within quota. Total allowed storage size: %s ( = configured quota: %s * number replicas: %d). New estimated table size of all replicas: %s. Current table size of all replicas: %s. Incoming uncompressed segment size of all replicas: %s ( = single incoming uncompressed segment size: %s * number replicas: %d). Existing same segment size of all replicas: %s. Formula: New estimated size = current table size - existing same segment size + incoming segment size",
            segmentName, tableName, DataSize.fromBytes(allowedStorageBytes), DataSize.fromBytes(quotaConfig.storageSizeBytes()), numReplicas, DataSize.fromBytes(estimatedFinalSizeBytes), DataSize.fromBytes(tableSubtypeSize.estimatedSizeInBytes), DataSize.fromBytes(totalIncomingSegmentSizeBytes), DataSize.fromBytes(incomingSegmentSizeBytes), numReplicas, DataSize.fromBytes(existingSegmentSizeBytes));
      }
      LOGGER.info(message);
      return success(message);
    } else {
      String message;
      if (tableSubtypeSize.estimatedSizeInBytes > allowedStorageBytes) {
        message = String.format(
            "Table %s already over quota. Existing estimated uncompressed table size of all replicas: %s > total allowed storage size: %s ( = configured quota: %s * num replicas: %d). Check if indexes were enabled recently and adjust table quota accordingly.",
            tableName, DataSize.fromBytes(tableSubtypeSize.estimatedSizeInBytes), DataSize.fromBytes(allowedStorageBytes), DataSize.fromBytes(quotaConfig.storageSizeBytes()),
            numReplicas);
      } else {
        message = String.format(
            "Storage quota exceeded for Table %s. New estimated size: %s > total allowed storage size: %s, where new estimated size = existing estimated uncompressed size of all replicas: %s - existing segment sizes of all replicas: %s + (incoming uncompressed segment size: %s * number replicas: %d), total allowed storage size = configured quota: %s * number replicas: %d",
            tableName, DataSize.fromBytes(estimatedFinalSizeBytes), DataSize.fromBytes(allowedStorageBytes), DataSize.fromBytes(tableSubtypeSize.estimatedSizeInBytes), DataSize.fromBytes(existingSegmentSizeBytes),
            DataSize.fromBytes(incomingSegmentSizeBytes), numReplicas, DataSize.fromBytes(quotaConfig.storageSizeBytes()), numReplicas);
      }
      LOGGER.warn(message);
      return failure(message);
    }
  }
}
