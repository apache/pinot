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
package org.apache.pinot.controller.validation;

import com.google.common.base.Preconditions;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to check if a new segment is within the configured storage quota for the table
 *
 */
public class StorageQuotaChecker {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageQuotaChecker.class);

  private final TableSizeReader _tableSizeReader;
  private final ControllerMetrics _controllerMetrics;
  private final LeadControllerManager _leadControllerManager;
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final boolean _isEnabled;
  private final int _timeoutMs;

  public StorageQuotaChecker(TableSizeReader tableSizeReader,
      ControllerMetrics controllerMetrics, LeadControllerManager leadControllerManager,
      PinotHelixResourceManager pinotHelixResourceManager, ControllerConf controllerConf) {
    _tableSizeReader = tableSizeReader;
    _controllerMetrics = controllerMetrics;
    _leadControllerManager = leadControllerManager;
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _isEnabled = controllerConf.getEnableStorageQuotaCheck();
    _timeoutMs = controllerConf.getServerAdminRequestTimeoutSeconds() * 1000;
    Preconditions.checkArgument(_timeoutMs > 0, "Timeout value must be > 0, input: %s", _timeoutMs);
  }

  public static class QuotaCheckerResponse {
    public boolean _isSegmentWithinQuota;
    public String _reason;

    QuotaCheckerResponse(boolean isSegmentWithinQuota, String reason) {
      _isSegmentWithinQuota = isSegmentWithinQuota;
      _reason = reason;
    }
  }

  public static QuotaCheckerResponse success(String msg) {
    return new QuotaCheckerResponse(true, msg);
  }

  public static QuotaCheckerResponse failure(String msg) {
    return new QuotaCheckerResponse(false, msg);
  }

  /**
   * Returns whether the new added segment is within the storage quota.
   */
  public QuotaCheckerResponse isSegmentStorageWithinQuota(TableConfig tableConfig, String segmentName,
      long segmentSizeInBytes)
      throws InvalidConfigException {
    if (!_isEnabled) {
      return success("Storage quota check is disabled, skipping the check");
    }

    // 1. Read table config
    // 2. read table size from all the servers
    // 3. update predicted segment sizes
    // 4. is the updated size within quota
    QuotaConfig quotaConfig = tableConfig.getQuotaConfig();
    int numReplicas = _pinotHelixResourceManager.getNumReplicas(tableConfig);

    final String tableNameWithType = tableConfig.getTableName();

    if (quotaConfig == null || quotaConfig.getStorage() == null) {
      // no quota configuration...so ignore for backwards compatibility
      String message =
          String.format("Storage quota is not configured for table: %s, skipping the check", tableNameWithType);
      LOGGER.info(message);
      return success(message);
    }

    long allowedStorageBytes = numReplicas * quotaConfig.getStorageInBytes();
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.TABLE_QUOTA, allowedStorageBytes);

    // read table size
    TableSizeReader.TableSubTypeSizeDetails tableSubtypeSize;
    try {
      tableSubtypeSize = _tableSizeReader.getTableSubtypeSize(tableNameWithType, _timeoutMs);
    } catch (InvalidConfigException e) {
      LOGGER.error("Failed to get table size for table {}", tableNameWithType, e);
      throw e;
    }

    if (tableSubtypeSize._estimatedSizeInBytes == -1) {
      // don't fail the quota check in this case
      return success("Missing size reports from all servers. Bypassing storage quota check for " + tableNameWithType);
    }

    // The logic inside this if block is applicable for missing segments as well as
    // when we are checking the quota for only existing segments (segmentSizeInBytes == 0)
    // as in both cases quota is checked across existing segments estimated size alone
    if (segmentSizeInBytes == 0 || tableSubtypeSize._missingSegments > 0) {
      emitStorageQuotaUtilizationMetric(tableNameWithType, tableSubtypeSize, allowedStorageBytes);
      if (tableSubtypeSize._estimatedSizeInBytes > allowedStorageBytes) {
        return failure("Table " + tableNameWithType + " already over quota. Estimated size for all replicas is "
            + DataSizeUtils.fromBytes(tableSubtypeSize._estimatedSizeInBytes) + ". Configured size for " + numReplicas
            + " is " + DataSizeUtils.fromBytes(allowedStorageBytes));
      } else {
        return success("Missing size report for " + tableSubtypeSize._missingSegments
            + " segments. Bypassing storage quota check for " + tableNameWithType);
      }
    }

    // If the segment exists(refresh), get the existing size
    TableSizeReader.SegmentSizeDetails sizeDetails = tableSubtypeSize._segments.get(segmentName);
    long existingSegmentSizeBytes = sizeDetails != null ? sizeDetails._estimatedSizeInBytes : 0;

    // Since tableNameWithType comes with the table type(OFFLINE), thus we guarantee that
    // tableSubtypeSize.estimatedSizeInBytes is the offline table size.
    _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.OFFLINE_TABLE_ESTIMATED_SIZE,
        tableSubtypeSize._estimatedSizeInBytes);

    LOGGER.info("Table {}'s estimatedSizeInBytes is {}. ReportedSizeInBytes (actual reports from servers) is {}",
        tableNameWithType, tableSubtypeSize._estimatedSizeInBytes, tableSubtypeSize._reportedSizeInBytes);

    emitStorageQuotaUtilizationMetric(tableNameWithType, tableSubtypeSize, allowedStorageBytes);

    // Note: incomingSegmentSizeBytes is uncompressed data size for just 1 replica,
    // while estimatedFinalSizeBytes is for all replicas of all segments put together.
    long totalIncomingSegmentSizeBytes = segmentSizeInBytes * numReplicas;
    long estimatedFinalSizeBytes =
        tableSubtypeSize._estimatedSizeInBytes - existingSegmentSizeBytes + totalIncomingSegmentSizeBytes;
    if (estimatedFinalSizeBytes <= allowedStorageBytes) {
      String message;
      if (sizeDetails == null) {
        // append use case
        message = String.format(
            "Appending Segment %s of Table %s is within quota. Total allowed storage size: %s ( = configured quota: "
                + "%s * number replicas: %d). New estimated table size of all replicas: %s. Current table size of all"
                + " replicas: %s. Incoming uncompressed segment size of all replicas: %s ( = single incoming "
                + "uncompressed segment size: %s * number replicas: %d). Formula: New estimated size = current table "
                + "size + incoming segment size", segmentName, tableNameWithType,
            DataSizeUtils.fromBytes(allowedStorageBytes), quotaConfig.getStorage(), numReplicas,
            DataSizeUtils.fromBytes(estimatedFinalSizeBytes),
            DataSizeUtils.fromBytes(tableSubtypeSize._estimatedSizeInBytes),
            DataSizeUtils.fromBytes(totalIncomingSegmentSizeBytes), DataSizeUtils.fromBytes(segmentSizeInBytes),
            numReplicas);
      } else {
        // refresh use case
        message = String.format(
            "Refreshing Segment %s of Table %s is within quota. Total allowed storage size: %s ( = configured quota: "
                + "%s * number replicas: %d). New estimated table size of all replicas: %s. Current table size of all"
                + " replicas: %s. Incoming uncompressed segment size of all replicas: %s ( = single incoming "
                + "uncompressed segment size: %s * number replicas: %d). Existing same segment size of all replicas: "
                + "%s. Formula: New estimated size = current table size - existing same segment size + incoming "
                + "segment size", segmentName, tableNameWithType, DataSizeUtils.fromBytes(allowedStorageBytes),
            quotaConfig.getStorage(), numReplicas, DataSizeUtils.fromBytes(estimatedFinalSizeBytes),
            DataSizeUtils.fromBytes(tableSubtypeSize._estimatedSizeInBytes),
            DataSizeUtils.fromBytes(totalIncomingSegmentSizeBytes), DataSizeUtils.fromBytes(segmentSizeInBytes),
            numReplicas, DataSizeUtils.fromBytes(existingSegmentSizeBytes));
      }
      LOGGER.info(message);
      return success(message);
    } else {
      String message;
      if (tableSubtypeSize._estimatedSizeInBytes > allowedStorageBytes) {
        message = String.format(
            "Table %s already over quota. Existing estimated uncompressed table size of all replicas: %s > total "
                + "allowed storage size: %s ( = configured quota: %s * num replicas: %d). Check if indexes were "
                + "enabled recently and adjust table quota accordingly.", tableNameWithType,
            DataSizeUtils.fromBytes(tableSubtypeSize._estimatedSizeInBytes),
            DataSizeUtils.fromBytes(allowedStorageBytes), quotaConfig.getStorage(), numReplicas);
      } else {
        message = String.format(
            "Storage quota exceeded for Table %s. New estimated size: %s > total allowed storage size: %s, where new "
                + "estimated size = existing estimated uncompressed size of all replicas: %s - existing segment sizes"
                + " of all replicas: %s + (incoming uncompressed segment size: %s * number replicas: %d), total "
                + "allowed storage size = configured quota: %s * number replicas: %d", tableNameWithType,
            DataSizeUtils.fromBytes(estimatedFinalSizeBytes), DataSizeUtils.fromBytes(allowedStorageBytes),
            DataSizeUtils.fromBytes(tableSubtypeSize._estimatedSizeInBytes),
            DataSizeUtils.fromBytes(existingSegmentSizeBytes), DataSizeUtils.fromBytes(segmentSizeInBytes), numReplicas,
            quotaConfig.getStorage(), numReplicas);
      }
      LOGGER.warn(message);
      return failure(message);
    }
  }

  private void emitStorageQuotaUtilizationMetric(String tableNameWithType, TableSizeReader.TableSubTypeSizeDetails
      tableSubtypeSize, long allowedStorageBytes) {
    // Only emit the real percentage of storage quota usage by lead controller, otherwise emit 0L.
    if (_leadControllerManager.isLeaderForTable(tableNameWithType)) {
      long existingStorageQuotaUtilization = tableSubtypeSize._estimatedSizeInBytes * 100 / allowedStorageBytes;
      _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.TABLE_STORAGE_QUOTA_UTILIZATION,
          existingStorageQuotaUtilization);
    } else {
      _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.TABLE_STORAGE_QUOTA_UTILIZATION, 0L);
    }
  }

  /**
   * Checks whether the table is within the storage quota.
   * @return true if storage quota is exceeded by the table, else false.
   */
  public boolean isTableStorageQuotaExceeded(TableConfig tableConfig) {
    try {
      return !isSegmentStorageWithinQuota(tableConfig, null, 0)._isSegmentWithinQuota;
    } catch (InvalidConfigException e) {
      // skip the check upon exception
      return false;
    }
  }
}
