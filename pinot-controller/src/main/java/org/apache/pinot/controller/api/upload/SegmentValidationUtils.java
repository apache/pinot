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

import java.util.OptionalLong;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.RetentionUtils;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.validation.StorageQuotaChecker;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
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

  /// When `controller.segment.upload.rejectOutOfRetention.enabled` is true on the controller, rejects the upload if
  /// the segment's data end time (from segment file metadata) is past the table's `retentionTimeValue` /
  /// `retentionTimeUnit` window, using the same boundary as the controller retention manager. **Index creation time is
  /// not** used as a fallback here: it can reflect upstream segment timestamps (for example upsert compaction) and is a
  /// poor proxy for the data window at upload time.
  ///
  /// Rejection uses HTTP 403 so callers that enable the controller flag can treat the response as a hard failure.
  /// Clients that do not enable the flag see no behavior change.
  ///
  /// Controller wiring: {@link org.apache.pinot.controller.api.resources.PinotSegmentUploadDownloadRestletResource}
  /// invokes this for single-segment upload only. METADATA batch upload (`POST /segments/batchUpload`) and
  /// reingested-segment upload do not call it.
  ///
  /// For tables where the retention manager does not apply time-based retention to completed segments (offline tables
  /// whose batch ingestion type is not `APPEND`), this method returns without evaluating retention.
  public static void rejectUploadIfOutOfRetention(SegmentMetadata segmentMetadata, TableConfig tableConfig,
      long currentTimeMs, boolean controllerRejectOutOfRetentionEnabled,
      @Nullable ControllerMetrics controllerMetrics) {
    if (!controllerRejectOutOfRetentionEnabled) {
      return;
    }
    SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
    if (validationConfig == null) {
      return;
    }
    if (!RetentionUtils.shouldManageTimeBasedDataRetention(tableConfig)) {
      return;
    }
    OptionalLong retentionMsOpt = RetentionUtils.parseTableDataRetentionMillis(validationConfig);
    if (retentionMsOpt.isEmpty()) {
      return;
    }
    long retentionMs = retentionMsOpt.getAsLong();
    if (RetentionUtils.isPurgeable(tableConfig.getTableName(), segmentMetadata, retentionMs, currentTimeMs, false)) {
      if (controllerMetrics != null) {
        controllerMetrics.addMeteredGlobalValue(ControllerMeter.OUT_OF_RETENTION_SEGMENT_UPLOAD_REJECTED, 1L);
      }
      throw new ControllerApplicationException(LOGGER, String.format(
          "Segment %s of table %s is outside the retention window (%s %s); upload rejected.",
          segmentMetadata.getName(), tableConfig.getTableName(), validationConfig.getRetentionTimeValue(),
          validationConfig.getRetentionTimeUnit()),
          Response.Status.FORBIDDEN);
    }
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
