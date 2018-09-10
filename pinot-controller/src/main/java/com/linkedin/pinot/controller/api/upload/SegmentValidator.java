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
package com.linkedin.pinot.controller.api.upload;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.exception.InvalidConfigException;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.api.resources.ControllerApplicationException;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.util.TableSizeReader;
import com.linkedin.pinot.controller.validation.StorageQuotaChecker;
import java.io.File;
import java.util.Date;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;
import javax.ws.rs.core.Response;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * SegmentValidator is a util class used during segment upload. It does verification such as a quota check and validating
 * that the segment time values stored in the segment are valid.
 */
public class SegmentValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentValidator.class);
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final ControllerConf _controllerConf;
  private final Executor _executor;
  private final HttpConnectionManager _connectionManager;
  private final ControllerMetrics _controllerMetrics;

  public SegmentValidator(PinotHelixResourceManager pinotHelixResourceManager, ControllerConf controllerConf,
      Executor executor, HttpConnectionManager connectionManager, ControllerMetrics controllerMetrics) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _controllerConf = controllerConf;
    _executor = executor;
    _connectionManager = connectionManager;
    _controllerMetrics = controllerMetrics;

  }

  public void validateSegment(SegmentMetadata segmentMetadata, File tempSegmentDir) {
    String rawTableName = segmentMetadata.getTableName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
    String segmentName = segmentMetadata.getName();
    TableConfig offlineTableConfig =
        ZKMetadataProvider.getOfflineTableConfig(_pinotHelixResourceManager.getPropertyStore(), offlineTableName);

    if (offlineTableConfig == null) {
      throw new ControllerApplicationException(LOGGER, "Failed to find table config for table: " + offlineTableName,
          Response.Status.NOT_FOUND);
    }

    StorageQuotaChecker.QuotaCheckerResponse quotaResponse;
    try {
      quotaResponse = checkStorageQuota(tempSegmentDir, segmentMetadata, offlineTableConfig);
    } catch (InvalidConfigException e) {
      // Admin port is missing, return response with 500 status code.
      throw new ControllerApplicationException(LOGGER,
          "Quota check failed for segment: " + segmentName + " of table: " + offlineTableName + ", reason: " + e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
    }
    if (!quotaResponse.isSegmentWithinQuota) {
      throw new ControllerApplicationException(LOGGER,
          "Quota check failed for segment: " + segmentName + " of table: " + offlineTableName + ", reason: " + quotaResponse.reason, Response.Status.FORBIDDEN);
    }

    // Check time range
    if (!isSegmentTimeValid(segmentMetadata)) {
      throw new ControllerApplicationException(LOGGER,
          "Invalid segment start/end time for segment: " + segmentName + " of table: " + offlineTableName,
          Response.Status.NOT_ACCEPTABLE);
    }
  }

  /**
   * check if the segment represented by segmentFile is within the storage quota
   * @param segmentFile untarred segment. This should not be null.
   *                    segmentFile must exist on disk and must be a directory
   * @param metadata segment metadata. This should not be null.
   * @param offlineTableConfig offline table configuration. This should not be null.
   */
  private StorageQuotaChecker.QuotaCheckerResponse checkStorageQuota(@Nonnull File segmentFile,
      @Nonnull SegmentMetadata metadata, @Nonnull TableConfig offlineTableConfig) throws InvalidConfigException {
    if (!_controllerConf.getEnableStorageQuotaCheck()) {
      return StorageQuotaChecker.success("Quota check is disabled");
    }
    TableSizeReader tableSizeReader = new TableSizeReader(_executor, _connectionManager,
        _controllerMetrics, _pinotHelixResourceManager);
    StorageQuotaChecker quotaChecker = new StorageQuotaChecker(offlineTableConfig, tableSizeReader, _controllerMetrics, _pinotHelixResourceManager);
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(metadata.getTableName());
    return quotaChecker.isSegmentStorageWithinQuota(segmentFile, offlineTableName, metadata.getName(),
        _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000);
  }

  /**
   * Returns true if:
   * - Segment does not have a start/end time, OR
   * - The start/end time are in a valid range (Jan 01 1971 - Jan 01, 2071)
   * @param metadata Segment metadata
   * @return
   */
  private boolean isSegmentTimeValid(SegmentMetadata metadata) {
    Interval interval = metadata.getTimeInterval();
    if (interval == null) {
      return true;
    }

    long startMillis = interval.getStartMillis();
    long endMillis = interval.getEndMillis();

    if (!TimeUtils.timeValueInValidRange(startMillis) || !TimeUtils.timeValueInValidRange(endMillis)) {
      Date minDate = new Date(TimeUtils.getValidMinTimeMillis());
      Date maxDate = new Date(TimeUtils.getValidMaxTimeMillis());

      LOGGER.error(
          "Invalid start time '{}ms' or end time '{}ms' for segment {}, must be between '{}' and '{}' (timecolumn {}, timeunit {})",
          interval.getStartMillis(), interval.getEndMillis(), metadata.getName(), minDate, maxDate,
          metadata.getTimeColumn(), metadata.getTimeUnit().toString());
      return false;
    }

    return true;
  }
}
