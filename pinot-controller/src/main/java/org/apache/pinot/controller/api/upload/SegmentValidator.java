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

import java.io.File;
import java.util.concurrent.Executor;
import javax.ws.rs.core.Response;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.resources.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.controller.validation.StorageQuotaChecker;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.TimeUtils;
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
  private final boolean _isLeaderForTable;

  public SegmentValidator(PinotHelixResourceManager pinotHelixResourceManager, ControllerConf controllerConf,
      Executor executor, HttpConnectionManager connectionManager, ControllerMetrics controllerMetrics,
      boolean isLeaderForTable) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _controllerConf = controllerConf;
    _executor = executor;
    _connectionManager = connectionManager;
    _controllerMetrics = controllerMetrics;
    _isLeaderForTable = isLeaderForTable;
  }

  public void validateOfflineSegment(String offlineTableName, SegmentMetadata segmentMetadata, File tempSegmentDir) {
    TableConfig offlineTableConfig =
        ZKMetadataProvider.getOfflineTableConfig(_pinotHelixResourceManager.getPropertyStore(), offlineTableName);
    if (offlineTableConfig == null) {
      throw new ControllerApplicationException(LOGGER, "Failed to find table config for table: " + offlineTableName,
          Response.Status.NOT_FOUND);
    }

    String segmentName = segmentMetadata.getName();
    StorageQuotaChecker.QuotaCheckerResponse quotaResponse;
    try {
      quotaResponse = checkStorageQuota(tempSegmentDir, segmentMetadata, offlineTableConfig);
    } catch (InvalidConfigException e) {
      // Admin port is missing, return response with 500 status code.
      throw new ControllerApplicationException(LOGGER,
          "Quota check failed for segment: " + segmentName + " of table: " + offlineTableName + ", reason: " + e
              .getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
    }
    if (!quotaResponse.isSegmentWithinQuota) {
      throw new ControllerApplicationException(LOGGER,
          "Quota check failed for segment: " + segmentName + " of table: " + offlineTableName + ", reason: "
              + quotaResponse.reason, Response.Status.FORBIDDEN);
    }

    // Check time interval
    // TODO: Pass in schema and check the existence of time interval when time field exists
    Interval timeInterval = segmentMetadata.getTimeInterval();
    if (timeInterval != null && !TimeUtils.isValidTimeInterval(timeInterval)) {
      throw new ControllerApplicationException(LOGGER, String.format(
          "Invalid segment start/end time: %s (in millis: %d/%d) for segment: %s of table: %s, must be between: %s",
          timeInterval, timeInterval.getStartMillis(), timeInterval.getEndMillis(), segmentName, offlineTableName,
          TimeUtils.VALID_TIME_INTERVAL), Response.Status.NOT_ACCEPTABLE);
    }
  }

  /**
   * check if the segment represented by segmentFile is within the storage quota
   * @param segmentFile untarred segment. This should not be null.
   *                    segmentFile must exist on disk and must be a directory
   * @param metadata segment metadata. This should not be null.
   * @param offlineTableConfig offline table configuration. This should not be null.
   */
  private StorageQuotaChecker.QuotaCheckerResponse checkStorageQuota(File segmentFile, SegmentMetadata metadata,
      TableConfig offlineTableConfig)
      throws InvalidConfigException {
    if (!_controllerConf.getEnableStorageQuotaCheck()) {
      return StorageQuotaChecker.success("Quota check is disabled");
    }
    TableSizeReader tableSizeReader =
        new TableSizeReader(_executor, _connectionManager, _controllerMetrics, _pinotHelixResourceManager);
    StorageQuotaChecker quotaChecker =
        new StorageQuotaChecker(offlineTableConfig, tableSizeReader, _controllerMetrics, _isLeaderForTable);
    return quotaChecker.isSegmentStorageWithinQuota(metadata.getName(), FileUtils.sizeOfDirectory(segmentFile),
        _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000);
  }
}
