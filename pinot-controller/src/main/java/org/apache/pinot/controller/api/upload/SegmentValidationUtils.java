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

import java.util.concurrent.Executor;
import javax.ws.rs.core.Response;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
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

  public static void checkStorageQuota(String segmentName, long segmentSizeInBytes, TableConfig tableConfig,
      PinotHelixResourceManager resourceManager, ControllerConf controllerConf, ControllerMetrics controllerMetrics,
      HttpClientConnectionManager connectionManager, Executor executor, boolean isLeaderForTable) {
    if (!controllerConf.getEnableStorageQuotaCheck()) {
      return;
    }
    TableSizeReader tableSizeReader =
        new TableSizeReader(executor, connectionManager, controllerMetrics, resourceManager);
    StorageQuotaChecker quotaChecker =
        new StorageQuotaChecker(tableConfig, tableSizeReader, controllerMetrics, isLeaderForTable, resourceManager);
    StorageQuotaChecker.QuotaCheckerResponse response;
    try {
      response = quotaChecker.isSegmentStorageWithinQuota(segmentName, segmentSizeInBytes,
          controllerConf.getServerAdminRequestTimeoutSeconds() * 1000);
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
