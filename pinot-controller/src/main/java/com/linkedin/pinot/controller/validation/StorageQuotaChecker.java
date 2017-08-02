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

package com.linkedin.pinot.controller.validation;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.QuotaConfig;
import com.linkedin.pinot.common.config.TableConfig;
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

  private final TableSizeReader tableSizeReader;
  private final TableConfig tableConfig;

  public StorageQuotaChecker(TableConfig tableConfig, TableSizeReader tableSizeReader) {
    this.tableConfig = tableConfig;
    this.tableSizeReader = tableSizeReader;
  }

  public class QuotaCheckerResponse {
    public boolean isSegmentWithinQuota;
    public String reason;
    QuotaCheckerResponse(boolean isSegmentWithinQuota, String reason) {
      this.isSegmentWithinQuota = isSegmentWithinQuota;
      this.reason = reason;
    }
  }
  /**
   * check if the segment represented by segmentFile is within the storage quota
   * @param segmentFile untarred segment. This should not be null.
   *                    segmentFile must exist on disk and must be a directory
   * @param tableNameWithType table name without type (OFFLINE/REALTIME) information
   * @param segmentName name of the segment being added
   * @param timeoutMsec timeout in milliseconds for reading table sizes from server
   *
   */
  public QuotaCheckerResponse isSegmentStorageWithinQuota(@Nonnull File segmentFile, @Nonnull String tableNameWithType,
      @Nonnull String segmentName,
      @Nonnegative int timeoutMsec) {
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
    QuotaConfig quotaConfig = tableConfig.getQuotaConfig();
    int numReplicas = tableConfig.getValidationConfig().getReplicationNumber();
    final String tableName = tableConfig.getTableName();

    if (quotaConfig == null) {
      // no quota configuration...so ignore for backwards compatibility
      return new QuotaCheckerResponse(true,
          "Quota configuration not set for table: " +tableNameWithType);
    }

    long allowedStorageBytes = numReplicas * quotaConfig.storageSizeBytes();
    if (allowedStorageBytes < 0) {
      return new QuotaCheckerResponse(true,
          "Storage quota is not configured for table: " + tableNameWithType);
    }

    long incomingSegmentSizeBytes = FileUtils.sizeOfDirectory(segmentFile);

    // read table size
    TableSizeReader.TableSubTypeSizeDetails tableSubtypeSize =
        tableSizeReader.getTableSubtypeSize(tableNameWithType, timeoutMsec);

    // If the segment exists(refresh), get the existing size
    TableSizeReader.SegmentSizeDetails sizeDetails = tableSubtypeSize.segments.get(segmentName);
    long existingSegmentSizeBytes = sizeDetails != null ?  sizeDetails.estimatedSizeInBytes : 0;

    long estimatedFinalSizeBytes = tableSubtypeSize.estimatedSizeInBytes - existingSegmentSizeBytes + incomingSegmentSizeBytes;
    if (estimatedFinalSizeBytes <= allowedStorageBytes) {
      return new QuotaCheckerResponse(true,
          String.format("Estimated size: %d bytes is within the configured quota of %d (bytes) for table %s. Incoming segment size: %d (bytes)",
          estimatedFinalSizeBytes, allowedStorageBytes, tableName, incomingSegmentSizeBytes) );
    } else {
      return new QuotaCheckerResponse(false,
          String.format("Estimated size: %d bytes exceeds the configured quota of %d (bytes) for table %s. Incoming segment size: %d (bytes)",
          estimatedFinalSizeBytes, allowedStorageBytes, tableName, incomingSegmentSizeBytes));
    }
  }
}
