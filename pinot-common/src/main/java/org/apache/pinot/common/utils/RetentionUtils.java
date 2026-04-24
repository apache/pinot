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
package org.apache.pinot.common.utils;

import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility methods for evaluating segment retention eligibility.
 */
public class RetentionUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(RetentionUtils.class);

  private RetentionUtils() {
  }

  /**
   * Returns whether the given segment should be considered expired under the given retention period.
   * Uses the same strict greater-than comparison as {@code TimeRetentionStrategy}: a segment is expired when
   * {@code currentTimeMs - endTimeMs > retentionMs}.
   * <p>
   * If the segment's end time is invalid, the segment is treated as not expired (fail-open).
   * Callers that need to handle incomplete segments must do so before calling this method.
   *
   * @param tableNameWithType table name with type suffix, used for logging
   * @param segmentZKMetadata segment metadata
   * @param retentionMs       retention period in milliseconds (must be positive)
   * @param currentTimeMs     current wall-clock time in milliseconds
   * @return true if the segment is past the retention boundary, false otherwise
   */
  public static boolean isPurgeable(String tableNameWithType, SegmentZKMetadata segmentZKMetadata, long retentionMs,
      long currentTimeMs) {
    long endTimeMs = segmentZKMetadata.getEndTimeMs();
    if (!TimeUtils.timeValueInValidRange(endTimeMs)) {
      LOGGER.warn("Segment: {} of table: {} has invalid end time in millis: {}", segmentZKMetadata.getSegmentName(),
          tableNameWithType, endTimeMs);
      return false;
    }
    return currentTimeMs - endTimeMs > retentionMs;
  }
}
