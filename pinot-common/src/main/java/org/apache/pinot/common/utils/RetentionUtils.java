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
   * Implements the time-comparison and creation-time fallback logic used by {@code TimeRetentionStrategy} for
   * completed segments. Does NOT check segment completion status — callers must guarantee that only completed
   * segments (DONE or UPLOADED) are passed in.
   * <ul>
   *   <li>If end time is valid: expired when {@code currentTimeMs - endTimeMs > retentionMs}.</li>
   *   <li>If end time is invalid and {@code useCreationTimeFallback} is true and creation time is valid:
   *       expired when {@code currentTimeMs - creationTimeMs > retentionMs}.</li>
   *   <li>Otherwise: not expired (fail-open).</li>
   * </ul>
   *
   * @param tableNameWithType        table name with type suffix, used for logging
   * @param segmentZKMetadata        segment metadata
   * @param retentionMs              retention period in milliseconds (must be positive)
   * @param currentTimeMs            current wall-clock time in milliseconds
   * @param useCreationTimeFallback  when true, fall back to creation time if end time is invalid
   *                                 (must match {@code controller.retentionManager.enableCreationTimeFallback})
   * @return true if the segment is past the retention boundary, false otherwise
   */
  public static boolean isPurgeable(String tableNameWithType, SegmentZKMetadata segmentZKMetadata, long retentionMs,
      long currentTimeMs, boolean useCreationTimeFallback) {
    String segmentName = segmentZKMetadata.getSegmentName();
    long endTimeMs = segmentZKMetadata.getEndTimeMs();
    if (TimeUtils.timeValueInValidRange(endTimeMs)) {
      return currentTimeMs - endTimeMs > retentionMs;
    }
    if (useCreationTimeFallback) {
      long creationTimeMs = segmentZKMetadata.getCreationTime();
      if (TimeUtils.timeValueInValidRange(creationTimeMs)) {
        LOGGER.debug("Segment: {} of table: {} has invalid end time: {}. Using creation time: {} as fallback",
            segmentName, tableNameWithType, endTimeMs, creationTimeMs);
        return currentTimeMs - creationTimeMs > retentionMs;
      }
      LOGGER.warn("Segment: {} of table: {} has invalid end time: {} and invalid creation time: {}. "
          + "Cannot determine retention, skipping", segmentName, tableNameWithType, endTimeMs, creationTimeMs);
    } else {
      LOGGER.warn("Segment: {} of table: {} has invalid end time in millis: {}. "
          + "Creation time fallback is disabled", segmentName, tableNameWithType, endTimeMs);
    }
    return false;
  }
}
