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
package org.apache.pinot.controller.helix.core.retention.strategy;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>TimeRetentionStrategy</code> class uses segment end time to manage the retention for segments.
 */
public class TimeRetentionStrategy implements RetentionStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeRetentionStrategy.class);

  private final long _retentionMs;
  private final boolean _useCreationTimeFallback;

  public TimeRetentionStrategy(TimeUnit timeUnit, long timeValue) {
    this(timeUnit, timeValue, false);
  }

  public TimeRetentionStrategy(TimeUnit timeUnit, long timeValue, boolean useCreationTimeFallback) {
    _retentionMs = timeUnit.toMillis(timeValue);
    _useCreationTimeFallback = useCreationTimeFallback;
  }

  @Override
  public boolean isPurgeable(String tableNameWithType, SegmentZKMetadata segmentZKMetadata) {

    // For realtime tables, only completed segments(DONE or UPLOADED) are eligible for purging.
    // For offline tables, status defaults to UPLOADED which is completed, so they proceed to normal retention
    if (!segmentZKMetadata.getStatus().isCompleted()) {
      return false; // Incomplete segments don't have final end time and should not be purged
    }

    String segmentName = segmentZKMetadata.getSegmentName();
    long endTimeMs = segmentZKMetadata.getEndTimeMs();

    // If end time is valid, use it directly
    if (TimeUtils.timeValueInValidRange(endTimeMs)) {
      return System.currentTimeMillis() - endTimeMs > _retentionMs;
    }

    long creationTimeMs = segmentZKMetadata.getCreationTime();

    if (_useCreationTimeFallback && TimeUtils.timeValueInValidRange(creationTimeMs)) {
      LOGGER.debug("Segment: {} of table: {} has invalid end time: {}. Using creation time: {} as fallback",
          segmentName, tableNameWithType, endTimeMs, creationTimeMs);
      return System.currentTimeMillis() - creationTimeMs > _retentionMs;
    }

    if (_useCreationTimeFallback) {
      LOGGER.warn("Segment: {} of table: {} has invalid end time: {} and invalid creation time: {}. "
          + "Cannot determine retention, skipping", segmentName, tableNameWithType, endTimeMs, creationTimeMs);
    } else {
      LOGGER.warn("Segment: {} of table: {} has invalid end time in millis: {}. "
          + "Creation time fallback is disabled", segmentName, tableNameWithType, endTimeMs);
    }
    return false;
  }

  @Override
  public boolean isPurgeable(String tableNameWithType, String segmentName, long segmentTimeMs) {

    // Check that the end time is between 1971 and 2071
    if (!TimeUtils.timeValueInValidRange(segmentTimeMs)) {
      LOGGER.warn("Segment: {} of table: {} has invalid end time in millis: {}", segmentName,
          tableNameWithType, segmentTimeMs);
      return false;
    }

    return System.currentTimeMillis() - segmentTimeMs > _retentionMs;
  }
}
