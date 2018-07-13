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
package com.linkedin.pinot.controller.helix.core.retention.strategy;

import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>TimeRetentionStrategy</code> class uses segment end time to manage the retention for segments.
 */
public class TimeRetentionStrategy implements RetentionStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeRetentionStrategy.class);

  private final long _retentionMs;

  public TimeRetentionStrategy(TimeUnit timeUnit, long timeValue) {
    _retentionMs = timeUnit.toMillis(timeValue);
  }

  @Override
  public boolean isPurgeable(SegmentZKMetadata segmentZKMetadata) {
    TimeUnit timeUnit = segmentZKMetadata.getTimeUnit();
    if (timeUnit == null) {
      LOGGER.warn("Time unit is not set for {} segment: {} of table: {}", segmentZKMetadata.getSegmentType(),
          segmentZKMetadata.getSegmentName(), segmentZKMetadata.getTableName());
      return false;
    }
    long endTime = segmentZKMetadata.getEndTime();
    long endTimeMs = timeUnit.toMillis(endTime);

    // Check that the end time is between 1971 and 2071
    if (!TimeUtils.timeValueInValidRange(endTimeMs)) {
      LOGGER.warn("{} segment: {} of table: {} has invalid end time: {} {}", segmentZKMetadata.getSegmentType(),
          segmentZKMetadata.getSegmentName(), segmentZKMetadata.getTableName(), endTime, timeUnit);
      return false;
    }

    return System.currentTimeMillis() - endTimeMs > _retentionMs;
  }
}
