/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.util.concurrent.TimeUnit;

import org.joda.time.Duration;

import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This strategy is default and will check the segment Interval from segmentMetadata and
 * purge segment passed the retention duration.
 *
 * @author xiafu
 *
 */
public class TimeRetentionStrategy implements RetentionStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeRetentionStrategy.class);

  private Duration _retentionDuration;

  public TimeRetentionStrategy(String timeUnit, String timeValue) throws Exception {
    try {
      _retentionDuration = new Duration(TimeUtils.toMillis(timeUnit, timeValue));
      if (_retentionDuration.getMillis() <= 0) {
        throw new RuntimeException("No retention value set.");
      }
    } catch (Exception e) {
      _retentionDuration = null;
      throw e;
    }
  }

  public TimeRetentionStrategy(TimeUnit retentionTimeUnit, int retentionTimeValue) {
    if (retentionTimeUnit != null && retentionTimeValue > 0) {
      _retentionDuration = new Duration(retentionTimeUnit.toMillis(retentionTimeValue));
    } else {
      _retentionDuration = null;
    }
  }

  @Override
  public boolean isPurgeable(SegmentZKMetadata segmentZKMetadata) {
    if (_retentionDuration == null || _retentionDuration.getMillis() <= 0) {
      return false;
    }
    try {
      TimeUnit segmentTimeUnit = segmentZKMetadata.getTimeUnit();
      long endsMills = segmentTimeUnit.toMillis(segmentZKMetadata.getEndTime());
      Duration segmentTimeUntilNow = new Duration(endsMills, System.currentTimeMillis());
      if (_retentionDuration.isShorterThan(segmentTimeUntilNow)) {
        return true;
      }
    } catch (Exception e) {
      LOGGER.warn("Caught exception while checking if a segment is purgeable", e);
      return false;
    }
    return false;
  }
}
