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
import org.joda.time.Interval;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.time.TimeUtils;


/**
 * This strategy is default and will check the segment Interval from segmentMetadata and
 * purge segment passed the retention duration.
 *
 * @author xiafu
 *
 */
public class TimeRetentionStrategy implements RetentionStrategy {

  private Duration _retentionDuration;

  public TimeRetentionStrategy(String timeUnit, String timeValue) throws Exception {
    try {
      _retentionDuration = new Duration(TimeUtils.toMillis(timeUnit, timeValue));
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
  public boolean isPurgeable(SegmentMetadata segmentMetadata) {
    if (_retentionDuration == null || _retentionDuration.getMillis() <= 0) {
      return false;
    }
    Interval timeInterval = segmentMetadata.getTimeInterval();
    if (timeInterval == null) {
      return false;
    }
    long endsMills = timeInterval.getEndMillis();
    Duration segmentTimeUntilNow = new Duration(endsMills, System.currentTimeMillis());
    if (_retentionDuration.isShorterThan(segmentTimeUntilNow)) {
      return true;
    }
    return false;
  }

}
