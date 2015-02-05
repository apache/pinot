package com.linkedin.pinot.controller.helix.core.retention.strategy;

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
