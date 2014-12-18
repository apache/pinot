package com.linkedin.pinot.controller.helix.core.retention.strategy;

import com.linkedin.pinot.common.segment.SegmentMetadata;


/**
 * @author xiafu
 *
 */
public interface RetentionStrategy {

  // Return true when segment meets deletion conditions.
  boolean purgeSegment(SegmentMetadata segmentMetadata);
}
