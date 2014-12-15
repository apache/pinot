package com.linkedin.pinot.controller.helix.core.sharding;

/**
 * Supported SegmentAssignmentStrategies.
 * 
 * @author xiafu
 *
 */
public enum SegmentAssignmentStrategyEnum {
  RandomAssignmentStrategy,
  BalanceNumSegmentAssignmentStrategy,
  BucketizedSegmentAssignmentStrategy;

}
