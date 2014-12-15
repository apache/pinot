package com.linkedin.pinot.controller.helix.core.sharding;

/**
 * Get SegmentAssignmentStrategyFactory methods.
 *
 * @author xiafu
 *
 */
public class SegmentAssignmentStrategyFactory {

  public static SegmentAssignmentStrategy getSegmentAssignmentStrategy(String strategy) {

    if (strategy == null || strategy.equals("null")) {
      return new BalanceNumSegmentAssignmentStrategy();
    }

    switch (SegmentAssignmentStrategyEnum.valueOf(strategy)) {
      case BalanceNumSegmentAssignmentStrategy:
        return new BalanceNumSegmentAssignmentStrategy();
      case RandomAssignmentStrategy:
        return new RandomAssignmentStrategy();
      case BucketizedSegmentAssignmentStrategy:
        return new BucketizedSegmentStrategy();
      default:
        return new BalanceNumSegmentAssignmentStrategy();
    }
  }


}
