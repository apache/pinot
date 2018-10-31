/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.helix.core.sharding;

/**
 * Get SegmentAssignmentStrategyFactory methods.
 *
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
      case ReplicaGroupSegmentAssignmentStrategy:
        return new ReplicaGroupSegmentAssignmentStrategy();
      default:
        return new BalanceNumSegmentAssignmentStrategy();
    }
  }

}
