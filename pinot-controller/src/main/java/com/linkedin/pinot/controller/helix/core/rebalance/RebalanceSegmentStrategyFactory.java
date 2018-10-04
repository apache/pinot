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
package com.linkedin.pinot.controller.helix.core.rebalance;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.controller.helix.core.sharding.SegmentAssignmentStrategyEnum;
import org.apache.helix.HelixManager;


/**
 * Singleton factory class, to fetch the right rebalance segments strategy based on table config
 */
public class RebalanceSegmentStrategyFactory {

  private static RebalanceSegmentStrategyFactory INSTANCE = null;

  private HelixManager _helixManager;

  private RebalanceSegmentStrategyFactory(HelixManager helixManager) {
    _helixManager = helixManager;
  }

  public static void createInstance(HelixManager helixManager) {
    if (INSTANCE != null) {
      throw new RuntimeException("Instance already created for " + RebalanceSegmentStrategyFactory.class.getName());
    }
    INSTANCE = new RebalanceSegmentStrategyFactory(helixManager);
  }

  public static RebalanceSegmentStrategyFactory getInstance() {
    if (INSTANCE == null) {
      throw new RuntimeException("Instance not yet created for " + RebalanceSegmentStrategyFactory.class.getName());
    }
    return INSTANCE;
  }

  public RebalanceSegmentStrategy getRebalanceSegmentsStrategy(TableConfig tableConfig) {
    // If we use replica group segment assignment strategy, we pick the replica group rebalancer
    String segmentAssignmentStrategy = tableConfig.getValidationConfig().getSegmentAssignmentStrategy();
    if (segmentAssignmentStrategy == null) {
      return new DefaultRebalanceSegmentStrategy(_helixManager);
    }
    switch (SegmentAssignmentStrategyEnum.valueOf(segmentAssignmentStrategy)) {
      case ReplicaGroupSegmentAssignmentStrategy:
        return new ReplicaGroupRebalanceSegmentStrategy(_helixManager);
      default:
        return new DefaultRebalanceSegmentStrategy(_helixManager);
    }
  }
}
