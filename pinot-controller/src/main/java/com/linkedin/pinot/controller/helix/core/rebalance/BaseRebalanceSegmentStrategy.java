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

package com.linkedin.pinot.controller.helix.core.rebalance;

import com.google.common.base.Function;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;


/**
 * Abstract base class for RebalanceSegmentStrategy, to encapsulate common methods between all strategies
 */
public abstract class BaseRebalanceSegmentStrategy implements RebalanceSegmentStrategy {

  private HelixManager _helixManager;

  BaseRebalanceSegmentStrategy(HelixManager helixManager) {
    _helixManager = helixManager;
  }

  /**
   * Helper method to update idealstate with the new segment assignment
   *  @param tableNameWithType Table name with type
   * @param numReplica The number of replica
   * @param segmentAssignmentMapping Segment assignment mapping
   */
  void updateIdealState(final String tableNameWithType, final int numReplica,
      final Map<String, Map<String, String>> segmentAssignmentMapping) {
    HelixHelper.updateIdealState(_helixManager, tableNameWithType, new Function<IdealState, IdealState>() {
      @Nullable
      @Override
      public IdealState apply(@Nullable IdealState idealState) {
        for (Map.Entry<String, Map<String, String>> entry : segmentAssignmentMapping.entrySet()) {
          idealState.setInstanceStateMap(entry.getKey(), entry.getValue());
        }
        idealState.setReplicas(Integer.toString(numReplica));
        return idealState;
      }
    }, RetryPolicies.exponentialBackoffRetryPolicy(5, 1000, 2.0f));
  }

}