/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.helix.core.rebalance;

import java.util.Map;


/**
 * The <code>TableRebalanceObserver</code> interface provides callbacks to take actions
 * during critical triggers. The 3 main triggers during a rebalance operation are show below.
 * For example, we can track stats + status of rebalance during these triggers.
 */

public interface TableRebalanceObserver {
  enum Trigger {
    // Start of rebalance Trigger
    START_TRIGGER,
    // Waiting for external view to converge towards ideal state
    EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER,
    // Ideal state changes due to external events and new target for rebalance is computed
    IDEAL_STATE_CHANGE_TRIGGER,
  }

  void onTrigger(Trigger trigger, Map<String, Map<String, String>> currentState,
      Map<String, Map<String, String>> targetState);

  void onSuccess(String msg);

  void onError(String errorMsg);

  boolean isStopped();

  RebalanceResult.Status getStopStatus();
}
