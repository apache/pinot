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
package org.apache.pinot.query.planner.logical;

import javax.annotation.Nullable;


/// One decision of [EquivalentStagesReplacer]: whether a receive that read from `replacedStageId` now reads from
/// `leaderStageId`, the stage that the replacer keeps for the equivalence group.
///
/// @param reason why the substitution was rejected, or null when it was applied
public record SpoolSubstitution(int receiverStageId, int replacedStageId, int leaderStageId, Outcome outcome,
                                @Nullable String reason) {
  /// The result of a decision.
  public enum Outcome {
    /// The receive reads from the leader, and the replaced stage is removed from the plan.
    APPLIED,
    /// [SpoolDeadlockGuard] found that the substitution can block the query, so the replaced stage stays.
    REJECTED_BY_GUARD,
    /// The leader already sends to the stage of the receive, and a stage can receive from a sender only once.
    REJECTED_SAME_RECEIVER_STAGE
  }

  @Override
  public String toString() {
    return "stage " + receiverStageId + ": " + replacedStageId + " -> " + leaderStageId + " " + outcome
        + (reason != null ? " (" + reason + ")" : "");
  }
}
