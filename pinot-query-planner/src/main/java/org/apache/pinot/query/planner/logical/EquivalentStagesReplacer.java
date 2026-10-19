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

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;


/// EquivalentStageReplacer is used to replace equivalent stages in the query plan.
///
/// Given a [org.apache.pinot.query.planner.plannode.PlanNode] and a
/// [GroupedStages], modifies the plan node to replace equivalent stages.
///
/// For each [MailboxReceiveNode] in the plan, if the sender is not the leader of the group,
/// replaces the sender with the leader.
/// The leader is also updated to include the receiver in its list of receivers.
///
/// When the replacement is guarded, each substitution is kept only if [SpoolDeadlockGuard] finds no risk in the
/// resulting plan. Otherwise the substitution is undone and the original sender stays.
public class EquivalentStagesReplacer {
  private EquivalentStagesReplacer() {
  }

  public static void replaceEquivalentStages(PlanNode root, GroupedStages equivalentStages) {
    replaceEquivalentStages(root, equivalentStages, OnSubstitution.NO_OP);
  }

  /// Replaces the equivalent stages in the query plan, without [SpoolDeadlockGuard].
  ///
  /// @param root Root plan node
  /// @param equivalentStages Equivalent stages
  public static void replaceEquivalentStages(PlanNode root, GroupedStages equivalentStages, OnSubstitution listener) {
    replaceEquivalentStages(root, equivalentStages, listener, false);
  }

  /// Replaces the equivalent stages in the query plan.
  ///
  /// @param root Root plan node
  /// @param equivalentStages Equivalent stages
  /// @param listener Called once for each applied substitution
  /// @param guarded Whether each substitution must pass [SpoolDeadlockGuard]
  /// @return One entry for each receive whose sender is not the leader of its group, in the order of the decisions
  public static List<SpoolSubstitution> replaceEquivalentStages(PlanNode root, GroupedStages equivalentStages,
      OnSubstitution listener, boolean guarded) {
    Replacer replacer = new Replacer(root, listener, guarded);
    root.visit(replacer, equivalentStages);
    return replacer._substitutions;
  }

  public interface OnSubstitution {
    OnSubstitution NO_OP = (receiver, oldSender, newSender) -> {
    };
    void onSubstitution(int receiver, int oldSender, int newSender);
  }

  private static class Replacer extends PlanNodeVisitor.DepthFirstVisitor<Void, GroupedStages> {
    private final PlanNode _root;
    private final OnSubstitution _listener;
    private final boolean _guarded;
    private final List<SpoolSubstitution> _substitutions = new ArrayList<>();
    /// A stage can be visited more than once (for example the leader of a group), so a receive is decided only once.
    private final Set<MailboxReceiveNode> _visitedReceives = Collections.newSetFromMap(new IdentityHashMap<>());

    public Replacer(PlanNode root, OnSubstitution listener, boolean guarded) {
      _root = root;
      _listener = listener;
      _guarded = guarded;
    }

    @Override
    public Void visitMailboxReceive(MailboxReceiveNode node, GroupedStages equivalenceGroups) {
      if (!_visitedReceives.add(node)) {
        return null;
      }
      MailboxSendNode sender = node.getSender();
      MailboxSendNode leader = equivalenceGroups.getGroup(sender).first();
      if (sender == leader) {
        visitMailboxSend(leader, equivalenceGroups);
        return null;
      }
      if (leader.sharesReceiverStages(sender)) {
        // The leader is already sending to this stage. Given we don't have the ability to send to multiple
        // receivers in the same stage, we cannot optimize this case right now.
        _substitutions.add(new SpoolSubstitution(node.getStageId(), sender.getStageId(), leader.getStageId(),
            SpoolSubstitution.Outcome.REJECTED_SAME_RECEIVER_STAGE, null));
        visitMailboxSend(leader, equivalenceGroups);
        return null;
      }
      node.setSender(leader);
      leader.addReceiver(node);
      if (_guarded) {
        SpoolDeadlockGuard.DeadlockRisk risk = SpoolDeadlockGuard.findRisk(_root);
        if (risk != null) {
          leader.removeReceiver(node);
          node.setSender(sender);
          _substitutions.add(new SpoolSubstitution(node.getStageId(), sender.getStageId(), leader.getStageId(),
              SpoolSubstitution.Outcome.REJECTED_BY_GUARD, risk.toString()));
          // The original sender stays in the plan, so the stages below it can still be shared.
          visitMailboxSend(sender, equivalenceGroups);
          return null;
        }
      }
      // we don't want to visit the children of the node given it is going to be pruned
      _listener.onSubstitution(node.getStageId(), sender.getStageId(), leader.getStageId());
      _substitutions.add(new SpoolSubstitution(node.getStageId(), sender.getStageId(), leader.getStageId(),
          SpoolSubstitution.Outcome.APPLIED, null));
      return null;
    }
  }
}
