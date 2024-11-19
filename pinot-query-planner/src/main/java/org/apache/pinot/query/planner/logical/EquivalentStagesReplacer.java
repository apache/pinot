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

import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;


/**
 * EquivalentStageReplacer is used to replace equivalent stages in the query plan.
 *
 * Given a {@link org.apache.pinot.query.planner.plannode.PlanNode} and a
 * {@link GroupedStages}, modifies the plan node to replace equivalent stages.
 *
 * For each {@link MailboxReceiveNode} in the plan, if the sender is not the leader of the group,
 * replaces the sender with the leader.
 * The leader is also updated to include the receiver in its list of receivers.
 */
public class EquivalentStagesReplacer {
  private EquivalentStagesReplacer() {
  }

  /**
   * Replaces the equivalent stages in the query plan.
   *
   * @param root Root plan node
   * @param equivalentStages Equivalent stages
   */
  public static void replaceEquivalentStages(PlanNode root, GroupedStages equivalentStages) {
    root.visit(Replacer.INSTANCE, equivalentStages);
  }

  private static class Replacer extends PlanNodeVisitor.DepthFirstVisitor<Void, GroupedStages> {
    private static final Replacer INSTANCE = new Replacer();

    private Replacer() {
    }

    @Override
    public Void visitMailboxReceive(MailboxReceiveNode node, GroupedStages equivalenceGroups) {
      MailboxSendNode sender = node.getSender();
      MailboxSendNode leader = equivalenceGroups.getGroup(sender).first();
      if (leader != sender) {
        node.setSender(leader);
        leader.addReceiver(node);
      }
      return null;
    }
  }
}
