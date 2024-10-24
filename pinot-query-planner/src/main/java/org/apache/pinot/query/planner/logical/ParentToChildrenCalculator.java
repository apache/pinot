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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;


/**
 * Utility class to calculate the parent to children mapping for a given plan tree.
 */
public class ParentToChildrenCalculator {
  private ParentToChildrenCalculator() {
  }

  /**
   * Returns an identity map indexed by the parent node, with the value being a set of its <strong>direct</strong> child
   * nodes.
   */
  public static IdentityHashMap<MailboxSendNode, Set<MailboxSendNode>> calculate(MailboxSendNode root) {
    Visitor visitor = new Visitor();
    root.getInputs().forEach(node -> node.visit(visitor, root));

    return visitor._parentToChild;
  }

  private static class Visitor extends PlanNodeVisitor.DepthFirstVisitor<Void, MailboxSendNode> {
    private IdentityHashMap<MailboxSendNode, Set<MailboxSendNode>> _parentToChild = new IdentityHashMap<>();

    @Override
    protected Void defaultCase(PlanNode node, MailboxSendNode parent) {
      return null;
    }

    @Override
    public Void visitMailboxSend(MailboxSendNode node, MailboxSendNode parent) {
      _parentToChild.computeIfAbsent(parent, k -> Collections.newSetFromMap(new IdentityHashMap<>())).add(node);
      visitChildren(node, node); // children will be called with the current node as the parent
      return null;
    }
  }
}
