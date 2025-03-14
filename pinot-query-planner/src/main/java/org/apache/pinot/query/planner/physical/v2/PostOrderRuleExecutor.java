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
package org.apache.pinot.query.planner.physical.v2;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.apache.pinot.query.context.PhysicalPlannerContext;


public class PostOrderRuleExecutor extends RuleExecutor {
  private final Deque<PRelNode> _parents = new ArrayDeque<>();

  protected PostOrderRuleExecutor() {
  }

  @Override
  public PRelNode execute(PRelNode currentNode, PRelOptRule rule, PhysicalPlannerContext context) {
    _parents.addLast(currentNode);
    List<PRelNode> newInputs = new ArrayList<>();
    try {
      for (PRelNode input : currentNode.getInputs()) {
        newInputs.add(execute(input, rule, context));
      }
    } finally {
      _parents.removeLast();
    }
    currentNode = currentNode.withNewInputs(currentNode.getNodeId(), newInputs, currentNode.getPinotDataDistribution());
    PRelOptRuleCall call = new PRelOptRuleCall(currentNode, _parents, context);
    if (rule.matches(call)) {
      currentNode = rule.onMatch(call);
    }
    currentNode = rule.onDone(currentNode);
    return currentNode;
  }
}
