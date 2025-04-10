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
package org.apache.pinot.query.planner.physical.v2.opt;

import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.physical.v2.PRelNode;


/**
 * A RuleExecutor that runs a DFS and processes the tree in the following order:
 * <ol>
 *   <li>First process the leftmost subtree input (if any)</li>
 *   <li>Then process the current node</li>
 *   <li>Then process all the other inputs</li>
 * </ol>
 */
public class LeftInputFirstRuleExecutor extends RuleExecutor {
  private final PRelOptRule _rule;
  private final PhysicalPlannerContext _physicalPlannerContext;

  LeftInputFirstRuleExecutor(PRelOptRule rule, PhysicalPlannerContext context) {
    _rule = rule;
    _physicalPlannerContext = context;
  }

  public PRelNode execute(PRelNode currentNode) {
    // Step-1: Execute left-most input if it exists.
    if (!currentNode.getPRelInputs().isEmpty()) {
      currentNode = executeForInputs(currentNode, 0, 1);
    }
    // Step-2: Execute for the current node, if the rule matches.
    PRelOptRuleCall call = new PRelOptRuleCall(currentNode, _parents, _physicalPlannerContext);
    if (_rule.matches(call)) {
      currentNode = _rule.onMatch(call);
    }
    // Step-3: Execute remaining inputs if they exist.
    if (currentNode.getPRelInputs().size() > 1) {
      currentNode = executeForInputs(currentNode, 1, currentNode.getPRelInputs().size());
    }
    // Step-4: Call the onDone hook to allow the rule to execute custom actions on completion.
    currentNode = _rule.onDone(currentNode);
    return currentNode;
  }
}
