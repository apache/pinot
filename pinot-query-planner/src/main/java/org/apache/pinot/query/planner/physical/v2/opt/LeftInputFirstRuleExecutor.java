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
