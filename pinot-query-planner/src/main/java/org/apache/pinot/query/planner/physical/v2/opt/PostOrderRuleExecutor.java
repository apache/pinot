package org.apache.pinot.query.planner.physical.v2.opt;

import org.apache.calcite.rel.RelNode;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.physical.v2.PRelNode;


public class PostOrderRuleExecutor extends RuleExecutor {
  private final PRelOptRule _rule;
  private final PhysicalPlannerContext _physicalPlannerContext;

  PostOrderRuleExecutor(PRelOptRule rule, PhysicalPlannerContext context) {
    _rule = rule;
    _physicalPlannerContext = context;
  }

  @Override
  public PRelNode execute(PRelNode currentNode) {
    // Step-1: Execute for all inputs from left to right.
    currentNode = executeForInputs(currentNode, 0, currentNode.getPRelInputs().size());
    // Step-2: Execute for the current node, if the rule matches.
    PRelOptRuleCall call = new PRelOptRuleCall(currentNode, _parents, _physicalPlannerContext);
    if (_rule.matches(call)) {
      currentNode = _rule.onMatch(call);
    }
    // Step-4: Call the onDone hook to allow the rule to execute custom actions on completion.
    currentNode = _rule.onDone(currentNode);
    return currentNode;
  }
}
