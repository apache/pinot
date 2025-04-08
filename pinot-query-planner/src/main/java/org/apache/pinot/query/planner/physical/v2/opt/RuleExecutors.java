package org.apache.pinot.query.planner.physical.v2.opt;

import org.apache.pinot.query.context.PhysicalPlannerContext;


public class RuleExecutors {
  private RuleExecutors() {
  }

  public static RuleExecutor create(Type type, PRelOptRule rule, PhysicalPlannerContext context) {
    switch (type) {
      case POST_ORDER:
        return new PostOrderRuleExecutor(rule, context);
      case IN_ORDER:
        return new LeftInputFirstRuleExecutor(rule, context);
      default:
        throw new IllegalStateException(String.format("Unrecognized rule executor type: %s", type));
    }
  }

  public enum Type {
    POST_ORDER,
    IN_ORDER
  }
}
