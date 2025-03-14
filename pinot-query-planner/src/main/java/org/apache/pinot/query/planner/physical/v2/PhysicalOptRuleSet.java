package org.apache.pinot.query.planner.physical.v2;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.physical.v2.rules.AggregatePushdownRule;
import org.apache.pinot.query.planner.physical.v2.rules.LeafStageBoundaryRule;
import org.apache.pinot.query.planner.physical.v2.rules.LeafStageWorkerAssignmentRule;
import org.apache.pinot.query.planner.physical.v2.rules.SortPushdownRule;
import org.apache.pinot.query.planner.physical.v2.rules.WorkerExchangeAssignmentRule;


public class PhysicalOptRuleSet {
  private PhysicalOptRuleSet() {
  }

  public static List<Pair<PRelOptRule, RuleExecutor>> create(PhysicalPlannerContext context,
      Map<String, String> queryOptions) {
    return List.of(
        Pair.of(LeafStageBoundaryRule.INSTANCE, RuleExecutors.POST_ORDER),
        Pair.of(new LeafStageWorkerAssignmentRule(context), RuleExecutors.POST_ORDER),
        Pair.of(new WorkerExchangeAssignmentRule(context), RuleExecutors.IN_ORDER),
        Pair.of(AggregatePushdownRule.INSTANCE, RuleExecutors.POST_ORDER),
        Pair.of(SortPushdownRule.INSTANCE, RuleExecutors.POST_ORDER));
  }
}
