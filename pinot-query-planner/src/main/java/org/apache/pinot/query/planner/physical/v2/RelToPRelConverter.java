package org.apache.pinot.query.planner.physical.v2;

import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RelToPRelConverter {
  public static final RelToPRelConverter INSTANCE = new RelToPRelConverter();
  private static final Logger LOGGER = LoggerFactory.getLogger(RelToPRelConverter.class);

  private RelToPRelConverter() {
  }

  public PRelNode toPRelNode(RelNode relNode, PhysicalPlannerContext context, Map<String, String> queryOptions) {
    PRelNode pRelNode = PRelNode.wrapRelTree(relNode, context.getNodeIdGenerator());
    var rules = PhysicalOptRuleSet.create(context, queryOptions);
    for (var ruleAndExecutor : rules) {
      PRelOptRule rule = ruleAndExecutor.getLeft();
      RuleExecutor executor = ruleAndExecutor.getRight();
      pRelNode = executor.execute(pRelNode, rule, context);
    }
    PRelNode.printWrappedRelNode(pRelNode, 0);
    return pRelNode;
  }
}
