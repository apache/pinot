package org.apache.pinot.query.rules;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.tools.RelBuilderFactory;


/**
 * Special rule for Pinot, always insert exchange after JOIN
 */
public class PinotExchangeNodeInsertRule extends RelOptRule {
  public static final PinotExchangeNodeInsertRule INSTANCE =
      new PinotExchangeNodeInsertRule(RelFactories.LOGICAL_BUILDER);

  public PinotExchangeNodeInsertRule(RelBuilderFactory factory) {
    super(operand(LogicalJoin.class, any()), factory, null);
  }

  @Override public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1) {
      return false;
    }
    if (call.rel(0) instanceof Join) {
      Join join = call.rel(0);
      return !isExchange(join.getLeft()) && !isExchange(join.getRight());
    }
    return false;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    RelNode leftInput = join.getInput(0);
    RelNode rightInput = join.getInput(1);

    RelNode leftExchange = LogicalExchange.create(leftInput, RelDistributions.SINGLETON);
    RelNode rightExchange = LogicalExchange.create(rightInput, RelDistributions.BROADCAST_DISTRIBUTED);

    RelNode newJoinNode = new LogicalJoin(join.getCluster(), join.getTraitSet(), leftExchange, rightExchange,
        join.getCondition(), join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone(),
        ImmutableList.copyOf(join.getSystemFieldList()));

    call.transformTo(newJoinNode);
  }

  private static boolean isExchange(RelNode rel) {
    RelNode reference = rel;
    if (reference instanceof HepRelVertex) {
      reference = ((HepRelVertex) reference).getCurrentRel();
    }
    return reference instanceof Exchange;
  }
}
