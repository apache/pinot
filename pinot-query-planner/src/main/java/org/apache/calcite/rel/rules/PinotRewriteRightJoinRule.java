package org.apache.calcite.rel.rules;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.tools.RelBuilderFactory;


public class PinotRewriteRightJoinRule extends RelOptRule {
 public static final PinotRewriteRightJoinRule INSTANCE =
      new PinotRewriteRightJoinRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotRewriteRightJoinRule(RelBuilderFactory factory) {
    super(operand(LogicalJoin.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1) {
      return false;
    }
    if (call.rel(0) instanceof Join) {
      Join join = call.rel(0);
      return join.getJoinType() == JoinRelType.RIGHT;
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    RelNode leftInput = join.getInput(0);
    RelNode rightInput = join.getInput(1);

    RelNode newJoinNode =
        new LogicalJoin(join.getCluster(), join.getTraitSet(), rightInput, leftInput, join.getCondition(),
            join.getVariablesSet(), JoinRelType.LEFT, join.isSemiJoinDone(),
            ImmutableList.copyOf(join.getSystemFieldList()));

    call.transformTo(newJoinNode);
  }
}
