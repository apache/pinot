package org.apache.calcite.rel.rules;

import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Sarg;


public class PinotFilterExpandSearchRule extends RelOptRule {
  public static final PinotFilterExpandSearchRule INSTANCE =
      new PinotFilterExpandSearchRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotFilterExpandSearchRule(RelBuilderFactory factory) {
    super(operand(LogicalFilter.class, any()), factory, null);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1) {
      return false;
    }
    if (call.rel(0) instanceof Filter) {
      Filter filter = call.rel(0);
      if (filter.getCondition().getKind() == SqlKind.SEARCH) {
        RexCall rexCall = (RexCall) filter.getCondition();
        List<RexNode> operands = rexCall.getOperands();
        RexLiteral rexLiteral = (RexLiteral) operands.get(1);
        Sarg sarg = rexLiteral.getValueAs(Sarg.class);
        return sarg != null && !sarg.isPoints() && !sarg.isComplementedPoints();
      }
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Filter filter = call.rel(0);
    RexNode newCondition = RexUtil.expandSearch(filter.getCluster().getRexBuilder(), null, filter.getCondition());
    call.transformTo(LogicalFilter.create(filter.getInput(), newCondition));
  }
}
