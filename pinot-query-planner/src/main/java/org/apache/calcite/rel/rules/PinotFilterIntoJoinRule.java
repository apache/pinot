package org.apache.calcite.rel.rules;

import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.sql.SqlKind;


public class PinotFilterIntoJoinRule extends FilterJoinRule.FilterIntoJoinRule {
  public static final PinotFilterIntoJoinRule INSTANCE = new PinotFilterIntoJoinRule();
  protected PinotFilterIntoJoinRule() {
    super(ImmutableFilterIntoJoinRuleConfig.of((join, joinType, exp) ->
            exp.getKind() == SqlKind.AND || exp.getKind() == SqlKind.EQUALS)
        .withOperandSupplier(b0 ->
            b0.operand(Filter.class).oneInput(b1 ->
                b1.operand(Join.class).anyInputs()))
        .withSmart(true));
  }
}
