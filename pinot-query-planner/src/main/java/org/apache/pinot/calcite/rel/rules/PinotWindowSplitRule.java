package org.apache.pinot.calcite.rel.rules;

import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.tools.RelBuilder;

public class PinotWindowSplitRule extends RelOptRule{
    public static final PinotWindowSplitRule INSTANCE =
            new PinotWindowSplitRule(PinotRuleUtils.PINOT_REL_FACTORY);

    protected PinotWindowSplitRule(RelBuilderFactory factory) {
        super(operand(Window.class, any()), factory, null);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Window window = call.rel(0);
        return !window.groups.isEmpty();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Window originalWindow = call.rel(0);
        RelNode newInput = originalWindow.getInput();
        for (Window.Group group : originalWindow.groups) {
            // Each window node wraps the previous one
            newInput = LogicalWindow.create(originalWindow.getTraitSet(), newInput, originalWindow.getConstants(), originalWindow.getRowType(), List.of(group));
        }

        RelBuilder relBuilder = call.builder();
        RelNode projected = relBuilder.push(newInput)
                .project(relBuilder.fields())
                .build();
        call.transformTo(projected);
    }
}
