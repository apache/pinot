package org.apache.pinot.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.calcite.rel.logical.PinotLogicalTableScan;


public class PinotTableScanConverterRule extends RelOptRule {
  public static final PinotTableScanConverterRule INSTANCE =
      new PinotTableScanConverterRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotTableScanConverterRule(RelBuilderFactory factory) {
    super(operand(TableScan.class, none()), factory, null);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    TableScan tableScan = call.rel(0);
    if (tableScan instanceof LogicalTableScan) {
      call.transformTo(PinotLogicalTableScan.create(call.rel(0)));
    } else if (!(tableScan instanceof PinotLogicalTableScan)) {
      throw new IllegalStateException("Unknown table scan in PinotTableScanConverterRule: " + tableScan.getClass());
    }
  }
}
