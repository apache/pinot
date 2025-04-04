package org.apache.pinot.query.planner.physical.v2.opt.rules;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.opt.PRelOptRule;
import org.apache.pinot.query.planner.physical.v2.opt.PRelOptRuleCall;


/**
 * The leaf stage consists of a table-scan and an optional project and/or filter. The filter and project nodes
 * may be in any order. We don't include sort or aggregate in the leaf stage in this rule, because they will made part
 * of Leaf stage (if appropriate) as part of the Aggregate and Sort pushdown rules.
 * The idea is that you can and should always make filter and project part of the leaf stage and compute them locally
 * on the server where the table-scan is computed. Whether it makes sense to run the aggregate or sort in the leaf
 * stage depends on quite a few conditions, and it may not always be optimal to do so.
 */
public class LeafStageBoundaryRule extends PRelOptRule {
  public static final LeafStageBoundaryRule INSTANCE = new LeafStageBoundaryRule();

  private LeafStageBoundaryRule() {
  }

  @Override
  public boolean matches(PRelOptRuleCall call) {
    RelNode currentRel = call._currentNode.unwrap();
    if (currentRel instanceof TableScan) {
      return true;
    }
    if (!(currentRel instanceof Project) && !(currentRel instanceof Filter)) {
      return false;
    }
    if (currentRel.getInput(0) instanceof TableScan) {
      return true;
    }
    return currentRel.getInput(0).getInput(0) instanceof TableScan;
  }

  @Override
  public PRelNode onMatch(PRelOptRuleCall call) {
    PRelNode currentNode = call._currentNode;
    return currentNode.asLeafStage();
  }
}
