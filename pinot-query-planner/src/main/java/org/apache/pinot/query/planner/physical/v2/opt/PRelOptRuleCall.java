package org.apache.pinot.query.planner.physical.v2.opt;

import java.util.Deque;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.physical.v2.PRelNode;


public class PRelOptRuleCall {
  public final PhysicalPlannerContext _physicalPlannerContext;
  public final PRelNode _currentNode;
  public final Deque<PRelNode> _parents;

  public PRelOptRuleCall(PRelNode currentNode, Deque<PRelNode> parents, PhysicalPlannerContext physicalPlannerContext) {
    _currentNode = currentNode;
    _parents = parents;
    _physicalPlannerContext = physicalPlannerContext;
  }
}
