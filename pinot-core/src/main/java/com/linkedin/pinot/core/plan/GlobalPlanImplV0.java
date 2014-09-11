package com.linkedin.pinot.core.plan;

import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.block.query.InstanceResponseBlock;
import com.linkedin.pinot.core.operator.UResultOperator;


/**
 * GlobalPlan for a query applied to all the pruned segments.
 * 
 * @author xiafu
 *
 */
public class GlobalPlanImplV0 extends Plan {

  private InstanceResponsePlanNode _rootNode;
  private DataTable _instanceResponseDataTable;

  public GlobalPlanImplV0(InstanceResponsePlanNode rootNode) {
    _rootNode = rootNode;
  }

  @Override
  public void print() {
    _rootNode.showTree("");
  }

  @Override
  public PlanNode getRoot() {
    return _rootNode;
  }

  @Override
  public void execute() {
    PlanNode root = getRoot();
    UResultOperator operator = (UResultOperator) root.run();
    InstanceResponseBlock instanceResponseBlock = (InstanceResponseBlock) operator.nextBlock();
    _instanceResponseDataTable = instanceResponseBlock.getInstanceResponseDataTable();
  }

  @Override
  public DataTable getInstanceResponse() {
    try {
      return _instanceResponseDataTable;
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

}
