package com.linkedin.pinot.core.plan;

import com.linkedin.pinot.common.response.InstanceResponse;
import com.linkedin.pinot.core.block.aggregation.InstanceResponseBlock;
import com.linkedin.pinot.core.operator.UResultOperator;


/**
 * GlobalPlan for a query applied to all the pruned segments.
 * 
 * @author xiafu
 *
 */
public class GlobalPlanImplV0 extends Plan {

  private InstanceResponsePlanNode _rootNode;
  private InstanceResponse _instanceResponse;

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
  public InstanceResponse execute() {
    PlanNode root = getRoot();
    UResultOperator operator = (UResultOperator) root.run();
    InstanceResponseBlock instanceResponseBlock = (InstanceResponseBlock) operator.nextBlock();
    _instanceResponse = instanceResponseBlock.getInstanceResponse();
    return _instanceResponse;
  }

  @Override
  public InstanceResponse getInstanceResponse() {
    return _instanceResponse;
  }

}
