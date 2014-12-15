package com.linkedin.pinot.core.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.MCombineOperator;


/**
 * CombinePlanNode takes care how to create MCombineOperator.
 * 
 * @author xiafu
 *
 */
public class CombinePlanNode implements PlanNode {

  private List<PlanNode> _planNodeList = new ArrayList<PlanNode>();
  private final BrokerRequest _brokerRequest;
  private final ExecutorService _executorService;
  private final long _timeOutMs;

  public CombinePlanNode(BrokerRequest brokerRequest, ExecutorService executorService, long timeOutMs) {
    _brokerRequest = brokerRequest;
    _executorService = executorService;
    _timeOutMs = timeOutMs;
  }

  public void addPlanNode(PlanNode planNode) {
    _planNodeList.add(planNode);
  }

  public List<PlanNode> getPlanNodeList() {
    return _planNodeList;
  }

  @Override
  public Operator run() {
    List<Operator> retOperators = new ArrayList<Operator>();
    for (PlanNode planNode : _planNodeList) {
      retOperators.add(planNode.run());
    }
    return new MCombineOperator(retOperators, _executorService, _timeOutMs, _brokerRequest);
  }

  @Override
  public void showTree(String prefix) {
    System.out.println(prefix + "Combine Plan Node :");
    System.out.println(prefix + "Operator: MCombineOperator");
    System.out.println(prefix + "Argument 0: BrokerRequest - " + _brokerRequest);
    System.out.println(prefix + "Argument 1: isParallel - " + ((_executorService == null) ? false : true));
    int i = 2;
    for (PlanNode planNode : _planNodeList) {
      System.out.println(prefix + "Argument " + (i++) + ":");
      planNode.showTree(prefix + "    ");
    }
  }

}
