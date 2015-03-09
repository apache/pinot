/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

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
  private static final Logger _logger = Logger.getLogger("QueryPlanLog");
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
    _logger.debug(prefix + "Combine Plan Node :");
    _logger.debug(prefix + "Operator: MCombineOperator");
    _logger.debug(prefix + "Argument 0: BrokerRequest - " + _brokerRequest);
    _logger.debug(prefix + "Argument 1: isParallel - " + ((_executorService == null) ? false : true));
    int i = 2;
    for (PlanNode planNode : _planNodeList) {
      _logger.debug(prefix + "Argument " + (i++) + ":");
      planNode.showTree(prefix + "    ");
    }
  }

}
