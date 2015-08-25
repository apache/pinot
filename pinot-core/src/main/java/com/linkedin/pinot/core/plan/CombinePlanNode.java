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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.linkedin.pinot.core.trace.TraceRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.MCombineOperator;


/**
 * CombinePlanNode takes care how to create MCombineOperator.
 *
 *
 */
public class CombinePlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(CombinePlanNode.class);
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
    long start = System.currentTimeMillis();
    final List<Operator> retOperators = new ArrayList<Operator>(_planNodeList.size());
    if (_planNodeList.size() < 10) {
      for (PlanNode planNode : _planNodeList) {
        retOperators.add(planNode.run());
      }
    } else {
      final CountDownLatch latch = new CountDownLatch(_planNodeList.size());
      final ConcurrentLinkedQueue<Operator> queue = new ConcurrentLinkedQueue<Operator>();
      for (final PlanNode planNode : _planNodeList) {
        _executorService.execute(new TraceRunnable() {
          @Override
          public void runJob() {
            try {
              Operator operator = planNode.run();
              queue.add(operator);
            } catch (Exception e) {
              LOGGER.error("Getting exception when trying to run a planNode", e);
            } finally {
              latch.countDown();
            }
          }
        });
      }
      try {
        latch.await(60, TimeUnit.SECONDS);
        retOperators.addAll(queue);
      } catch (InterruptedException e) {
        LOGGER.error("Interupted exception. Planning each segment took more than 60 seconds: ", e);
        throw new RuntimeException(QueryException.COMBINE_SEGMENT_PLAN_TIMEOUT_ERROR);
      }
    }
    MCombineOperator mCombineOperator =
        new MCombineOperator(retOperators, _executorService, _timeOutMs, _brokerRequest);
    long end = System.currentTimeMillis();
    LOGGER.info("CombinePlanNode.run took: " + (end - start));
    return mCombineOperator;
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Combine Plan Node :");
    LOGGER.debug(prefix + "Operator: MCombineOperator");
    LOGGER.debug(prefix + "Argument 0: BrokerRequest - " + _brokerRequest);
    LOGGER.debug(prefix + "Argument 1: isParallel - " + ((_executorService == null) ? false : true));
    int i = 2;
    for (PlanNode planNode : _planNodeList) {
      LOGGER.debug(prefix + "Argument " + (i++) + ":");
      planNode.showTree(prefix + "    ");
    }
  }

}
