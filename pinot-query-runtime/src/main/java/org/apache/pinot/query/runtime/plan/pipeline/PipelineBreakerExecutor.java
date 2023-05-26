/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.query.runtime.plan.pipeline;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.executor.OpChainSchedulerService;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.plan.PhysicalPlanContext;
import org.apache.pinot.query.runtime.plan.PhysicalPlanVisitor;


/**
 * Utility class to run pipeline breaker execution and collects the results.
 */
public class PipelineBreakerExecutor {
  private PipelineBreakerExecutor() {
    // do not instantiate.
  }

  /**
   * Execute a pipeline breaker and collect the results (synchronously)
   *
   * Currently, pipeline breaker executor can only execute mailbox receive pipeline breaker.
   */
  public static Map<Integer, List<TransferableBlock>> execute(OpChainSchedulerService scheduler,
      PipelineBreakerContext context, PhysicalPlanContext physicalPlanContext)
      throws Exception {
    Map<Integer, Operator<TransferableBlock>> pipelineWorkerMap = new HashMap<>();
    for (Map.Entry<Integer, PlanNode> e : context.getPipelineBreakerMap().entrySet()) {
      int key = e.getKey();
      PlanNode planNode = e.getValue();
      // TODO: supprot other pipeline breaker node type as well.
      if (!(planNode instanceof MailboxReceiveNode)) {
        throw new UnsupportedOperationException("Only MailboxReceiveNode is supported to run as pipeline breaker now");
      }
      OpChain tempOpChain = PhysicalPlanVisitor.walkPlanNode(planNode, physicalPlanContext);
      pipelineWorkerMap.put(key, tempOpChain.getRoot());
    }
    return runMailboxReceivePipelineBreaker(scheduler, pipelineWorkerMap, physicalPlanContext);
  }

  private static Map<Integer, List<TransferableBlock>> runMailboxReceivePipelineBreaker(
      OpChainSchedulerService scheduler, Map<Integer, Operator<TransferableBlock>> pipelineWorkerMap,
      PhysicalPlanContext physicalPlanContext)
      throws Exception {
    PipelineBreakerOperator pipelineBreakerOperator = new PipelineBreakerOperator(
        physicalPlanContext.getOpChainExecutionContext(), pipelineWorkerMap);
    OpChain pipelineBreakerOpChain = new OpChain(physicalPlanContext.getOpChainExecutionContext(),
        pipelineBreakerOperator,
        physicalPlanContext.getReceivingMailboxIds());
    scheduler.register(pipelineBreakerOpChain);
    return pipelineBreakerOperator.getResult();
  }
}
