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
import org.apache.pinot.query.runtime.plan.PhysicalPlanVisitor;
import org.apache.pinot.query.runtime.plan.PlanRequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class to run pipeline breaker execution and collects the results.
 */
public class PipelineBreakerExecutionUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipelineBreakerExecutionUtils.class);

  private PipelineBreakerExecutionUtils() {
    // do not instantiate.
  }

  public static Map<Integer, List<TransferableBlock>> execute(OpChainSchedulerService scheduler,
      PipelineBreakerContext context, PlanRequestContext planRequestContext) {
    Map<Integer, Operator<TransferableBlock>> pipelineWorkerMap = new HashMap<>();
    for (Map.Entry<Integer, PlanNode> e : context.getPipelineBreakerMap().entrySet()) {
      int key = e.getKey();
      PlanNode planNode = e.getValue();
      if (!(planNode instanceof MailboxReceiveNode)) {
        throw new UnsupportedOperationException("Only MailboxReceiveNode is supported to run as pipeline breaker now");
      }
      OpChain tempOpChain = PhysicalPlanVisitor.walkPlanNode(planNode, planRequestContext);
      pipelineWorkerMap.put(key, tempOpChain.getRoot());
    }
    return runMailboxReceivePipelineBreaker(scheduler, pipelineWorkerMap, planRequestContext);
  }

  private static Map<Integer, List<TransferableBlock>> runMailboxReceivePipelineBreaker(
      OpChainSchedulerService scheduler, Map<Integer, Operator<TransferableBlock>> pipelineWorkerMap,
      PlanRequestContext planRequestContext) {
    PipelineBreakOperator pipelineBreakOperator = new PipelineBreakOperator(
        planRequestContext.getOpChainExecutionContext(), pipelineWorkerMap);
    OpChain pipelineBreakerOpChain = new OpChain(planRequestContext.getOpChainExecutionContext(), pipelineBreakOperator,
        planRequestContext.getReceivingMailboxIds());
    scheduler.register(pipelineBreakerOpChain);
    try {
      return pipelineBreakOperator.getResult();
    } catch (Exception e) {
      LOGGER.error("Error executing pipeline breaker.", e);
      throw new RuntimeException("Error executing pipeline breaker.", e);
    }
  }
}
