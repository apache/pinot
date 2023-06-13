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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.executor.OpChainSchedulerService;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.runtime.plan.PhysicalPlanContext;
import org.apache.pinot.query.runtime.plan.PhysicalPlanVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class to run pipeline breaker execution and collects the results.
 */
public class PipelineBreakerExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipelineBreakerExecutor.class);
  private PipelineBreakerExecutor() {
    // do not instantiate.
  }

  /**
   * Execute a pipeline breaker and collect the results (synchronously)
   *
   * Currently, pipeline breaker executor can only execute mailbox receive pipeline breaker.
   */
  public static PipelineBreakerResult executePipelineBreakers(OpChainSchedulerService scheduler,
      MailboxService mailboxService, DistributedStagePlan distributedStagePlan, long timeoutMs, long deadlineMs,
      long requestId, boolean isTraceEnabled)
      throws Exception {
    PipelineBreakerContext pipelineBreakerContext = new PipelineBreakerContext(
        DistributedStagePlan.isLeafStage(distributedStagePlan));
    PipelineBreakerVisitor.visitPlanRoot(distributedStagePlan.getStageRoot(), pipelineBreakerContext);
    if (pipelineBreakerContext.getPipelineBreakerMap().size() > 0) {
      PlanNode stageRoot = distributedStagePlan.getStageRoot();
      // TODO: This PlanRequestContext needs to indicate it is a pre-stage opChain and only listens to pre-stage OpChain
      //     receive-mail callbacks.
      // see also: MailboxIdUtils TODOs, de-couple mailbox id from query information
      PhysicalPlanContext physicalPlanContext =
          new PhysicalPlanContext(mailboxService, requestId, stageRoot.getPlanFragmentId(), timeoutMs, deadlineMs,
              distributedStagePlan.getServer(), distributedStagePlan.getStageMetadata(), null, isTraceEnabled);
      Map<Integer, List<TransferableBlock>> resultMap =
          PipelineBreakerExecutor.execute(scheduler, pipelineBreakerContext, physicalPlanContext);
      return new PipelineBreakerResult(pipelineBreakerContext.getNodeIdMap(), resultMap);
    } else {
      return null;
    }
  }

  private static Map<Integer, List<TransferableBlock>> execute(OpChainSchedulerService scheduler,
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
    CountDownLatch latch = new CountDownLatch(1);
    OpChain pipelineBreakerOpChain = new OpChain(physicalPlanContext.getOpChainExecutionContext(),
        pipelineBreakerOperator, physicalPlanContext.getReceivingMailboxIds(), (id) -> latch.countDown());
    scheduler.register(pipelineBreakerOpChain);
    long timeoutMs = physicalPlanContext.getDeadlineMs() - System.currentTimeMillis();
    if (latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
      return pipelineBreakerOperator.getResultMap();
    } else {
      throw new IOException("Exception occur when awaiting breaker results!");
    }
  }
}
