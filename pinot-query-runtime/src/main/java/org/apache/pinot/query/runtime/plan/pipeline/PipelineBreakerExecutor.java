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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.executor.OpChainSchedulerService;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.PhysicalPlanContext;
import org.apache.pinot.query.runtime.plan.PhysicalPlanVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class to run pipeline breaker execution and collects the results.
 */
public class PipelineBreakerExecutor {
  private PipelineBreakerExecutor() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(PipelineBreakerExecutor.class);

  /**
   * Execute a pipeline breaker and collect the results (synchronously). Currently, pipeline breaker executor can only
   *    execute mailbox receive pipeline breaker.
   *
   * @param scheduler scheduler service to run the pipeline breaker main thread.
   * @param mailboxService mailbox service to attach the {@link MailboxReceiveNode} against.
   * @param distributedStagePlan the distributed stage plan to run pipeline breaker on.
   * @param deadlineMs execution deadline
   * @param requestId request ID
   * @param isTraceEnabled whether to enable trace.
   * @return pipeline breaker result;
   *   - If exception occurs, exception block will be wrapped in {@link TransferableBlock} and assigned to each PB node.
   *   - Normal stats will be attached to each PB node and downstream execution should return with stats attached.
   */
  @Nullable
  public static PipelineBreakerResult executePipelineBreakers(OpChainSchedulerService scheduler,
      MailboxService mailboxService, DistributedStagePlan distributedStagePlan, long deadlineMs, long requestId,
      boolean isTraceEnabled) {
    PipelineBreakerContext pipelineBreakerContext = new PipelineBreakerContext();
    PipelineBreakerVisitor.visitPlanRoot(distributedStagePlan.getStageRoot(), pipelineBreakerContext);
    if (!pipelineBreakerContext.getPipelineBreakerMap().isEmpty()) {
      try {
        PlanNode stageRoot = distributedStagePlan.getStageRoot();
        // TODO: This PlanRequestContext needs to indicate it is a pre-stage opChain and only listens to pre-stage
        //     OpChain receive-mail callbacks.
        // see also: MailboxIdUtils TODOs, de-couple mailbox id from query information
        OpChainExecutionContext opChainContext =
            new OpChainExecutionContext(mailboxService, requestId, stageRoot.getPlanFragmentId(),
                distributedStagePlan.getServer(), deadlineMs, distributedStagePlan.getStageMetadata(), null,
                isTraceEnabled);
        PhysicalPlanContext physicalPlanContext = new PhysicalPlanContext(opChainContext, null);
        return PipelineBreakerExecutor.execute(scheduler, pipelineBreakerContext, physicalPlanContext);
      } catch (Exception e) {
        LOGGER.error("Caught exception executing pipeline breaker for request: {}, stage: {}", requestId,
            distributedStagePlan.getStageId(), e);
        return new PipelineBreakerResult(pipelineBreakerContext.getNodeIdMap(), Collections.emptyMap(),
            TransferableBlockUtils.getErrorTransferableBlock(e), null);
      }
    } else {
      return null;
    }
  }

  private static PipelineBreakerResult execute(OpChainSchedulerService scheduler, PipelineBreakerContext context,
      PhysicalPlanContext physicalPlanContext)
      throws Exception {
    Map<Integer, Operator<TransferableBlock>> pipelineWorkerMap = new HashMap<>();
    for (Map.Entry<Integer, PlanNode> e : context.getPipelineBreakerMap().entrySet()) {
      int key = e.getKey();
      PlanNode planNode = e.getValue();
      if (!(planNode instanceof MailboxReceiveNode)) {
        throw new UnsupportedOperationException("Only MailboxReceiveNode is supported to run as pipeline breaker now");
      }
      OpChain tempOpChain = PhysicalPlanVisitor.walkPlanNode(planNode, physicalPlanContext);
      pipelineWorkerMap.put(key, tempOpChain.getRoot());
    }
    return runMailboxReceivePipelineBreaker(scheduler, context, pipelineWorkerMap, physicalPlanContext);
  }

  private static PipelineBreakerResult runMailboxReceivePipelineBreaker(OpChainSchedulerService scheduler,
      PipelineBreakerContext context, Map<Integer, Operator<TransferableBlock>> pipelineWorkerMap,
      PhysicalPlanContext physicalPlanContext)
      throws Exception {
    PipelineBreakerOperator pipelineBreakerOperator =
        new PipelineBreakerOperator(physicalPlanContext.getOpChainExecutionContext(), pipelineWorkerMap);
    CountDownLatch latch = new CountDownLatch(1);
    OpChain pipelineBreakerOpChain =
        new OpChain(physicalPlanContext.getOpChainExecutionContext(), pipelineBreakerOperator,
            physicalPlanContext.getReceivingMailboxIds(), (id) -> latch.countDown());
    scheduler.register(pipelineBreakerOpChain);
    long timeoutMs = physicalPlanContext.getDeadlineMs() - System.currentTimeMillis();
    if (latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
      return new PipelineBreakerResult(context.getNodeIdMap(), pipelineBreakerOperator.getResultMap(),
          pipelineBreakerOperator.getErrorBlock(), pipelineBreakerOpChain.getStats());
    } else {
      throw new TimeoutException(
          String.format("Timed out waiting for pipeline breaker results after: %dms", timeoutMs));
    }
  }
}
