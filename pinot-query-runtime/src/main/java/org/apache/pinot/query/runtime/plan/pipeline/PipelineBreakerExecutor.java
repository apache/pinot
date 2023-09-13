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
   * @param contextMetadata request metadata, including query options
   * @param requestId request ID
   * @param deadlineMs execution deadline
   * @return pipeline breaker result;
   *   - If exception occurs, exception block will be wrapped in {@link TransferableBlock} and assigned to each PB node.
   *   - Normal stats will be attached to each PB node and downstream execution should return with stats attached.
   */
  @Nullable
  public static PipelineBreakerResult executePipelineBreakers(OpChainSchedulerService scheduler,
      MailboxService mailboxService, DistributedStagePlan distributedStagePlan, Map<String, String> contextMetadata,
      long requestId, long deadlineMs) {
    PipelineBreakerContext pipelineBreakerContext = new PipelineBreakerContext();
    PipelineBreakerVisitor.visitPlanRoot(distributedStagePlan.getStageRoot(), pipelineBreakerContext);
    if (!pipelineBreakerContext.getPipelineBreakerMap().isEmpty()) {
      try {
        // TODO: This PlanRequestContext needs to indicate it is a pre-stage opChain and only listens to pre-stage
        //     OpChain receive-mail callbacks.
        // see also: MailboxIdUtils TODOs, de-couple mailbox id from query information
        OpChainExecutionContext opChainExecutionContext =
            new OpChainExecutionContext(mailboxService, requestId, distributedStagePlan.getStageId(),
                distributedStagePlan.getServer(), deadlineMs, contextMetadata, distributedStagePlan.getStageMetadata(),
                null);
        return execute(scheduler, pipelineBreakerContext, opChainExecutionContext);
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

  private static PipelineBreakerResult execute(OpChainSchedulerService scheduler,
      PipelineBreakerContext pipelineBreakerContext, OpChainExecutionContext opChainExecutionContext)
      throws Exception {
    Map<Integer, Operator<TransferableBlock>> pipelineWorkerMap = new HashMap<>();
    for (Map.Entry<Integer, PlanNode> e : pipelineBreakerContext.getPipelineBreakerMap().entrySet()) {
      int key = e.getKey();
      PlanNode planNode = e.getValue();
      if (!(planNode instanceof MailboxReceiveNode)) {
        throw new UnsupportedOperationException("Only MailboxReceiveNode is supported to run as pipeline breaker now");
      }
      OpChain opChain = PhysicalPlanVisitor.walkPlanNode(planNode, opChainExecutionContext);
      pipelineWorkerMap.put(key, opChain.getRoot());
    }
    return runMailboxReceivePipelineBreaker(scheduler, pipelineBreakerContext, pipelineWorkerMap,
        opChainExecutionContext);
  }

  private static PipelineBreakerResult runMailboxReceivePipelineBreaker(OpChainSchedulerService scheduler,
      PipelineBreakerContext pipelineBreakerContext, Map<Integer, Operator<TransferableBlock>> pipelineWorkerMap,
      OpChainExecutionContext opChainExecutionContext)
      throws Exception {
    PipelineBreakerOperator pipelineBreakerOperator =
        new PipelineBreakerOperator(opChainExecutionContext, pipelineWorkerMap);
    CountDownLatch latch = new CountDownLatch(1);
    OpChain pipelineBreakerOpChain =
        new OpChain(opChainExecutionContext, pipelineBreakerOperator, (id) -> latch.countDown());
    scheduler.register(pipelineBreakerOpChain);
    long timeoutMs = opChainExecutionContext.getDeadlineMs() - System.currentTimeMillis();
    if (latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
      return new PipelineBreakerResult(pipelineBreakerContext.getNodeIdMap(), pipelineBreakerOperator.getResultMap(),
          pipelineBreakerOperator.getErrorBlock(), pipelineBreakerOpChain.getStats());
    } else {
      throw new TimeoutException(
          String.format("Timed out waiting for pipeline breaker results after: %dms", timeoutMs));
    }
  }
}
