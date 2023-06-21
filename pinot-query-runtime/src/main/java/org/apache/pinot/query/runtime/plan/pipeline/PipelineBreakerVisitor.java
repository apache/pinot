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

import org.apache.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.query.planner.plannode.DefaultPostOrderTraversalVisitor;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;


class PipelineBreakerVisitor extends DefaultPostOrderTraversalVisitor<Void, PipelineBreakerContext> {
  private static final PlanNodeVisitor<Void, PipelineBreakerContext> INSTANCE = new PipelineBreakerVisitor();

  public static void visitPlanRoot(PlanNode root, PipelineBreakerContext context) {
    root.visit(PipelineBreakerVisitor.INSTANCE, context);
  }

  @Override
  public Void process(PlanNode planNode, PipelineBreakerContext context) {
    context.visitedNewPlanNode(planNode);
    return null;
  }

  @Override
  public Void visitMailboxReceive(MailboxReceiveNode node, PipelineBreakerContext context) {
    process(node, context);
    if (node.getExchangeType() == PinotRelExchangeType.PIPELINE_BREAKER) {
      context.addPipelineBreaker(node);
    }
    return null;
  }
}
