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
package org.apache.pinot.query.planner.plannode;

import org.apache.pinot.query.planner.explain.PhysicalExplainPlanVisitor;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;


/**
 * {@code PlanNodeVisitor} is a skeleton class that allows for implementations of {@code PlanNode}
 * tree traversals using the {@link PlanNode#visit(PlanNodeVisitor, Object)} method. There is no
 * enforced traversal order, and should be implemented by subclasses.
 *
 * <p>It is recommended that implementors use private constructors and static methods to access main
 * functionality (see {@link PhysicalExplainPlanVisitor#explain(DispatchableSubPlan)}
 * as an example of a usage of this pattern.
 *
 * @param <T> the return type for all visitsPlanNodeVisitor
 * @param <C> a Context that will be passed as the second parameter to {@code PlanNode#visit},
 *            implementors can decide how they want to use this context (e.g. whether or not
 *            it can be modified in place or whether it's an immutable context)
 */
public interface PlanNodeVisitor<T, C> {

  T visitAggregate(AggregateNode node, C context);

  T visitFilter(FilterNode node, C context);

  T visitJoin(JoinNode node, C context);

  T visitMailboxReceive(MailboxReceiveNode node, C context);

  T visitMailboxSend(MailboxSendNode node, C context);

  T visitProject(ProjectNode node, C context);

  T visitSort(SortNode node, C context);

  T visitTableScan(TableScanNode node, C context);

  T visitValue(ValueNode node, C context);

  T visitWindow(WindowNode node, C context);

  T visitSetOp(SetOpNode node, C context);

  T visitExchange(ExchangeNode node, C context);

  T visitExplained(ExplainedNode node, C context);

  abstract class DepthFirstVisitor<T, C> implements PlanNodeVisitor<T, C> {

    protected void visitChildren(PlanNode node, C context) {
      for (PlanNode input : node.getInputs()) {
        input.visit(this, context);
      }
    }

    /**
     * Whether to visit the sender node (going to another stage) when visiting a {@link MailboxReceiveNode}.
     *
     * Defaults to true.
     */
    protected boolean traverseStageBoundary() {
      return true;
    }

    protected abstract T defaultCase(PlanNode node, C context);

    @Override
    public T visitAggregate(AggregateNode node, C context) {
      visitChildren(node, context);
      return defaultCase(node, context);
    }

    @Override
    public T visitFilter(FilterNode node, C context) {
      visitChildren(node, context);
      return defaultCase(node, context);
    }

    @Override
    public T visitJoin(JoinNode node, C context) {
      visitChildren(node, context);
      return defaultCase(node, context);
    }

    @Override
    public T visitMailboxReceive(MailboxReceiveNode node, C context) {
      visitChildren(node, context);
      if (traverseStageBoundary()) {
        node.getSender().visit(this, context);
      }
      return defaultCase(node, context);
    }

    @Override
    public T visitMailboxSend(MailboxSendNode node, C context) {
      visitChildren(node, context);
      return defaultCase(node, context);
    }

    @Override
    public T visitProject(ProjectNode node, C context) {
      visitChildren(node, context);
      return defaultCase(node, context);
    }

    @Override
    public T visitSort(SortNode node, C context) {
      visitChildren(node, context);
      return defaultCase(node, context);
    }

    @Override
    public T visitTableScan(TableScanNode node, C context) {
      visitChildren(node, context);
      return defaultCase(node, context);
    }

    @Override
    public T visitValue(ValueNode node, C context) {
      visitChildren(node, context);
      return defaultCase(node, context);
    }

    @Override
    public T visitWindow(WindowNode node, C context) {
      visitChildren(node, context);
      return defaultCase(node, context);
    }

    @Override
    public T visitSetOp(SetOpNode node, C context) {
      visitChildren(node, context);
      return defaultCase(node, context);
    }

    @Override
    public T visitExchange(ExchangeNode node, C context) {
      visitChildren(node, context);
      return defaultCase(node, context);
    }

    @Override
    public T visitExplained(ExplainedNode node, C context) {
      visitChildren(node, context);
      return defaultCase(node, context);
    }
  }
}
