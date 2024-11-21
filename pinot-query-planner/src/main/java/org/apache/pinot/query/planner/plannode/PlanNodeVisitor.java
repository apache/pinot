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

  /**
   * A depth-first visitor that visits all children of a node before visiting the node itself.
   *
   * The default implementation for each plan node type does nothing but visiting its inputs
   * (see {@link #visitChildren(PlanNode, Object)}) and then returning the result of calling
   * {@link #postChildren(PlanNode, Object)}.
   *
   * Subclasses can override each method to provide custom behavior for each plan node type.
   * For example:
   *
   * <pre>
   *   public ResultClass visitMailboxSend(MailboxSendNode node, ContextClass context) {
   *     somethingToDoBeforeChildren(node);
   *     visitChildren(node, context);
   *     return somethingToDoAfterChildren(node);
   *   }
   * </pre>
   *
   * It is not mandatory to override all methods nor to call {@link #visitChildren(PlanNode, Object)} when
   * overriding a visit method.
   *
   * Notice that {@link MailboxReceiveNode} nodes do not have inputs. Instead, they may store the sender node in a
   * different field. Whether to visit the sender node when visiting a {@link MailboxReceiveNode} is controlled by
   * {@link #traverseStageBoundary()}.
   *
   * @param <T>
   * @param <C>
   */
  abstract class DepthFirstVisitor<T, C> implements PlanNodeVisitor<T, C> {

    /**
     * Visits all children of a node.
     *
     * Notice that {@link MailboxReceiveNode} nodes do not have inputs and therefore this method is a no-op for them.
     * The default {@link #visitMailboxReceive(MailboxReceiveNode, Object)} implementation will visit the sender node
     * if {@link #traverseStageBoundary()} returns true, but if it is overridden, it is up to the implementor to decide
     * whether to visit the sender node or not.
     */
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

    /**
     * The method that is called by default to handle a node that does not have a specific visit method.
     *
     * This method can be overridden to provide a default behavior for all nodes.
     *
     * The returned value of this method is ignored by default
     */
    protected T preChildren(PlanNode node, C context) {
      return null;
    }

    /**
     * The method that is called by default to handle a node that does not have a specific visit method.
     *
     * This method can be overridden to provide a default behavior for all nodes.
     *
     * The returned value of this method is what each default visit method will return.
     */
    protected T postChildren(PlanNode node, C context) {
      return null;
    }

    @Override
    public T visitAggregate(AggregateNode node, C context) {
      preChildren(node, context);
      visitChildren(node, context);
      return postChildren(node, context);
    }

    @Override
    public T visitFilter(FilterNode node, C context) {
      preChildren(node, context);
      visitChildren(node, context);
      return postChildren(node, context);
    }

    @Override
    public T visitJoin(JoinNode node, C context) {
      preChildren(node, context);
      visitChildren(node, context);
      return postChildren(node, context);
    }

    @Override
    public T visitMailboxReceive(MailboxReceiveNode node, C context) {
      preChildren(node, context);
      visitChildren(node, context);
      if (traverseStageBoundary()) {
        node.getSender().visit(this, context);
      }
      return postChildren(node, context);
    }

    @Override
    public T visitMailboxSend(MailboxSendNode node, C context) {
      preChildren(node, context);
      visitChildren(node, context);
      return postChildren(node, context);
    }

    @Override
    public T visitProject(ProjectNode node, C context) {
      preChildren(node, context);
      visitChildren(node, context);
      return postChildren(node, context);
    }

    @Override
    public T visitSort(SortNode node, C context) {
      preChildren(node, context);
      visitChildren(node, context);
      return postChildren(node, context);
    }

    @Override
    public T visitTableScan(TableScanNode node, C context) {
      preChildren(node, context);
      visitChildren(node, context);
      return postChildren(node, context);
    }

    @Override
    public T visitValue(ValueNode node, C context) {
      preChildren(node, context);
      visitChildren(node, context);
      return postChildren(node, context);
    }

    @Override
    public T visitWindow(WindowNode node, C context) {
      preChildren(node, context);
      visitChildren(node, context);
      return postChildren(node, context);
    }

    @Override
    public T visitSetOp(SetOpNode node, C context) {
      preChildren(node, context);
      visitChildren(node, context);
      return postChildren(node, context);
    }

    @Override
    public T visitExchange(ExchangeNode node, C context) {
      preChildren(node, context);
      visitChildren(node, context);
      return postChildren(node, context);
    }

    @Override
    public T visitExplained(ExplainedNode node, C context) {
      preChildren(node, context);
      visitChildren(node, context);
      return postChildren(node, context);
    }
  }
}
