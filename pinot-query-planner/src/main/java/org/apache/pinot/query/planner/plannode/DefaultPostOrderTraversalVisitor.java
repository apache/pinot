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

/**
 * A base implementation of a visitor pattern where the children of a given node are visited first and after that the
 * node is processed (post-order traversal).
 */
public abstract class DefaultPostOrderTraversalVisitor<T, C> implements PlanNodeVisitor<T, C> {

  public abstract T process(PlanNode planNode, C context);

  @Override
  public T visitAggregate(AggregateNode node, C context) {
    node.getInputs().get(0).visit(this, context);
    return process(node, context);
  }

  @Override
  public T visitFilter(FilterNode node, C context) {
    node.getInputs().get(0).visit(this, context);
    return process(node, context);
  }

  @Override
  public T visitJoin(JoinNode node, C context) {
    node.getInputs().get(0).visit(this, context);
    node.getInputs().get(1).visit(this, context);
    return process(node, context);
  }

  @Override
  public T visitMailboxReceive(MailboxReceiveNode node, C context) {
    node.getSender().visit(this, context);
    return process(node, context);
  }

  @Override
  public T visitMailboxSend(MailboxSendNode node, C context) {
    node.getInputs().get(0).visit(this, context);
    return process(node, context);
  }

  @Override
  public T visitProject(ProjectNode node, C context) {
    node.getInputs().get(0).visit(this, context);
    return process(node, context);
  }

  @Override
  public T visitSort(SortNode node, C context) {
    node.getInputs().get(0).visit(this, context);
    return process(node, context);
  }

  @Override
  public T visitTableScan(TableScanNode node, C context) {
    return process(node, context);
  }

  @Override
  public T visitValue(ValueNode node, C context) {
    return process(node, context);
  }

  @Override
  public T visitWindow(WindowNode node, C context) {
    node.getInputs().get(0).visit(this, context);
    return process(node, context);
  }

  @Override
  public T visitSetOp(SetOpNode node, C context) {
    node.getInputs().forEach(input -> input.visit(this, context));
    return process(node, context);
  }

  @Override
  public T visitExchange(ExchangeNode node, C context) {
    node.getInputs().forEach(input -> input.visit(this, context));
    return process(node, context);
  }
}
