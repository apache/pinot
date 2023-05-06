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
package org.apache.pinot.query.planner.logical;

import org.apache.pinot.query.planner.stage.AggregateNode;
import org.apache.pinot.query.planner.stage.ExchangeNode;
import org.apache.pinot.query.planner.stage.FilterNode;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.planner.stage.MailboxSendNode;
import org.apache.pinot.query.planner.stage.ProjectNode;
import org.apache.pinot.query.planner.stage.SetOpNode;
import org.apache.pinot.query.planner.stage.SortNode;
import org.apache.pinot.query.planner.stage.StageNodeVisitor;
import org.apache.pinot.query.planner.stage.TableScanNode;
import org.apache.pinot.query.planner.stage.ValueNode;
import org.apache.pinot.query.planner.stage.WindowNode;


public class StageIdAssigner implements StageNodeVisitor<Void, StageIdAssigner.Context> {
  public static final StageIdAssigner INSTANCE = new StageIdAssigner();

  @Override
  public Void visitAggregate(AggregateNode node, Context context) {
    node.setStageId(context._currentStageId);
    node.getInputs().forEach(input -> {
      context._previousStageId = node.getStageId();
      input.visit(this, context);
    });
    return null;
  }

  @Override
  public Void visitFilter(FilterNode node, Context context) {
    node.setStageId(context._currentStageId);
    node.getInputs().forEach(input -> {
      context._previousStageId = node.getStageId();
      input.visit(this, context);
    });
    return null;
  }

  @Override
  public Void visitJoin(JoinNode node, Context context) {
    node.setStageId(context._currentStageId);
    node.getInputs().forEach(input -> {
      context._previousStageId = node.getStageId();
      input.visit(this, context);
    });
    return null;
  }

  @Override
  public Void visitMailboxReceive(MailboxReceiveNode node, Context context) {
    node.setStageId(context._previousStageId);
    context._currentStageId++;
    node.setSenderStageId(context._currentStageId);
    context._previousStageId = node.getStageId();
    node.getSender().visit(this, context);
    return null;
  }

  @Override
  public Void visitMailboxSend(MailboxSendNode node, Context context) {
    node.setStageId(context._currentStageId);
    node.setReceiverStageId(context._previousStageId);
    node.getInputs().forEach(input -> {
      context._previousStageId = node.getStageId();
      input.visit(this, context);
    });
    return null;
  }

  @Override
  public Void visitProject(ProjectNode node, Context context) {
    node.setStageId(context._currentStageId);
    node.getInputs().forEach(input -> {
      context._previousStageId = node.getStageId();
      input.visit(this, context);
    });
    return null;
  }

  @Override
  public Void visitSort(SortNode node, Context context) {
    node.setStageId(context._currentStageId);
    node.getInputs().forEach(input -> {
      context._previousStageId = node.getStageId();
      input.visit(this, context);
    });
    return null;
  }

  @Override
  public Void visitTableScan(TableScanNode node, Context context) {
    node.setStageId(context._currentStageId);
    node.getInputs().forEach(input -> {
      context._previousStageId = node.getStageId();
      input.visit(this, context);
    });
    return null;
  }

  @Override
  public Void visitValue(ValueNode node, Context context) {
    node.setStageId(context._currentStageId);
    node.getInputs().forEach(input -> {
      context._previousStageId = node.getStageId();
      input.visit(this, context);
    });
    return null;
  }

  @Override
  public Void visitWindow(WindowNode node, Context context) {
    node.setStageId(context._currentStageId);
    node.getInputs().forEach(input -> {
      context._previousStageId = node.getStageId();
      input.visit(this, context);
    });
    return null;
  }

  @Override
  public Void visitSetOp(SetOpNode node, Context context) {
    node.setStageId(context._currentStageId);
    node.getInputs().forEach(input -> {
      context._previousStageId = node.getStageId();
      input.visit(this, context);
    });
    return null;
  }

  @Override
  public Void visitExchange(ExchangeNode node, Context context) {
    throw new UnsupportedOperationException("ExchangeNode should not be visited by StageIdAssigner");
  }

  public static class Context {

    // Stage ID starts with 1, 0 will be reserved for ROOT stage.
    Integer _currentStageId = 1;
    Integer _previousStageId = 1;
  }
}
