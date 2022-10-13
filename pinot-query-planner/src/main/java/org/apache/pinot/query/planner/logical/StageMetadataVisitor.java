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

import java.util.HashMap;
import java.util.List;
import org.apache.calcite.util.Pair;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.stage.AggregateNode;
import org.apache.pinot.query.planner.stage.FilterNode;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.planner.stage.MailboxSendNode;
import org.apache.pinot.query.planner.stage.ProjectNode;
import org.apache.pinot.query.planner.stage.SortNode;
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.planner.stage.StageNodeVisitor;
import org.apache.pinot.query.planner.stage.TableScanNode;
import org.apache.pinot.query.planner.stage.ValueNode;


/**
 * {@code StageMetadataVisitor} computes the {@link StageMetadata} for a {@link StageNode}
 * tree and attaches it in the form of a {@link QueryPlan}.
 */
public class StageMetadataVisitor implements StageNodeVisitor<Void, QueryPlan> {

  public static QueryPlan attachMetadata(List<Pair<Integer, String>> fields, StageNode root) {
    QueryPlan queryPlan = new QueryPlan(fields, new HashMap<>(), new HashMap<>());
    root.visit(new StageMetadataVisitor(), queryPlan);
    return queryPlan;
  }

  /**
   * Usage of this class should only come through {@link #attachMetadata(List, StageNode)}.
   */
  private StageMetadataVisitor() {
  }

  private void visit(StageNode node, QueryPlan queryPlan) {
    queryPlan
        .getStageMetadataMap()
        .computeIfAbsent(node.getStageId(), (id) -> new StageMetadata())
        .attach(node);
  }

  @Override
  public Void visitAggregate(AggregateNode node, QueryPlan context) {
    node.getInputs().get(0).visit(this, context);
    visit(node, context);
    return null;
  }

  @Override
  public Void visitFilter(FilterNode node, QueryPlan context) {
    node.getInputs().get(0).visit(this, context);
    visit(node, context);
    return null;
  }

  @Override
  public Void visitJoin(JoinNode node, QueryPlan context) {
    node.getInputs().forEach(join -> join.visit(this, context));
    visit(node, context);
    return null;
  }

  @Override
  public Void visitMailboxReceive(MailboxReceiveNode node, QueryPlan context) {
    node.getSender().visit(this, context);
    visit(node, context);

    // special case for the global mailbox receive node
    if (node.getStageId() == 0) {
      context.getQueryStageMap().put(0, node);
    }

    return null;
  }

  @Override
  public Void visitMailboxSend(MailboxSendNode node, QueryPlan context) {
    node.getInputs().get(0).visit(this, context);
    visit(node, context);

    context.getQueryStageMap().put(node.getStageId(), node);
    return null;
  }

  @Override
  public Void visitProject(ProjectNode node, QueryPlan context) {
    node.getInputs().get(0).visit(this, context);
    visit(node, context);
    return null;
  }

  @Override
  public Void visitSort(SortNode node, QueryPlan context) {
    node.getInputs().get(0).visit(this, context);
    visit(node, context);
    return null;
  }

  @Override
  public Void visitTableScan(TableScanNode node, QueryPlan context) {
    visit(node, context);
    return null;
  }

  @Override
  public Void visitValue(ValueNode node, QueryPlan context) {
    visit(node, context);
    return null;
  }
}
