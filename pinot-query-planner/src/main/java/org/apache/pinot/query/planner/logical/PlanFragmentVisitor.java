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
import java.util.Map;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.plannode.WindowNode;


public class PlanFragmentVisitor implements PlanNodeVisitor<Void, PlanFragmentVisitor.Context> {
  @Override
  public Void visitAggregate(AggregateNode node, PlanFragmentVisitor.Context context) {
    return null;
  }

  @Override
  public Void visitFilter(FilterNode node, PlanFragmentVisitor.Context context) {
    return null;
  }

  @Override
  public Void visitJoin(JoinNode node, PlanFragmentVisitor.Context context) {
    return null;
  }

  @Override
  public Void visitMailboxReceive(MailboxReceiveNode node, PlanFragmentVisitor.Context context) {
    return null;
  }

  @Override
  public Void visitMailboxSend(MailboxSendNode node, PlanFragmentVisitor.Context context) {
    return null;
  }

  @Override
  public Void visitProject(ProjectNode node, PlanFragmentVisitor.Context context) {
    return null;
  }

  @Override
  public Void visitSort(SortNode node, PlanFragmentVisitor.Context context) {
    return null;
  }

  @Override
  public Void visitTableScan(TableScanNode node, PlanFragmentVisitor.Context context) {
    return null;
  }

  @Override
  public Void visitValue(ValueNode node, PlanFragmentVisitor.Context context) {
    return null;
  }

  @Override
  public Void visitWindow(WindowNode node, PlanFragmentVisitor.Context context) {
    return null;
  }

  @Override
  public Void visitSetOp(SetOpNode setOpNode, PlanFragmentVisitor.Context context) {
    return null;
  }

  @Override
  public Void visitExchange(ExchangeNode exchangeNode, Context context) {
    return null;
  }

  public class Context {
    private final int _fragmentId;

    private final Map<Integer, PlanNode> _fragmentIdToRootNodeMap;
    private final Map<Integer, List<Integer>> _fragmentIdToChildFragmentIdsMap;

    public Context(int fragmentId) {
      _fragmentId = fragmentId;
      _fragmentIdToRootNodeMap = new HashMap<>();
      _fragmentIdToChildFragmentIdsMap = new HashMap<>();
    }

    public int getFragmentId() {
      return _fragmentId;
    }

    public Map<Integer, PlanNode> getFragmentIdToRootNodeMap() {
      return _fragmentIdToRootNodeMap;
    }

    public Map<Integer, List<Integer>> getFragmentIdToChildFragmentIdsMap() {
      return _fragmentIdToChildFragmentIdsMap;
    }
  }
}
