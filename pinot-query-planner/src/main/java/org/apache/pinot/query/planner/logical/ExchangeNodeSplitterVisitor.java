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

import java.util.List;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.query.planner.partitioning.FieldSelectionKeySelector;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.stage.AggregateNode;
import org.apache.pinot.query.planner.stage.ExchangeNode;
import org.apache.pinot.query.planner.stage.FilterNode;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.planner.stage.MailboxSendNode;
import org.apache.pinot.query.planner.stage.ProjectNode;
import org.apache.pinot.query.planner.stage.SetOpNode;
import org.apache.pinot.query.planner.stage.SortNode;
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.planner.stage.StageNodeVisitor;
import org.apache.pinot.query.planner.stage.TableScanNode;
import org.apache.pinot.query.planner.stage.ValueNode;
import org.apache.pinot.query.planner.stage.WindowNode;


public class ExchangeNodeSplitterVisitor implements StageNodeVisitor<StageNode, Void> {
  public static final ExchangeNodeSplitterVisitor INSTANCE = new ExchangeNodeSplitterVisitor();

  @Override
  public StageNode visitAggregate(AggregateNode node, Void context) {
    List<StageNode> inputs = node.getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      inputs.set(i, inputs.get(i).visit(this, context));
    }
    return node;
  }

  @Override
  public StageNode visitFilter(FilterNode node, Void context) {
    List<StageNode> inputs = node.getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      inputs.set(i, inputs.get(i).visit(this, context));
    }
    return node;
  }

  @Override
  public StageNode visitJoin(JoinNode node, Void context) {
    List<StageNode> inputs = node.getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      inputs.set(i, inputs.get(i).visit(this, context));
    }
    return node;
  }

  @Override
  public StageNode visitMailboxReceive(MailboxReceiveNode node, Void context) {
    List<StageNode> inputs = node.getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      inputs.set(i, inputs.get(i).visit(this, context));
    }
    return node;
  }

  @Override
  public StageNode visitMailboxSend(MailboxSendNode node, Void context) {
    List<StageNode> inputs = node.getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      inputs.set(i, inputs.get(i).visit(this, context));
    }
    return node;
  }

  @Override
  public StageNode visitProject(ProjectNode node, Void context) {
    List<StageNode> inputs = node.getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      inputs.set(i, inputs.get(i).visit(this, context));
    }
    return node;
  }

  @Override
  public StageNode visitSort(SortNode node, Void context) {
    List<StageNode> inputs = node.getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      inputs.set(i, inputs.get(i).visit(this, context));
    }
    return node;
  }

  @Override
  public StageNode visitTableScan(TableScanNode node, Void context) {
    List<StageNode> inputs = node.getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      inputs.set(i, inputs.get(i).visit(this, context));
    }
    return node;
  }

  @Override
  public StageNode visitValue(ValueNode node, Void context) {
    List<StageNode> inputs = node.getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      inputs.set(i, inputs.get(i).visit(this, context));
    }
    return node;
  }

  @Override
  public StageNode visitWindow(WindowNode node, Void context) {
    List<StageNode> inputs = node.getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      inputs.set(i, inputs.get(i).visit(this, context));
    }
    return node;
  }

  @Override
  public StageNode visitSetOp(SetOpNode node, Void context) {
    List<StageNode> inputs = node.getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      inputs.set(i, inputs.get(i).visit(this, context));
    }
    return node;
  }

  @Override
  public StageNode visitExchange(ExchangeNode exchangeNode, Void context) {
    List<StageNode> inputs = exchangeNode.getInputs();
    for (int i = 0; i < inputs.size(); i++) {
      inputs.set(i, inputs.get(i).visit(this, context));
    }
    StageNode nextStageRoot = exchangeNode.getInputs().get(0);
    List<Integer> distributionKeys = exchangeNode.getDistributionKeys();
    RelDistribution.Type exchangeType = exchangeNode.getDistributionType();

    // make an exchange sender and receiver node pair
    // only HASH_DISTRIBUTED requires a partition key selector; so all other types (SINGLETON and BROADCAST)
    // of exchange will not carry a partition key selector.
    KeySelector<Object[], Object[]> keySelector = exchangeType == RelDistribution.Type.HASH_DISTRIBUTED
        ? new FieldSelectionKeySelector(distributionKeys) : null;

    StageNode mailboxSender = new MailboxSendNode(nextStageRoot.getStageId(), nextStageRoot.getDataSchema(),
        -1, exchangeType, keySelector, exchangeNode.getCollations(), exchangeNode.isSortOnSender());
    StageNode mailboxReceiver = new MailboxReceiveNode(-1, nextStageRoot.getDataSchema(),
        nextStageRoot.getStageId(), exchangeType, keySelector,
        exchangeNode.getCollations(), exchangeNode.isSortOnSender(), exchangeNode.isSortOnReceiver(), mailboxSender);
    mailboxSender.addInput(nextStageRoot);

    return mailboxReceiver;
  }
}
