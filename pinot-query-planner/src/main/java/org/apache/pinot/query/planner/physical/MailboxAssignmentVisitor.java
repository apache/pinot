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
package org.apache.pinot.query.planner.physical;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.query.planner.stage.AggregateNode;
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
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.QueryServerInstance;


public class MailboxAssignmentVisitor implements StageNodeVisitor<Void, DispatchablePlanContext> {
  public static final MailboxAssignmentVisitor INSTANCE = new MailboxAssignmentVisitor();

  @Override
  public Void visitAggregate(AggregateNode node, DispatchablePlanContext context) {
    node.getInputs().stream().forEach(input -> input.visit(this, context));
    return null;
  }

  @Override
  public Void visitFilter(FilterNode node, DispatchablePlanContext context) {
    node.getInputs().stream().forEach(input -> input.visit(this, context));
    return null;
  }

  @Override
  public Void visitJoin(JoinNode node, DispatchablePlanContext context) {
    node.getInputs().stream().forEach(input -> input.visit(this, context));
    return null;
  }

  @Override
  public Void visitMailboxReceive(MailboxReceiveNode node, DispatchablePlanContext context) {
    node.getInputs().stream().forEach(input -> input.visit(this, context));
    int receiveStageId = node.getStageId();
    int senderStageId = node.getSenderStageId();
    DispatchablePlanMetadata receiveStagePlanMetadata = context.getDispatchablePlanMetadataMap().get(receiveStageId);
    DispatchablePlanMetadata senderStagePlanMetadata = context.getDispatchablePlanMetadataMap().get(senderStageId);
    receiveStagePlanMetadata.getServerInstanceToWorkerIdMap().entrySet().stream().forEach(receiveEntry -> {
      List<Integer> receiveWorkerIds = receiveEntry.getValue();
      for (int receiveWorkerId : receiveWorkerIds) {
        receiveStagePlanMetadata.getStageIdToMailBoxIdsMap().putIfAbsent(receiveWorkerId, new ArrayList<>());
        senderStagePlanMetadata.getStageIdToMailBoxIdsMap().putIfAbsent(receiveWorkerId, new ArrayList<>());
        senderStagePlanMetadata.getServerInstanceToWorkerIdMap().entrySet().stream().forEach(senderEntry -> {
          QueryServerInstance senderServerInstance = senderEntry.getKey();
          List<Integer> senderWorkerIds = senderEntry.getValue();
          for (int senderWorkerId : senderWorkerIds) {
            String mailboxId =
                MailboxIdUtils.toPlanMailboxId(senderStageId, senderWorkerId, receiveStageId, receiveWorkerId);
            MailboxInfo mailboxInfo =
                new MailboxInfo(mailboxId, node.getExchangeType().toString(), senderServerInstance.getHostname(),
                    senderServerInstance.getQueryMailboxPort(), ImmutableMap.of());
            receiveStagePlanMetadata.getStageIdToMailBoxIdsMap().get(receiveWorkerId).add(mailboxInfo);
            senderStagePlanMetadata.getStageIdToMailBoxIdsMap().get(receiveWorkerId).add(mailboxInfo);
          }
        });
      }
    });
    return null;
  }

  @Override
  public Void visitMailboxSend(MailboxSendNode node, DispatchablePlanContext context) {
    node.getInputs().stream().forEach(input -> input.visit(this, context));
    int senderStageId = node.getStageId();
    int receiveStageId = node.getReceiverStageId();
    DispatchablePlanMetadata senderStagePlanMetadata = context.getDispatchablePlanMetadataMap().get(senderStageId);
    DispatchablePlanMetadata receiveStagePlanMetadata = context.getDispatchablePlanMetadataMap().get(receiveStageId);
    receiveStagePlanMetadata.getServerInstanceToWorkerIdMap().entrySet().stream().forEach(receiveEntry -> {
      QueryServerInstance receiveServerInstance = receiveEntry.getKey();
      List<Integer> receiveWorkerIds = receiveEntry.getValue();
      for (int receiveWorkerId : receiveWorkerIds) {
        receiveStagePlanMetadata.getStageIdToMailBoxIdsMap().putIfAbsent(receiveWorkerId, new ArrayList<>());
        senderStagePlanMetadata.getStageIdToMailBoxIdsMap().putIfAbsent(receiveWorkerId, new ArrayList<>());
        senderStagePlanMetadata.getServerInstanceToWorkerIdMap().entrySet().stream().forEach(senderEntry -> {
          List<Integer> senderWorkerIds = senderEntry.getValue();
          for (int senderWorkerId : senderWorkerIds) {
            String mailboxId =
                MailboxIdUtils.toPlanMailboxId(senderStageId, senderWorkerId, receiveStageId, receiveWorkerId);
            MailboxInfo mailboxInfo =
                new MailboxInfo(mailboxId, node.getExchangeType().toString(), receiveServerInstance.getHostname(),
                    receiveServerInstance.getQueryMailboxPort(), ImmutableMap.of());
            receiveStagePlanMetadata.getStageIdToMailBoxIdsMap().get(receiveWorkerId).add(mailboxInfo);
            senderStagePlanMetadata.getStageIdToMailBoxIdsMap().get(receiveWorkerId).add(mailboxInfo);
          }
        });
      }
    });
    return null;
  }

  @Override
  public Void visitProject(ProjectNode node, DispatchablePlanContext context) {
    node.getInputs().stream().forEach(input -> input.visit(this, context));
    return null;
  }

  @Override
  public Void visitSort(SortNode node, DispatchablePlanContext context) {
    node.getInputs().stream().forEach(input -> input.visit(this, context));
    return null;
  }

  @Override
  public Void visitTableScan(TableScanNode node, DispatchablePlanContext context) {
    node.getInputs().stream().forEach(input -> input.visit(this, context));
    return null;
  }

  @Override
  public Void visitValue(ValueNode node, DispatchablePlanContext context) {
    node.getInputs().stream().forEach(input -> input.visit(this, context));
    return null;
  }

  @Override
  public Void visitWindow(WindowNode node, DispatchablePlanContext context) {
    node.getInputs().stream().forEach(input -> input.visit(this, context));
    return null;
  }

  @Override
  public Void visitSetOp(SetOpNode node, DispatchablePlanContext context) {
    node.getInputs().stream().forEach(input -> input.visit(this, context));
    return null;
  }
}
