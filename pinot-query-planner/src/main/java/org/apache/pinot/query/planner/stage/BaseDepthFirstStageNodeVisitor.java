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
package org.apache.pinot.query.planner.stage;

public abstract class BaseDepthFirstStageNodeVisitor<T, C> implements StageNodeVisitor<T, C> {

  public abstract T visitNode(StageNode stageNode, C context);

  @Override
  public T visitAggregate(AggregateNode node, C context) {
    return visitNode(node, context);
  }

  @Override
  public T visitFilter(FilterNode node, C context) {
    return visitNode(node, context);
  }

  @Override
  public T visitJoin(JoinNode node, C context) {
    return visitNode(node, context);
  }

  @Override
  public T visitMailboxReceive(MailboxReceiveNode node, C context) {
    return visitNode(node, context);
  }

  @Override
  public T visitMailboxSend(MailboxSendNode node, C context) {
    return visitNode(node, context);
  }

  @Override
  public T visitProject(ProjectNode node, C context) {
    return visitNode(node, context);
  }

  @Override
  public T visitSort(SortNode node, C context) {
    return visitNode(node, context);
  }

  @Override
  public T visitTableScan(TableScanNode node, C context) {
    return visitNode(node, context);
  }

  @Override
  public T visitValue(ValueNode node, C context) {
    return visitNode(node, context);
  }
}
