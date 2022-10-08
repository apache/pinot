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

public interface StageNodeVisitor<T, C> {

  T visitAggregate(AggregateNode node, C context);

  T visitFilter(FilterNode node, C context);

  T visitJoin(JoinNode node, C context);

  T visitMailboxReceive(MailboxReceiveNode node, C context);

  T visitMailboxSend(MailboxSendNode node, C context);

  T visitProject(ProjectNode node, C context);

  T visitSort(SortNode node, C context);

  T visitTableScan(TableScanNode node, C context);

  T visitValue(ValueNode node, C context);
}
