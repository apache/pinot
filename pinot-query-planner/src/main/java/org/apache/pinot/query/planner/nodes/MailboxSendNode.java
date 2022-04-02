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
package org.apache.pinot.query.planner.nodes;

import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.proto.Plan;


public class MailboxSendNode extends AbstractStageNode {
  private int _receiverStageId;
  private RelDistribution.Type _exchangeType;

  public MailboxSendNode(int stageId) {
    super(stageId);
  }

  public MailboxSendNode(int stageId, int receiverStageId, RelDistribution.Type exchangeType) {
    super(stageId);
    _receiverStageId = receiverStageId;
    _exchangeType = exchangeType;
  }

  public int getReceiverStageId() {
    return _receiverStageId;
  }

  public RelDistribution.Type getExchangeType() {
    return _exchangeType;
  }

  @Override
  public void setFields(Plan.ObjectFields objFields) {
    _receiverStageId = objFields.getLiteralFieldOrThrow("receivingStageId").getIntField();
    _exchangeType = RelDistribution.Type.valueOf(objFields.getLiteralFieldOrThrow("exchangeType").getStringField());
  }

  @Override
  public Plan.ObjectFields getFields() {
    return Plan.ObjectFields.newBuilder()
        .putLiteralField("receivingStageId", SerDeUtils.intField(_receiverStageId))
        .putLiteralField("exchangeType", SerDeUtils.stringField(_exchangeType.name()))
        .build();
  }
}
