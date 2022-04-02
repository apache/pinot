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


public class MailboxReceiveNode extends AbstractStageNode {
  private int _senderStageId;
  private RelDistribution.Type _exchangeType;

  public MailboxReceiveNode(int stageId) {
    super(stageId);
  }

  public MailboxReceiveNode(int stageId, int senderStageId, RelDistribution.Type exchangeType) {
    super(stageId);
    _senderStageId = senderStageId;
    _exchangeType = exchangeType;
  }

  public int getSenderStageId() {
    return _senderStageId;
  }

  public RelDistribution.Type getExchangeType() {
    return _exchangeType;
  }

  @Override
  public void setFields(Plan.ObjectFields objFields) {
    _senderStageId = objFields.getLiteralFieldOrThrow("senderStageId").getIntField();
    _exchangeType = RelDistribution.Type.valueOf(objFields.getLiteralFieldOrThrow("exchangeType").getStringField());
  }

  @Override
  public Plan.ObjectFields getFields() {
    return Plan.ObjectFields.newBuilder()
        .putLiteralField("senderStageId", SerDeUtils.intField(_senderStageId))
        .putLiteralField("exchangeType", SerDeUtils.stringField(_exchangeType.name()))
        .build();
  }
}
