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

import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.RelDistribution;


public class MailboxSendNode extends AbstractStageNode {
  private final StageNode _stageRoot;
  private final int _receiverStageId;
  private final RelDistribution.Type _exchangeType;

  public MailboxSendNode(StageNode stageRoot, int receiverStageId, RelDistribution.Type exchangeType) {
    super(stageRoot.getStageId());
    _stageRoot = stageRoot;
    _receiverStageId = receiverStageId;
    _exchangeType = exchangeType;
  }

  @Override
  public List<StageNode> getInputs() {
    return Collections.singletonList(_stageRoot);
  }

  @Override
  public void addInput(StageNode queryStageRoot) {
    throw new UnsupportedOperationException("mailbox cannot be changed!");
  }

  public int getReceiverStageId() {
    return _receiverStageId;
  }

  public RelDistribution.Type getExchangeType() {
    return _exchangeType;
  }
}
