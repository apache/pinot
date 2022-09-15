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

import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.serde.ProtoProperties;


public class MailboxReceiveNode extends AbstractStageNode {
  @ProtoProperties
  private int _senderStageId;
  @ProtoProperties
  private RelDistribution.Type _exchangeType;
  @ProtoProperties
  private KeySelector<Object[], Object[]> _partitionKeySelector;

  public MailboxReceiveNode(int stageId) {
    super(stageId);
  }

  public MailboxReceiveNode(int stageId, DataSchema dataSchema, int senderStageId,
      RelDistribution.Type exchangeType, @Nullable KeySelector<Object[], Object[]> partitionKeySelector) {
    super(stageId, dataSchema);
    _senderStageId = senderStageId;
    _exchangeType = exchangeType;
    _partitionKeySelector = partitionKeySelector;
  }

  public int getSenderStageId() {
    return _senderStageId;
  }

  public RelDistribution.Type getExchangeType() {
    return _exchangeType;
  }

  public KeySelector<Object[], Object[]> getPartitionKeySelector() {
    return _partitionKeySelector;
  }
}
