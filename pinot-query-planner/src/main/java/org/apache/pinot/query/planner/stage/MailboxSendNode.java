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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.serde.ProtoProperties;


public class MailboxSendNode extends AbstractStageNode {
  @ProtoProperties
  private int _receiverStageId;
  @ProtoProperties
  private RelDistribution.Type _exchangeType;
  @ProtoProperties
  private KeySelector<Object[], Object[]> _partitionKeySelector;
  @ProtoProperties
  private List<RexExpression> _collationKeys;
  @ProtoProperties
  private List<RelFieldCollation.Direction> _collationDirections;
  @ProtoProperties
  private boolean _isSortOnSender;

  public MailboxSendNode(int stageId) {
    super(stageId);
  }

  public MailboxSendNode(int stageId, DataSchema dataSchema, int receiverStageId,
      RelDistribution.Type exchangeType, @Nullable KeySelector<Object[], Object[]> partitionKeySelector,
      @Nullable List<RelFieldCollation> fieldCollations, boolean isSortOnSender) {
    super(stageId, dataSchema);
    _receiverStageId = receiverStageId;
    _exchangeType = exchangeType;
    _partitionKeySelector = partitionKeySelector;
    // TODO: Support ordering here if the 'fieldCollations' aren't empty and 'sortOnSender' is true
    Preconditions.checkState(!isSortOnSender, "Ordering is not yet supported on Mailbox Send");
    if (!CollectionUtils.isEmpty(fieldCollations) && isSortOnSender) {
      _collationKeys = new ArrayList<>(fieldCollations.size());
      _collationDirections = new ArrayList<>(fieldCollations.size());
      for (RelFieldCollation fieldCollation : fieldCollations) {
        _collationDirections.add(fieldCollation.getDirection());
        _collationKeys.add(new RexExpression.InputRef(fieldCollation.getFieldIndex()));
      }
    } else {
      _collationKeys = Collections.emptyList();
      _collationDirections = Collections.emptyList();
    }
    _isSortOnSender = isSortOnSender;
  }

  public int getReceiverStageId() {
    return _receiverStageId;
  }

  public void setExchangeType(RelDistribution.Type exchangeType) {
    _exchangeType = exchangeType;
  }

  public RelDistribution.Type getExchangeType() {
    return _exchangeType;
  }

  public KeySelector<Object[], Object[]> getPartitionKeySelector() {
    return _partitionKeySelector;
  }

  public List<RexExpression> getCollationKeys() {
    return _collationKeys;
  }

  public List<RelFieldCollation.Direction> getCollationDirections() {
    return _collationDirections;
  }

  public boolean isSortOnSender() {
    return _isSortOnSender;
  }

  @Override
  public String explain() {
    return "MAIL_SEND(" + _exchangeType + ")";
  }

  @Override
  public <T, C> T visit(StageNodeVisitor<T, C> visitor, C context) {
    return visitor.visitMailboxSend(this, context);
  }
}
