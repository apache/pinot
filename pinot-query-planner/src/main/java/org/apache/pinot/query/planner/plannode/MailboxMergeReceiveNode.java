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
package org.apache.pinot.query.planner.plannode;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.utils.DataSchema;


/// Receives streams ordered by the logical plan and preserves that order with a k-way merge.
/// This distinct wire node is emitted only when the broker verifies homogeneous cluster versions.
/// Instances belong to a single query plan and are not thread-safe.
public class MailboxMergeReceiveNode extends BasePlanNode {
  private int _senderStageId;
  private final PinotRelExchangeType _exchangeType;
  private RelDistribution.Type _distributionType;
  private final List<Integer> _keys;
  private final List<RelFieldCollation> _collations;
  // Available only during query planning; never serialized.
  private transient MailboxSendNode _sender;

  public MailboxMergeReceiveNode(int stageId, DataSchema schema, int senderStageId,
      PinotRelExchangeType exchangeType, RelDistribution.Type distribution, @Nullable List<Integer> keys,
      List<RelFieldCollation> collations, @Nullable MailboxSendNode sender) {
    super(stageId, schema, null, List.of());
    _senderStageId = senderStageId;
    _exchangeType = exchangeType;
    _distributionType = distribution;
    _keys = keys != null ? keys : List.of();
    _collations = collations;
    _sender = sender;
  }

  public int getSenderStageId() {
    assert _sender == null || _sender.getStageId() == _senderStageId;
    return _senderStageId;
  }

  public PinotRelExchangeType getExchangeType() {
    return _exchangeType;
  }

  public RelDistribution.Type getDistributionType() {
    return _distributionType;
  }

  public void setDistributionType(RelDistribution.Type distributionType) {
    _distributionType = distributionType;
  }

  public List<Integer> getKeys() {
    return _keys;
  }

  public List<RelFieldCollation> getCollations() {
    return _collations;
  }

  public MailboxSendNode getSender() {
    assert _sender != null;
    return _sender;
  }

  public void setSender(MailboxSendNode sender) {
    _senderStageId = sender.getStageId();
    _sender = sender;
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitMailboxMergeReceive(this, context);
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    Preconditions.checkArgument(inputs.isEmpty(), "Cannot set inputs for MailboxMergeReceiveNode");
    return this;
  }

  public MailboxMergeReceiveNode withSender(MailboxSendNode sender) {
    return new MailboxMergeReceiveNode(_stageId, _dataSchema, sender.getStageId(), _exchangeType,
        _distributionType, _keys, _collations, sender);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass() || !super.equals(other)) {
      return false;
    }
    MailboxMergeReceiveNode that = (MailboxMergeReceiveNode) other;
    return _senderStageId == that._senderStageId && _exchangeType == that._exchangeType
        && _distributionType == that._distributionType && _keys.equals(that._keys)
        && _collations.equals(that._collations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _senderStageId, _exchangeType, _distributionType, _keys, _collations);
  }

  @Override
  public String explain() {
    return "MAIL_MERGE_RECEIVE(" + _distributionType + ")";
  }
}
