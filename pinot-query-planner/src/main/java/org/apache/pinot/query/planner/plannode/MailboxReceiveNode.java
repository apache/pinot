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


public class MailboxReceiveNode extends BasePlanNode {
  private int _senderStageId;
  private final PinotRelExchangeType _exchangeType;
  private RelDistribution.Type _distributionType;
  private final List<Integer> _keys;
  private final List<RelFieldCollation> _collations;
  private final boolean _sort;
  private final boolean _sortedOnSender;

  // NOTE: This is only available during query planning, and should not be serialized.
  private transient MailboxSendNode _sender;

  // NOTE: null List is converted to empty List because there is no way to differentiate them in proto during ser/de.
  public MailboxReceiveNode(int stageId, DataSchema dataSchema, int senderStageId,
      PinotRelExchangeType exchangeType, RelDistribution.Type distributionType, @Nullable List<Integer> keys,
      @Nullable List<RelFieldCollation> collations, boolean sort, boolean sortedOnSender,
      @Nullable MailboxSendNode sender) {
    super(stageId, dataSchema, null, List.of());
    _senderStageId = senderStageId;
    _exchangeType = exchangeType;
    _distributionType = distributionType;
    _keys = keys != null ? keys : List.of();
    _collations = collations != null ? collations : List.of();
    _sort = sort;
    _sortedOnSender = sortedOnSender;
    _sender = sender;
  }

  public int getSenderStageId() {
    assert _sender == null || _sender.getStageId() == _senderStageId
        : "_senderStageId should match _sender.getStageId()";
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

  public boolean isSort() {
    return _sort;
  }

  public boolean isSortedOnSender() {
    return _sortedOnSender;
  }

  public MailboxSendNode getSender() {
    assert _sender != null;
    return _sender;
  }

  public void setSender(MailboxSendNode sender) {
    _senderStageId = _sender.getStageId();
    _sender = sender;
  }

  @Override
  public String explain() {
    return "MAIL_RECEIVE(" + _distributionType + ")";
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitMailboxReceive(this, context);
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    Preconditions.checkArgument(inputs.isEmpty(), "Cannot set inputs for MailboxReceiveNode");
    return this;
  }

  public MailboxReceiveNode withSender(MailboxSendNode sender) {
    return new MailboxReceiveNode(_stageId, _dataSchema, _senderStageId, _exchangeType, _distributionType, _keys,
        _collations, _sort, _sortedOnSender, sender);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MailboxReceiveNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    MailboxReceiveNode that = (MailboxReceiveNode) o;
    return _senderStageId == that._senderStageId && _sort == that._sort && _sortedOnSender == that._sortedOnSender
        && _exchangeType == that._exchangeType && _distributionType == that._distributionType && Objects.equals(_keys,
        that._keys) && Objects.equals(_collations, that._collations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _senderStageId, _exchangeType, _distributionType, _keys, _collations, _sort,
        _sortedOnSender);
  }
}
