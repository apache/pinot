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

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.utils.DataSchema;


public class MailboxSendNode extends BasePlanNode {
  private final int _receiverStageId;
  private final PinotRelExchangeType _exchangeType;
  private RelDistribution.Type _distributionType;
  private final List<Integer> _keys;
  private final boolean _prePartitioned;
  private final List<RelFieldCollation> _collations;
  private final boolean _sort;

  // NOTE: null List is converted to empty List because there is no way to differentiate them in proto during ser/de.
  public MailboxSendNode(int stageId, DataSchema dataSchema, List<PlanNode> inputs, int receiverStageId,
      PinotRelExchangeType exchangeType, RelDistribution.Type distributionType, @Nullable List<Integer> keys,
      boolean prePartitioned, @Nullable List<RelFieldCollation> collations, boolean sort) {
    super(stageId, dataSchema, null, inputs);
    _receiverStageId = receiverStageId;
    _exchangeType = exchangeType;
    _distributionType = distributionType;
    _keys = keys != null ? keys : List.of();
    _prePartitioned = prePartitioned;
    _collations = collations != null ? collations : List.of();
    _sort = sort;
  }

  public int getReceiverStageId() {
    return _receiverStageId;
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

  public boolean isPrePartitioned() {
    return _prePartitioned;
  }

  public List<RelFieldCollation> getCollations() {
    return _collations;
  }

  public boolean isSort() {
    return _sort;
  }

  @Override
  public String explain() {
    StringBuilder sb = new StringBuilder();
    sb.append("MAIL_SEND(");
    sb.append(_distributionType);
    sb.append(')');
    if (isPrePartitioned()) {
      sb.append("[PARTITIONED]");
    }
    if (isSort()) {
      sb.append("[SORTED]");
    }
    return sb.toString();
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitMailboxSend(this, context);
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new MailboxSendNode(_stageId, _dataSchema, inputs, _receiverStageId, _exchangeType, _distributionType, _keys,
        _prePartitioned, _collations, _sort);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MailboxSendNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    MailboxSendNode that = (MailboxSendNode) o;
    return _receiverStageId == that._receiverStageId && _prePartitioned == that._prePartitioned && _sort == that._sort
        && _exchangeType == that._exchangeType && _distributionType == that._distributionType && Objects.equals(_keys,
        that._keys) && Objects.equals(_collations, that._collations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _receiverStageId, _exchangeType, _distributionType, _keys, _prePartitioned,
        _collations, _sort);
  }

  @Override
  public String toString() {
    return "MailboxSendNode{"
        + "_stageId=" + _stageId
        + ", _receiverStageId=" + _receiverStageId
        + '}';
  }
}
