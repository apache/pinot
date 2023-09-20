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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.logical.PinotRelExchangeType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.serde.ProtoProperties;


public class MailboxSendNode extends AbstractPlanNode {
  @ProtoProperties
  private int _receiverStageId;
  @ProtoProperties
  private RelDistribution.Type _distributionType;
  @ProtoProperties
  private PinotRelExchangeType _exchangeType;
  @ProtoProperties
  private List<Integer> _distributionKeys;
  @ProtoProperties
  private List<RexExpression> _collationKeys;
  @ProtoProperties
  private List<RelFieldCollation.Direction> _collationDirections;
  @ProtoProperties
  private boolean _isSortOnSender;

  public MailboxSendNode(int planFragmentId) {
    super(planFragmentId);
  }

  public MailboxSendNode(int planFragmentId, DataSchema dataSchema, int receiverStageId,
      RelDistribution.Type distributionType, PinotRelExchangeType exchangeType,
      @Nullable List<Integer> distributionKeys, @Nullable List<RelFieldCollation> fieldCollations,
      boolean isSortOnSender) {
    super(planFragmentId, dataSchema);
    _receiverStageId = receiverStageId;
    _distributionType = distributionType;
    _exchangeType = exchangeType;
    _distributionKeys = distributionKeys;
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

  public void setReceiverStageId(int receiverStageId) {
    _receiverStageId = receiverStageId;
  }

  public void setDistributionType(RelDistribution.Type distributionType) {
    _distributionType = distributionType;
  }

  public RelDistribution.Type getDistributionType() {
    return _distributionType;
  }

  public void setExchangeType(PinotRelExchangeType exchangeType) {
    _exchangeType = exchangeType;
  }

  public PinotRelExchangeType getExchangeType() {
    return _exchangeType;
  }

  public List<Integer> getDistributionKeys() {
    return _distributionKeys;
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
    return "MAIL_SEND(" + _distributionType + ")";
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitMailboxSend(this, context);
  }
}
