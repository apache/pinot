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


public class MailboxReceiveNode extends BaseMailboxReceiveNode {
  private final List<RelFieldCollation> _collations;
  private final boolean _sort;
  private final boolean _sortedOnSender;
  private final boolean _autoProfile;

  // NOTE: null List is converted to empty List because there is no way to differentiate them in proto during ser/de.
  public MailboxReceiveNode(int stageId, DataSchema dataSchema, int senderStageId,
      PinotRelExchangeType exchangeType, RelDistribution.Type distributionType, @Nullable List<Integer> keys,
      @Nullable List<RelFieldCollation> collations, boolean sort, boolean sortedOnSender,
      @Nullable MailboxSendNode sender) {
    this(stageId, dataSchema, senderStageId, exchangeType, distributionType, keys, collations, sort,
        sortedOnSender, false, sender);
  }

  public MailboxReceiveNode(int stageId, DataSchema dataSchema, int senderStageId,
      PinotRelExchangeType exchangeType, RelDistribution.Type distributionType, @Nullable List<Integer> keys,
      @Nullable List<RelFieldCollation> collations, boolean sort, boolean sortedOnSender, boolean autoProfile,
      @Nullable MailboxSendNode sender) {
    super(stageId, dataSchema, senderStageId, exchangeType, distributionType, keys, sender);
    _collations = collations != null ? collations : List.of();
    _sort = sort;
    _sortedOnSender = sortedOnSender;
    _autoProfile = autoProfile;
  }

  public List<RelFieldCollation> getCollations() {
    return _collations;
  }

  /// @deprecated Current plans express ordering with explicit sort or merge receive nodes.
  @Deprecated(since = "1.6.0")
  public boolean isSort() {
    return _sort;
  }

  /// @deprecated Current plans express ordering with explicit sort or merge receive nodes.
  @Deprecated(since = "1.6.0")
  public boolean isSortedOnSender() {
    return _sortedOnSender;
  }

  /// Whether this receiver gathers bounded order samples for AUTO strategy selection.
  public boolean isAutoProfile() {
    return _autoProfile;
  }

  @Override
  public String explain() {
    return "MAIL_RECEIVE(" + getDistributionType() + ")" + (_autoProfile ? "[WINDOW_SORT_AUTO_PROFILE]" : "");
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitMailboxReceive(this, context);
  }

  @Override
  public MailboxReceiveNode withSender(MailboxSendNode sender) {
    return new MailboxReceiveNode(_stageId, _dataSchema, sender.getStageId(), getExchangeType(), getDistributionType(),
        getKeys(), _collations, _sort, _sortedOnSender, _autoProfile, sender);
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
    return _sort == that._sort && _sortedOnSender == that._sortedOnSender && _autoProfile == that._autoProfile
        && Objects.equals(_collations, that._collations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _collations, _sort, _sortedOnSender, _autoProfile);
  }
}
