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


/// Receives streams ordered by the logical plan and preserves their order with a k-way merge.
/// The broker emits this distinct wire node only for a homogeneous supported cluster.
/// Stage wiring is confined to query planning; instances are not thread-safe.
public class MailboxMergeReceiveNode extends BaseMailboxReceiveNode {
  private final List<RelFieldCollation> _collations;
  private final int _fetch;
  private final int _offset;

  public MailboxMergeReceiveNode(int stageId, DataSchema schema, int senderStageId,
      PinotRelExchangeType exchangeType, RelDistribution.Type distribution, @Nullable List<Integer> keys,
      List<RelFieldCollation> collations, @Nullable MailboxSendNode sender) {
    this(stageId, schema, senderStageId, exchangeType, distribution, keys, collations, -1, -1, sender);
  }

  public MailboxMergeReceiveNode(int stageId, DataSchema schema, int senderStageId,
      PinotRelExchangeType exchangeType, RelDistribution.Type distribution, @Nullable List<Integer> keys,
      List<RelFieldCollation> collations, int fetch, int offset, @Nullable MailboxSendNode sender) {
    super(stageId, schema, senderStageId, exchangeType, distribution, keys, sender);
    _collations = collations;
    _fetch = fetch;
    _offset = offset;
  }

  public List<RelFieldCollation> getCollations() {
    return _collations;
  }

  /// Returns the global merge limit, or -1 for an unbounded merge. Zero deliberately emits no rows.
  public int getFetch() {
    return _fetch;
  }

  public int getOffset() {
    return _offset;
  }

  @Override
  public MailboxMergeReceiveNode withSender(MailboxSendNode sender) {
    return new MailboxMergeReceiveNode(getStageId(), getDataSchema(), sender.getStageId(), getExchangeType(),
        getDistributionType(), getKeys(), _collations, _fetch, _offset, sender);
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitMailboxMergeReceive(this, context);
  }

  @Override
  public String explain() {
    return "MAIL_MERGE_RECEIVE(" + getDistributionType() + ")" + (_fetch >= 0 ? " LIMIT " + _fetch : "")
        + (_offset > 0 ? " OFFSET " + _offset : "");
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof MailboxMergeReceiveNode && super.equals(other)
        && _collations.equals(((MailboxMergeReceiveNode) other)._collations)
        && _fetch == ((MailboxMergeReceiveNode) other)._fetch
        && _offset == ((MailboxMergeReceiveNode) other)._offset;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _collations, _fetch, _offset);
  }
}
