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


/// Receives streams ordered by the logical plan and preserves that order with a k-way merge.
/// This distinct wire node is emitted only when the broker verifies homogeneous cluster versions.
/// Instances belong to a single query plan and are not thread-safe.
public class MailboxMergeReceiveNode extends MailboxReceiveNode {
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
    super(stageId, schema, senderStageId, exchangeType, distribution, keys, collations, false, false, sender);
    _fetch = fetch;
    _offset = offset;
  }

  public int getFetch() {
    return _fetch;
  }

  public int getOffset() {
    return _offset;
  }

  @Override
  public MailboxReceiveNode withSender(MailboxSendNode sender) {
    return new MailboxMergeReceiveNode(getStageId(), getDataSchema(), sender.getStageId(), getExchangeType(),
        getDistributionType(), getKeys(), getCollations(), _fetch, _offset, sender);
  }

  @Override
  public boolean equals(Object other) {
    return super.equals(other) && _fetch == ((MailboxMergeReceiveNode) other)._fetch
        && _offset == ((MailboxMergeReceiveNode) other)._offset;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _fetch, _offset);
  }

  @Override
  public String explain() {
    String description = "MAIL_MERGE_RECEIVE(" + getDistributionType() + ")";
    if (_fetch >= 0) {
      description += " LIMIT " + _fetch;
    }
    if (_offset > 0) {
      description += " OFFSET " + _offset;
    }
    return description;
  }
}
