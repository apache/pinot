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


/// Planning-only exchange requiring ordered inputs and producing a k-way merge receive and a plain send.
/// Instances are local to a query's planning thread and are never serialized.
public class KWayMergeExchangeNode extends ExchangeNode {
  private final int _fetch;
  private final int _offset;

  public KWayMergeExchangeNode(int stageId, DataSchema schema, List<PlanNode> inputs,
      RelDistribution.Type distribution, @Nullable List<Integer> keys, boolean prePartitioned,
      List<RelFieldCollation> collations, String hashFunction) {
    this(stageId, schema, inputs, distribution, keys, prePartitioned, collations, -1, -1, hashFunction);
  }

  public KWayMergeExchangeNode(int stageId, DataSchema schema, List<PlanNode> inputs,
      RelDistribution.Type distribution, @Nullable List<Integer> keys, boolean prePartitioned,
      List<RelFieldCollation> collations, int fetch, int offset, String hashFunction) {
    super(stageId, schema, inputs, PinotRelExchangeType.STREAMING, distribution, keys, prePartitioned, collations,
        false, false, null, null, hashFunction);
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
  public boolean equals(Object other) {
    return super.equals(other) && _fetch == ((KWayMergeExchangeNode) other)._fetch
        && _offset == ((KWayMergeExchangeNode) other)._offset;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _fetch, _offset);
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new KWayMergeExchangeNode(getStageId(), getDataSchema(), inputs, getDistributionType(), getKeys(),
        isPrePartitioned(), getCollations(), _fetch, _offset, getHashFunction());
  }
}
