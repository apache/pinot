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
import org.apache.pinot.common.utils.DataSchema;


/// Planning-only exchange requiring ordered inputs and producing a k-way merge receive and a plain send.
/// Instances are local to a query's planning thread and are never serialized.
public class KWayMergeExchangeNode extends BasePlanNode {
  private final RelDistribution.Type _distributionType;
  private final List<Integer> _keys;
  private final boolean _prePartitioned;
  private final List<RelFieldCollation> _collations;
  private final String _hashFunction;

  public KWayMergeExchangeNode(int stageId, DataSchema schema, List<PlanNode> inputs,
      RelDistribution.Type distribution, @Nullable List<Integer> keys, boolean prePartitioned,
      List<RelFieldCollation> collations, String hashFunction) {
    super(stageId, schema, null, inputs);
    _distributionType = distribution;
    _keys = keys != null ? keys : List.of();
    _prePartitioned = prePartitioned;
    _collations = collations;
    _hashFunction = hashFunction;
  }

  public RelDistribution.Type getDistributionType() {
    return _distributionType;
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

  public String getHashFunction() {
    return _hashFunction;
  }

  @Override
  public String explain() {
    return "K_WAY_MERGE_EXCHANGE";
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitKWayMergeExchange(this, context);
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new KWayMergeExchangeNode(getStageId(), getDataSchema(), inputs, getDistributionType(), getKeys(),
        isPrePartitioned(), getCollations(), getHashFunction());
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass() || !super.equals(other)) {
      return false;
    }
    KWayMergeExchangeNode that = (KWayMergeExchangeNode) other;
    return _distributionType == that._distributionType && _keys.equals(that._keys)
        && _prePartitioned == that._prePartitioned && _collations.equals(that._collations)
        && Objects.equals(_hashFunction, that._hashFunction);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _distributionType, _keys, _prePartitioned, _collations, _hashFunction);
  }
}
