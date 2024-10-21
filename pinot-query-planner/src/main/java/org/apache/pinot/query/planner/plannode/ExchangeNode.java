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
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.PlanFragmenter;


/**
 * ExchangeNode represents the exchange stage in the query plan.
 * It is used to exchange the data between the instances.
 * NOTE: ExchangeNode will be replaced by {@link PlanFragmenter} into {@link MailboxSendNode} and
 *       {@link MailboxReceiveNode} pair, so it is never serialized.
 */
public class ExchangeNode extends BasePlanNode {
  private final PinotRelExchangeType _exchangeType;
  private final RelDistribution.Type _distributionType;
  private final List<Integer> _keys;
  private final boolean _prePartitioned;
  private final List<RelFieldCollation> _collations;
  private final boolean _sortOnSender;
  private final boolean _sortOnReceiver;
  // Table names should be set for SUB_PLAN exchange type.
  private final Set<String> _tableNames;

  public ExchangeNode(int stageId, DataSchema dataSchema, List<PlanNode> inputs, PinotRelExchangeType exchangeType,
      RelDistribution.Type distributionType, @Nullable List<Integer> keys, boolean prePartitioned,
      @Nullable List<RelFieldCollation> collations, boolean sortOnSender, boolean sortOnReceiver,
      @Nullable Set<String> tableNames) {
    super(stageId, dataSchema, null, inputs);
    _exchangeType = exchangeType;
    _distributionType = distributionType;
    _keys = keys;
    _prePartitioned = prePartitioned;
    _collations = collations;
    _sortOnSender = sortOnSender;
    _sortOnReceiver = sortOnReceiver;
    _tableNames = tableNames;
  }

  public PinotRelExchangeType getExchangeType() {
    return _exchangeType;
  }

  public RelDistribution.Type getDistributionType() {
    return _distributionType;
  }

  @Nullable
  public List<Integer> getKeys() {
    return _keys;
  }

  public boolean isPrePartitioned() {
    return _prePartitioned;
  }

  @Nullable
  public List<RelFieldCollation> getCollations() {
    return _collations;
  }

  public boolean isSortOnSender() {
    return _sortOnSender;
  }

  public boolean isSortOnReceiver() {
    return _sortOnReceiver;
  }

  @Nullable
  public Set<String> getTableNames() {
    return _tableNames;
  }

  @Override
  public String explain() {
    return "EXCHANGE";
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitExchange(this, context);
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new ExchangeNode(_stageId, _dataSchema, inputs, _exchangeType, _distributionType, _keys, _prePartitioned,
        _collations, _sortOnSender, _sortOnReceiver, _tableNames);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ExchangeNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    ExchangeNode that = (ExchangeNode) o;
    return _sortOnSender == that._sortOnSender && _sortOnReceiver == that._sortOnReceiver
        && _prePartitioned == that._prePartitioned && _exchangeType == that._exchangeType
        && _distributionType == that._distributionType && Objects.equals(_keys, that._keys) && Objects.equals(
        _collations, that._collations) && Objects.equals(_tableNames, that._tableNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _exchangeType, _distributionType, _keys, _sortOnSender, _sortOnReceiver,
        _prePartitioned, _collations, _tableNames);
  }
}
