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
import java.util.Set;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.serde.ProtoProperties;


/**
 * ExchangeNode represents the exchange stage in the query plan.
 * It is used to exchange the data between the instances.
 */
public class ExchangeNode extends AbstractPlanNode {

  @ProtoProperties
  private PinotRelExchangeType _exchangeType;

  @ProtoProperties
  private RelDistribution.Type _distributionType;

  @ProtoProperties
  private List<Integer> _keys;

  @ProtoProperties
  private boolean _isSortOnSender = false;

  @ProtoProperties
  private boolean _isSortOnReceiver = false;

  @ProtoProperties
  private List<RelFieldCollation> _collations;

  /**
   * The set of tables that are scanned in this planFragment.
   */
  @ProtoProperties
  private Set<String> _tableNames;

  public ExchangeNode(int planFragmentId) {
    super(planFragmentId);
  }

  public ExchangeNode(int currentStageId, DataSchema dataSchema, PinotRelExchangeType exchangeType,
      Set<String> tableNames, RelDistribution distribution, List<RelFieldCollation> collations, boolean isSortOnSender,
      boolean isSortOnReceiver) {
    super(currentStageId, dataSchema);
    _exchangeType = exchangeType;
    _keys = distribution.getKeys();
    _distributionType = distribution.getType();
    _isSortOnSender = isSortOnSender;
    _isSortOnReceiver = isSortOnReceiver;
    _collations = collations;
    _tableNames = tableNames;
  }

  @Override
  public String explain() {
    return "EXCHANGE";
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitExchange(this, context);
  }

  public PinotRelExchangeType getExchangeType() {
    return _exchangeType;
  }

  public RelDistribution.Type getDistributionType() {
    return _distributionType;
  }

  public List<Integer> getDistributionKeys() {
    return _keys;
  }

  public boolean isSortOnSender() {
    return _isSortOnSender;
  }

  public boolean isSortOnReceiver() {
    return _isSortOnReceiver;
  }

  public List<RelFieldCollation> getCollations() {
    return _collations;
  }

  public Set<String> getTableNames() {
    return _tableNames;
  }
}
