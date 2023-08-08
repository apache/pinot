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
package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.SortExchange;


/**
 * Pinot's implementation of {@code SortExchange} which needs information about whether to sort on the sender
 * and/or receiver side of the exchange. Every {@code Exchange} is broken into a send and a receive node and the
 * decision on where to sort is made by the planner and this information has to b passed onto the send and receive
 * nodes for the correct execution.
 *
 * Note: This class does not extend {@code LogicalSortExchange} because its constructor which takes the list of
 * parameters is private.
 */
public class PinotLogicalSortExchange extends SortExchange {

  protected final boolean _isSortOnSender;
  protected final boolean _isSortOnReceiver;
  protected final PinotRelExchangeType _exchangeType;

  private PinotLogicalSortExchange(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, RelDistribution distribution, PinotRelExchangeType exchangeType, RelCollation collation,
      boolean isSortOnSender, boolean isSortOnReceiver) {
    super(cluster, traitSet, input, distribution, collation);
    _exchangeType = exchangeType;
    _isSortOnSender = isSortOnSender;
    _isSortOnReceiver = isSortOnReceiver;
  }

  /**
   * Creates a PinotLogicalSortExchange by parsing serialized output.
   */
  public PinotLogicalSortExchange(RelInput input) {
    super(input);
    _exchangeType = PinotRelExchangeType.STREAMING;
    _isSortOnSender = false;
    _isSortOnReceiver = true;
  }

  public static PinotLogicalSortExchange create(
      RelNode input,
      RelDistribution distribution,
      RelCollation collation,
      boolean isSortOnSender,
      boolean isSortOnReceiver) {
    return create(input, distribution, PinotRelExchangeType.getDefaultExchangeType(), collation, isSortOnSender,
        isSortOnReceiver);
  }

  /**
   * Creates a PinotLogicalSortExchange.
   *
   * @param input     Input relational expression
   * @param distribution Distribution specification
   * @param exchangeType Exchange type specification
   * @param collation array of sort specifications
   * @param isSortOnSender whether to sort on the sender
   * @param isSortOnReceiver whether to sort on receiver
   */
  public static PinotLogicalSortExchange create(
      RelNode input,
      RelDistribution distribution,
      PinotRelExchangeType exchangeType,
      RelCollation collation,
      boolean isSortOnSender,
      boolean isSortOnReceiver) {
    RelOptCluster cluster = input.getCluster();
    collation = RelCollationTraitDef.INSTANCE.canonize(collation);
    distribution = RelDistributionTraitDef.INSTANCE.canonize(distribution);
    RelTraitSet traitSet =
        input.getTraitSet().replace(Convention.NONE).replace(distribution).replace(collation);
    return new PinotLogicalSortExchange(cluster, traitSet, input, distribution, exchangeType,
        collation, isSortOnSender, isSortOnReceiver);
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public SortExchange copy(RelTraitSet traitSet, RelNode newInput,
      RelDistribution newDistribution, RelCollation newCollation) {
    return new PinotLogicalSortExchange(this.getCluster(), traitSet, newInput,
        newDistribution, _exchangeType, newCollation, _isSortOnSender, _isSortOnReceiver);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    RelWriter relWriter = super.explainTerms(pw)
        .item("isSortOnSender", _isSortOnSender)
        .item("isSortOnReceiver", _isSortOnReceiver);
    if (_exchangeType != PinotRelExchangeType.getDefaultExchangeType()) {
      relWriter.item("relExchangeType", _exchangeType);
    }
    return relWriter;
  }

  public boolean isSortOnSender() {
    return _isSortOnSender;
  }

  public boolean isSortOnReceiver() {
    return _isSortOnReceiver;
  }

  public PinotRelExchangeType getExchangeType() {
    return _exchangeType;
  }
}
