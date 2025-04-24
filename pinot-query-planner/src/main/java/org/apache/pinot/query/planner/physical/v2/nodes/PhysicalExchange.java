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
package org.apache.pinot.query.planner.physical.v2.nodes;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Exchange;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.calcite.rel.traits.PinotExecStrategyTrait;
import org.apache.pinot.calcite.rel.traits.PinotExecStrategyTraitDef;
import org.apache.pinot.query.planner.physical.v2.ExchangeStrategy;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PinotDataDistribution;


/**
 * Exchange is a special plan node because it is often used to change the number of streams. Since we track
 * {@link PinotDataDistribution} at node level, the question is whether we should assign this so that number of streams
 * is equal to the sender or the receiver.
 * <p>
 *   We have chosen to set it based on the receiver. The idea being that after PhysicalExchange is added, the trait
 *   constraints should ideally be satisfied between the receiver and the Exchange node. This is similar to how Calcite
 *   thinks of trait enforcement via Converter Rules.
 * </p>
 */
public class PhysicalExchange extends Exchange implements PRelNode {
  /**
   * Physical Exchange does not support adding traits. To store ordering and distribution details, we use other
   * variables and don't allow RelDistribution or RelCollation to be stored in the trait set. The idea being that we
   * should avoid duplicate storage of the same information, and that we use traits as constraints which are added
   * before any Exchange nodes are added to the plan.
   */
  private static final RelTraitSet EMPTY_TRAIT_SET = RelTraitSet.createEmpty();
  private final int _nodeId;
  private final List<PRelNode> _pRelInputs;
  /**
   * See javadocs for {@link PhysicalExchange}.
   */
  @Nullable
  private final PinotDataDistribution _pinotDataDistribution;
  /*
   * Exchange related metadata below.
   */
  /**
   * Which keys are used to re-distribute data. This may be empty if the data distribution is independent of record
   * values (e.g. identity exchange, singleton, broadcast, etc.)
   */
  private final List<Integer> _distributionKeys;
  private final ExchangeStrategy _exchangeStrategy;
  /**
   * When not empty, records in each output stream will be sorted by the ordering defined by this collation.
   */
  private final RelCollation _relCollation;

  public PhysicalExchange(int nodeId, PRelNode input, @Nullable PinotDataDistribution pinotDataDistribution,
      List<Integer> distributionKeys, ExchangeStrategy exchangeStrategy, @Nullable RelCollation relCollation,
      PinotExecStrategyTrait execStrategyTrait) {
    super(input.unwrap().getCluster(), EMPTY_TRAIT_SET.plus(execStrategyTrait), input.unwrap(),
        getRelDistribution(exchangeStrategy, distributionKeys));
    _nodeId = nodeId;
    _pRelInputs = Collections.singletonList(input);
    _pinotDataDistribution = pinotDataDistribution;
    _distributionKeys = distributionKeys;
    _exchangeStrategy = exchangeStrategy;
    _relCollation = relCollation == null ? RelCollations.EMPTY : relCollation;
  }

  @Override
  public Exchange copy(RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution) {
    Preconditions.checkState(newInput instanceof PRelNode, "Expected input of PhysicalExchange to be a PRelNode");
    Preconditions.checkState(traitSet.isEmpty(), "Expected empty trait set for PhysicalExchange");
    return new PhysicalExchange(_nodeId, (PRelNode) newInput, _pinotDataDistribution, _distributionKeys,
        _exchangeStrategy, _relCollation, PinotExecStrategyTrait.getDefaultExecStrategy());
  }

  @Override
  public int getNodeId() {
    return _nodeId;
  }

  @Override
  public List<PRelNode> getPRelInputs() {
    return _pRelInputs;
  }

  @Override
  public RelNode unwrap() {
    return this;
  }

  @Nullable
  @Override
  public PinotDataDistribution getPinotDataDistribution() {
    return _pinotDataDistribution;
  }

  @Override
  public boolean isLeafStage() {
    return false;
  }

  public List<Integer> getDistributionKeys() {
    return _distributionKeys;
  }

  public ExchangeStrategy getExchangeStrategy() {
    return _exchangeStrategy;
  }

  public RelCollation getRelCollation() {
    return _relCollation;
  }

  public PinotExecStrategyTrait getExecStrategy() {
    PinotExecStrategyTrait trait = traitSet.getTrait(PinotExecStrategyTraitDef.INSTANCE);
    if (trait == null) {
      return PinotExecStrategyTrait.getDefaultExecStrategy();
    }
    return trait;
  }

  public PinotRelExchangeType getRelExchangeType() {
    PinotExecStrategyTrait trait = traitSet.getTrait(PinotExecStrategyTraitDef.INSTANCE);
    if (trait == null) {
      return PinotExecStrategyTrait.getDefaultExecStrategy().getType();
    }
    return trait.getType();
  }

  @Override
  public PRelNode with(int newNodeId, List<PRelNode> newInputs, PinotDataDistribution newDistribution) {
    return new PhysicalExchange(newNodeId, newInputs.get(0), newDistribution, _distributionKeys, _exchangeStrategy,
        _relCollation, PinotExecStrategyTrait.getDefaultExecStrategy());
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return pw.item("input", input)
        .item("exchangeStrategy", _exchangeStrategy)
        .item("distKeys", _distributionKeys)
        .item("execStrategy", getRelExchangeType())
        .item("collation", _relCollation);
  }

  private static RelDistribution getRelDistribution(ExchangeStrategy exchangeStrategy, List<Integer> keys) {
    switch (exchangeStrategy) {
      case IDENTITY_EXCHANGE:
      case PARTITIONING_EXCHANGE:
      case SUB_PARTITIONING_HASH_EXCHANGE:
      case COALESCING_PARTITIONING_EXCHANGE:
        return RelDistributions.hash(keys);
      case BROADCAST_EXCHANGE:
        return RelDistributions.BROADCAST_DISTRIBUTED;
      case SINGLETON_EXCHANGE:
        return RelDistributions.SINGLETON;
      case SUB_PARTITIONING_RR_EXCHANGE:
        return RelDistributions.ROUND_ROBIN_DISTRIBUTED;
      default:
        throw new IllegalStateException(String.format("Unexpected exchange strategy: %s", exchangeStrategy));
    }
  }
}
