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
package org.apache.pinot.calcite.rel.logical;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.pinot.calcite.rel.ExchangeStrategy;


public class PinotPhysicalExchange extends Exchange {
  private static final RelTraitSet FIXED_TRAIT_SET = RelTraitSet.createEmpty().plus(RelDistributions.ANY);
  /** The key indexes used for performing the exchange. */
  private final List<Integer> _keys;
  /** Defines how records are distributed from streams in the sending operator to streams in the receiver. */
  private final ExchangeStrategy _exchangeStrategy;
  /** Whether each output stream needs to have an ordering defined. */
  private final RelCollation _collation;

  public PinotPhysicalExchange(RelNode input, List<Integer> keys, ExchangeStrategy exchangeStrategy) {
    this(input, keys, exchangeStrategy, null);
  }

  public PinotPhysicalExchange(RelNode input, List<Integer> keys, ExchangeStrategy desc,
      RelCollation collation) {
    super(input.getCluster(), FIXED_TRAIT_SET, input, RelDistributions.ANY);
    _keys = keys;
    _exchangeStrategy = desc;
    _collation = collation == null ? RelCollations.EMPTY : collation;
  }

  public static PinotPhysicalExchange broadcast(RelNode input) {
    return new PinotPhysicalExchange(input, Collections.emptyList(), ExchangeStrategy.BROADCAST_EXCHANGE,
        null);
  }

  public static PinotPhysicalExchange singleton(RelNode input) {
    return new PinotPhysicalExchange(input, Collections.emptyList(), ExchangeStrategy.SINGLETON_EXCHANGE,
        null);
  }

  @Override
  public Exchange copy(RelTraitSet traitSet, RelNode newInput, RelDistribution newDistributionIgnored) {
    Preconditions.checkState(newDistributionIgnored.equals(RelDistributions.ANY),
        "Exchange should always have ANY trait, because we use ExchangeStrategy instead.");
    Preconditions.checkState(traitSet.size() <= 1, "At most 1 trait allowed in PinotPhysicalExchange");
    Preconditions.checkState(traitSet.isEmpty() || traitSet.getDistribution() != null,
        "Only distribution trait allowed in PinotPhysicalExchange");
    return new PinotPhysicalExchange(newInput, _keys, _exchangeStrategy, _collation);
  }

  public List<Integer> getKeys() {
    return _keys;
  }

  public ExchangeStrategy getExchangeStrategy() {
    return _exchangeStrategy;
  }

  public RelCollation getCollation() {
    return _collation;
  }

  @Override
  public String getRelTypeName() {
    return String.format("PinotPhysicalExchange(strategy=%s, keys=%s, collation=%s)", _exchangeStrategy, _keys,
        _collation);
  }
}
