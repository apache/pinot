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
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Exchange;


/**
 * Pinot's implementation of {@link Exchange} which needs information about whether to exchange is
 * done on a streaming or a pipeline-breaking fashion.
 */
public class PinotLogicalExchange extends Exchange {
  private final PinotRelExchangeType _exchangeType;

  private PinotLogicalExchange(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, RelDistribution distribution, PinotRelExchangeType exchangeType) {
    super(cluster, traitSet, input, distribution);
    _exchangeType = exchangeType;
    assert traitSet.containsIfApplicable(Convention.NONE);
  }


  public static PinotLogicalExchange create(RelNode input,
      RelDistribution distribution) {
    return create(input, distribution, PinotRelExchangeType.getDefaultExchangeType());
  }

  /**
   * Creates a LogicalExchange.
   *
   * @param input     Input relational expression
   * @param distribution Distribution specification
   * @param exchangeType RelExchangeType specification
   */
  public static PinotLogicalExchange create(RelNode input,
      RelDistribution distribution, PinotRelExchangeType exchangeType) {
    RelOptCluster cluster = input.getCluster();
    distribution = RelDistributionTraitDef.INSTANCE.canonize(distribution);
    RelTraitSet traitSet =
        input.getTraitSet().replace(Convention.NONE).replace(distribution);
    return new PinotLogicalExchange(cluster, traitSet, input, distribution, exchangeType);
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public Exchange copy(RelTraitSet traitSet, RelNode newInput,
      RelDistribution newDistribution) {
    return new PinotLogicalExchange(getCluster(), traitSet, newInput,
        newDistribution, _exchangeType);
  }

  @Override
  public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    RelWriter relWriter = super.explainTerms(pw);
    if (_exchangeType != PinotRelExchangeType.getDefaultExchangeType()) {
      relWriter.item("relExchangeType", _exchangeType);
    }
    return relWriter;
  }

  public PinotRelExchangeType getExchangeType() {
    return _exchangeType;
  }
}
