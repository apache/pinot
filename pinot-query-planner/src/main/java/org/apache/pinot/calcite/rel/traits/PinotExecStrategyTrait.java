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
package org.apache.pinot.calcite.rel.traits;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;


/**
 * Execution strategy defines how a sub-tree of the plan will be executed by Pinot. There are three strategies:
 * <ol>
 *   <li>Streaming: This is the default strategy that indicates the operator will emit data in chunks until EOS.</li>
 *   <li>
 *     Pipeline Breaker: This indicates that Pinot Server will consume the entire output of this operator, before
 *     it compiles the plan for the rest of the Plan Fragment.
 *   </li>
 *   <li>
 *     Sub Plan: This indicates that Pinot Broker should execute the entire plan under this operator first, and then
 *     continue with planning the rest of the plan tree. Usually, you would get a constant out of the Sub-Plan, which
 *     can be put back in the original plan tree.
 *   </li>
 * </ol>
 */
public class PinotExecStrategyTrait implements RelTrait {
  public static final PinotExecStrategyTrait STREAMING = new PinotExecStrategyTrait(PinotRelExchangeType.STREAMING);
  public static final PinotExecStrategyTrait PIPELINE_BREAKER = new PinotExecStrategyTrait(
      PinotRelExchangeType.PIPELINE_BREAKER);
  public static final PinotExecStrategyTrait SUB_PLAN = new PinotExecStrategyTrait(PinotRelExchangeType.SUB_PLAN);

  /**
   * <b>Implementation Note:</b> We use {@link PinotRelExchangeType} in this trait because Pinot Runtime uses that
   * enum to determine the execution strategy in the dispatched plan to the server, and we can't change it now due to
   * b/w compatibility. Ideally we would have liked to introduce a new Enum in this trait, similar to
   * {@link org.apache.calcite.rel.RelDistribution}.
   */
  private final PinotRelExchangeType _type;

  PinotExecStrategyTrait(PinotRelExchangeType type) {
    _type = type;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public RelTraitDef getTraitDef() {
    return PinotExecStrategyTraitDef.INSTANCE;
  }

  @Override
  public boolean satisfies(RelTrait trait) {
    return trait.getTraitDef() == getTraitDef() && ((PinotExecStrategyTrait) trait)._type == _type;
  }

  @Override
  public void register(RelOptPlanner planner) {
  }

  public PinotRelExchangeType getType() {
    return _type;
  }

  public static PinotExecStrategyTrait getDefaultExecStrategy() {
    return STREAMING;
  }
}
