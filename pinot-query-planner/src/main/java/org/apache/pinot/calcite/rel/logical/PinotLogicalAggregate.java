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

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.checkerframework.checker.nullness.qual.Nullable;


public class PinotLogicalAggregate extends Aggregate {
  private final AggType _aggType;

  public PinotLogicalAggregate(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelNode input,
      ImmutableBitSet groupSet, @Nullable List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls,
      AggType aggType) {
    super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
    _aggType = aggType;
  }

  public PinotLogicalAggregate(Aggregate aggRel, List<AggregateCall> aggCalls, AggType aggType) {
    this(aggRel.getCluster(), aggRel.getTraitSet(), aggRel.getHints(), aggRel.getInput(), aggRel.getGroupSet(),
        aggRel.getGroupSets(), aggCalls, aggType);
  }

  public PinotLogicalAggregate(Aggregate aggRel, RelNode input, List<AggregateCall> aggCalls, AggType aggType) {
    this(aggRel.getCluster(), aggRel.getTraitSet(), aggRel.getHints(), input, aggRel.getGroupSet(),
        aggRel.getGroupSets(), aggCalls, aggType);
  }

  public PinotLogicalAggregate(Aggregate aggRel, RelNode input, ImmutableBitSet groupSet, List<AggregateCall> aggCalls,
      AggType aggType) {
    this(aggRel.getCluster(), aggRel.getTraitSet(), aggRel.getHints(), input, groupSet, null, aggCalls, aggType);
  }

  public AggType getAggType() {
    return _aggType;
  }

  @Override
  public PinotLogicalAggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    return new PinotLogicalAggregate(getCluster(), traitSet, hints, input, groupSet, groupSets, aggCalls, _aggType);
  }

  @Override
  public RelNode withHints(List<RelHint> hintList) {
    return new PinotLogicalAggregate(getCluster(), traitSet, hintList, input, groupSet, groupSets, aggCalls, _aggType);
  }
}
