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

import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.pinot.calcite.rel.logical.PinotLogicalAggregate;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PinotDataDistribution;
import org.apache.pinot.query.planner.plannode.AggregateNode;


public class PhysicalAggregate extends Aggregate implements PRelNode {
  private final int _nodeId;
  private final List<PRelNode> _pRelInputs;
  @Nullable
  private final PinotDataDistribution _pinotDataDistribution;
  private final boolean _leafStage;
  /*
   * Pinot related aggregation information follows.
   */
  private final AggregateNode.AggType _aggType;
  private final boolean _leafReturnFinalResult;
  // The following fields are set when group trim is enabled, and are extracted from the Sort on top of this Aggregate.
  private final List<RelFieldCollation> _collations;
  private final int _limit;

  public PhysicalAggregate(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints,
      ImmutableBitSet groupSet, @Nullable List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls,
      int nodeId, PRelNode pRelInput, @Nullable PinotDataDistribution pinotDataDistribution, boolean leafStage,
      AggregateNode.AggType aggType, boolean leafReturnFinalResult, List<RelFieldCollation> collations,
      int limit) {
    super(cluster, traitSet, hints, pRelInput.unwrap(), groupSet, groupSets, aggCalls);
    _nodeId = nodeId;
    _pRelInputs = List.of(pRelInput);
    _pinotDataDistribution = pinotDataDistribution;
    _leafStage = leafStage;
    _aggType = aggType;
    _leafReturnFinalResult = leafReturnFinalResult;
    _collations = collations;
    _limit = limit;
  }

  @Override
  public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    return new PhysicalAggregate(getCluster(), traitSet, getHints(), groupSet, groupSets, aggCalls, _nodeId,
        (PRelNode) input, _pinotDataDistribution, _leafStage, _aggType, _leafReturnFinalResult, _collations, _limit);
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
    return _leafStage;
  }

  @Override
  public PRelNode with(int newNodeId, List<PRelNode> newInputs, PinotDataDistribution newDistribution) {
    return new PhysicalAggregate(getCluster(), getTraitSet(), getHints(), getGroupSet(), getGroupSets(),
        getAggCallList(), newNodeId, newInputs.get(0), newDistribution, _leafStage, _aggType, _leafReturnFinalResult,
        _collations, _limit);
  }

  @Override
  public PhysicalAggregate asLeafStage() {
    if (isLeafStage()) {
      return this;
    }
    return new PhysicalAggregate(getCluster(), getTraitSet(), getHints(), getGroupSet(), getGroupSets(),
        getAggCallList(), _nodeId, _pRelInputs.get(0), _pinotDataDistribution, true, _aggType, _leafReturnFinalResult,
        _collations, _limit);
  }
}
