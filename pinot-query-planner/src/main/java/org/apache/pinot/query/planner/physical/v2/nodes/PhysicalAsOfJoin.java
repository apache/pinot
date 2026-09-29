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
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AsofJoin;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PinotDataDistribution;


public class PhysicalAsOfJoin extends AsofJoin implements PRelNode {
  private final int _nodeId;
  private final List<PRelNode> _pRelInputs;
  @Nullable
  private final PinotDataDistribution _pinotDataDistribution;

  public PhysicalAsOfJoin(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RexNode condition,
      RexNode matchCondition, Set<CorrelationId> variablesSet, JoinRelType joinType, int nodeId, PRelNode left,
      PRelNode right, @Nullable PinotDataDistribution pinotDataDistribution) {
    super(cluster, traitSet, hints, left.unwrap(), right.unwrap(), condition, matchCondition, variablesSet, joinType);
    _nodeId = nodeId;
    _pRelInputs = List.of(left, right);
    _pinotDataDistribution = pinotDataDistribution;
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

  @Override
  public PRelNode with(int newNodeId, List<PRelNode> newInputs, PinotDataDistribution newDistribution) {
    return new PhysicalAsOfJoin(getCluster(), getTraitSet(), getHints(), getCondition(), getMatchCondition(),
        getVariablesSet(), getJoinType(), newNodeId, newInputs.get(0), newInputs.get(1), newDistribution);
  }

  @Override
  public Join copy(RelTraitSet relTraitSet, RexNode rexNode, RelNode newLeft, RelNode newRight, JoinRelType joinRelType,
      boolean semiJoinDone) {
    return new PhysicalAsOfJoin(getCluster(), relTraitSet, getHints(), rexNode, getMatchCondition(),
        getVariablesSet(), joinRelType, _nodeId, (PRelNode) newLeft, (PRelNode) newRight, _pinotDataDistribution);
  }
}
