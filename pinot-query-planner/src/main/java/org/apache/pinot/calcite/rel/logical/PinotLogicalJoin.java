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
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.pinot.query.planner.plannode.JoinNode.JoinStrategy;


/**
 * Pinot's implementation of {@link Join} with extra information on join strategy.
 */
public class PinotLogicalJoin extends Join {
  private final boolean _semiJoinDone;
  private final List<RelDataTypeField> _systemFieldList;
  @Nullable
  private final JoinStrategy _joinStrategy;

  public PinotLogicalJoin(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelNode left,
      RelNode right, RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType, boolean semiJoinDone,
      List<RelDataTypeField> systemFieldList, @Nullable JoinStrategy joinStrategy) {
    super(cluster, traitSet, hints, left, right, condition, variablesSet, joinType);
    _semiJoinDone = semiJoinDone;
    _systemFieldList = systemFieldList;
    _joinStrategy = joinStrategy;
  }

  @Nullable
  public JoinStrategy getJoinStrategy() {
    return _joinStrategy;
  }

  @Override
  public PinotLogicalJoin copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right,
      JoinRelType joinType, boolean semiJoinDone) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new PinotLogicalJoin(getCluster(), getCluster().traitSetOf(Convention.NONE), hints, left, right, condition,
        variablesSet, joinType, semiJoinDone, _systemFieldList, _joinStrategy);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).itemIf("semiJoinDone", _semiJoinDone, _semiJoinDone)
        .itemIf("joinStrategy", _joinStrategy, _joinStrategy != null);
  }

  @Override
  public boolean deepEquals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof PinotLogicalJoin)) {
      return false;
    }
    PinotLogicalJoin that = (PinotLogicalJoin) obj;
    return deepEquals0(that) && _semiJoinDone == that._semiJoinDone && _systemFieldList.equals(that._systemFieldList)
        && _joinStrategy == that._joinStrategy;
  }

  @Override
  public int deepHashCode() {
    return Objects.hash(deepHashCode0(), _semiJoinDone, _systemFieldList, _joinStrategy);
  }

  @Override
  public boolean isSemiJoinDone() {
    return _semiJoinDone;
  }

  @Override
  public List<RelDataTypeField> getSystemFieldList() {
    return _systemFieldList;
  }

  @Override
  public RelNode withHints(List<RelHint> hintList) {
    return new PinotLogicalJoin(getCluster(), traitSet, hintList, left, right, condition, variablesSet, joinType,
        _semiJoinDone, _systemFieldList, _joinStrategy);
  }
}
