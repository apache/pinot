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
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import static com.google.common.base.Preconditions.checkNotNull;


public class PinotLogicalEnrichedJoin extends Join {
  private final RelDataType _joinRowType;
  private final RelDataType _projectedRowType;
  @Nullable
  private final RexNode _filter;
  @Nullable
  private final List<RexNode> _projects;
  @Nullable
  private final RelCollation _collation;
  @Nullable
  private final RexNode _fetch;
  @Nullable
  private final RexNode _offset;
  // currently variableSet of Project Rel is ignored since this EnrichedJoinRel
  //   is created at the end of logical transformations.
  //   We don't support nested expressions in execution
  private final Set<CorrelationId> _projectVariableSet;

  public PinotLogicalEnrichedJoin(RelOptCluster cluster, RelTraitSet traitSet,
      List<RelHint> hints, RelNode left, RelNode right, RexNode joinCondition,
      Set<CorrelationId> variablesSet, JoinRelType joinType,
      @Nullable RexNode filter, @Nullable List<RexNode> projects,
      @Nullable RelDataType projectRowType, @Nullable Set<CorrelationId> projectVariableSet,
      @Nullable RelCollation collation, @Nullable RexNode fetch, @Nullable RexNode offset) {
    super(cluster, traitSet, hints, left, right, joinCondition, variablesSet, joinType);
    _filter = filter;
    _projects = projects;
    _joinRowType = getJoinRowType();
    // if there's projection, getRowType() should return projected row type as output row type
    //   otherwise it's the same as _joinRowType
    _projectedRowType = projectRowType == null ? _joinRowType : projectRowType;
    _projectVariableSet = projectVariableSet;
    _collation = collation;
    _fetch = fetch;
    _offset = offset;
  }

  @Override
  public PinotLogicalEnrichedJoin copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType,
      boolean semiJoinDone) {
    return new PinotLogicalEnrichedJoin(getCluster(), traitSet, getHints(), left, right, conditionExpr, getVariablesSet(), getJoinType(),
        _filter, _projects, _projectedRowType, _projectVariableSet, _collation, _fetch, _offset);
  }

  @Override protected RelDataType deriveRowType() {
    return checkNotNull(_projectedRowType);
  }

  public final RelDataType getJoinRowType() {
    return SqlValidatorUtil.deriveJoinRowType(left.getRowType(),
        right.getRowType(), joinType, getCluster().getTypeFactory(), null,
        getSystemFieldList());
  }

  @Nullable
  public RexNode getFilter() {
    return _filter;
  }

  @Nullable
  public List<RexNode> getProjects() {
    return _projects;
  }

  @Nullable
  public RelCollation getCollation() { return _collation; }

  @Nullable
  public RexNode getFetch() {
    return _fetch;
  }

  @Nullable
  public RexNode getOffset() {
    return _offset;
  }
}
