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
package org.apache.pinot.query.planner.plannode;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;


public class EnrichedJoinNode extends JoinNode {
  // TODO: add filter, project, sort, limit info here
  @Nullable
  private final RexExpression _filterCondition;
  @Nullable
  private final List<RexExpression> _projects;
  // output schema of the join
  private final DataSchema _joinResultSchema;
  // output schema after projection, same as _joinResultSchema if no projection
  private final DataSchema _projectResultSchema;

  public EnrichedJoinNode(int stageId, DataSchema joinResultSchema, DataSchema projectResultSchema,
      NodeHint nodeHint, List<PlanNode> inputs,
      JoinRelType joinType, List<Integer> leftKeys, List<Integer> rightKeys,
      List<RexExpression> nonEquiConditions, JoinStrategy joinStrategy, RexExpression matchCondition,
      RexExpression filterCondition, List<RexExpression> projects) {
    super(stageId, projectResultSchema, nodeHint, inputs, joinType, leftKeys, rightKeys,
        nonEquiConditions, joinStrategy, matchCondition);
    _joinResultSchema = joinResultSchema;
    _projectResultSchema = projectResultSchema;

    _filterCondition = filterCondition;
    _projects = projects;
  }

  public DataSchema getJoinResultSchema() {
    return _joinResultSchema;
  }

  public DataSchema getProjectResultSchema() {
    return _projectResultSchema;
  }

  @Override
  // the final output schema is project output schema since only project and join alters schema
  public DataSchema getDataSchema() {
    return _projectResultSchema;
  }

  @Nullable
  public RexExpression getFilterCondition() {
    return _filterCondition;
  }

  @Nullable
  public List<RexExpression> getProjects() {
    return _projects;
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitEnrichedJoin(this, context);
  }

  @Override
  public String explain() {
    return "ENRICHED_JOIN"
        + (_filterCondition == null ? " WITH FILTER" : "")
        + (_projects == null ? " WITH PROJECT" : "");
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new EnrichedJoinNode(_stageId, _joinResultSchema, _projectResultSchema, _nodeHint,
        inputs, getJoinType(), getLeftKeys(), getRightKeys(), getNonEquiConditions(),
        getJoinStrategy(), getMatchCondition(), _filterCondition, _projects);
  }
}
