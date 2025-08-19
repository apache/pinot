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
  private final List<FilterProjectRex> _filterProjectRexes;
  /// Output schema of the join
  private final DataSchema _joinResultSchema;
  /// Output schema after projection, same as _joinResultSchema if no projection
  private final DataSchema _projectResultSchema;
  private final int _fetch;
  private final int _offset;


  public EnrichedJoinNode(int stageId, DataSchema joinResultSchema, DataSchema projectResultSchema,
      NodeHint nodeHint, List<PlanNode> inputs,
      JoinRelType joinType, List<Integer> leftKeys, List<Integer> rightKeys,
      List<RexExpression> nonEquiConditions, JoinStrategy joinStrategy, RexExpression matchCondition,
      List<FilterProjectRex> filterProjectRexes,
      int fetch, int offset) {
    super(stageId, projectResultSchema, nodeHint, inputs, joinType, leftKeys, rightKeys,
        nonEquiConditions, joinStrategy, matchCondition);
    _joinResultSchema = joinResultSchema;
    _projectResultSchema = projectResultSchema;
    _filterProjectRexes = filterProjectRexes;
    _fetch = fetch;
    _offset = offset;
  }

  public DataSchema getJoinResultSchema() {
    return _joinResultSchema;
  }

  /// The final output schema is project output schema since only project and join alters schema
  @Override
  public DataSchema getDataSchema() {
    return _projectResultSchema;
  }

  public List<FilterProjectRex> getFilterProjectRexes() {
    return _filterProjectRexes;
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitEnrichedJoin(this, context);
  }

  @Override
  public String explain() {
    return "ENRICHED_JOIN";
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new EnrichedJoinNode(_stageId, _joinResultSchema, _projectResultSchema, _nodeHint,
        inputs, getJoinType(), getLeftKeys(), getRightKeys(), getNonEquiConditions(),
        getJoinStrategy(), getMatchCondition(), _filterProjectRexes, _fetch, _offset);
  }

  public int getFetch() {
    return _fetch;
  }

  public int getOffset() {
    return _offset;
  }

  public enum FilterProjectRexType {
    FILTER,
    PROJECT
  }

  public static class FilterProjectRex {

    public static class ProjectAndResultSchema {
      private final List<RexExpression> _project;
      private final DataSchema _schema;

      private ProjectAndResultSchema(List<RexExpression> project, DataSchema resultSchema) {
        _project = project;
        _schema = resultSchema;
      }

      public List<RexExpression> getProject() {
        return _project;
      }

      public DataSchema getSchema() {
        return _schema;
      }
    }

    private final FilterProjectRexType _type;
    @Nullable
    private final RexExpression _filter;
    @Nullable
    private final ProjectAndResultSchema _projectAndResultSchema;

    public FilterProjectRex(RexExpression filter) {
      _type = FilterProjectRexType.FILTER;
      _filter = filter;
      _projectAndResultSchema = null;
    }

    public FilterProjectRex(List<RexExpression> projects, DataSchema resultSchema) {
      _type = FilterProjectRexType.PROJECT;
      _filter = null;
      _projectAndResultSchema = new ProjectAndResultSchema(projects, resultSchema);
    }

    @Nullable
    public RexExpression getFilter() {
      return _filter;
    }

    @Nullable
    public ProjectAndResultSchema getProjectAndResultSchema() {
      return _projectAndResultSchema;
    }

    public FilterProjectRexType getType() {
      return _type;
    }

    @Override
    public String toString() {
      if (_type == FilterProjectRexType.FILTER) {
        assert _filter != null;
        return "Filter: " + _filter;
      } else {
        assert _projectAndResultSchema != null;
        return "Project: " + _projectAndResultSchema.getProject().toString();
      }
    }
  }
}
