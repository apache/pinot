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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import static com.google.common.base.Preconditions.checkNotNull;


public class PinotLogicalEnrichedJoin extends Join {

  private final RelDataType _outputRowType;
  private final List<FilterProjectRexNode> _filterProjectRexNodes;
  /**
   * Currently variableSet of Project Rel is ignored since
   * we don't support nested expressions in execution
   */
  private final Set<CorrelationId> _projectVariableSet;
  @Nullable
  private final List<RexNode> _squashedProjects;
  @Nullable
  private final RexNode _fetch;
  @Nullable
  private final RexNode _offset;

  public PinotLogicalEnrichedJoin(RelOptCluster cluster, RelTraitSet traitSet,
      List<RelHint> hints, RelNode left, RelNode right, RexNode joinCondition,
      Set<CorrelationId> variablesSet, JoinRelType joinType,
      List<FilterProjectRexNode> filterProjectRexNodes,
      @Nullable RelDataType outputRowType, @Nullable Set<CorrelationId> projectVariableSet,
      @Nullable RexNode fetch, @Nullable RexNode offset) {
    super(cluster, traitSet, hints, left, right, joinCondition, variablesSet, joinType);
    _filterProjectRexNodes = filterProjectRexNodes;
    _squashedProjects = squashProjects();
    RelDataType joinRowType = getJoinRowType();
    // if there's projection, getRowType() should return the final projected row type as output row type
    //   otherwise it's the same as _joinRowType
    _outputRowType = outputRowType == null ? joinRowType : outputRowType;
    _projectVariableSet = projectVariableSet;
    _offset = offset;
    _fetch = fetch;
  }

  @Override
  public PinotLogicalEnrichedJoin copy(RelTraitSet traitSet, RexNode conditionExpr,
      RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    return new PinotLogicalEnrichedJoin(getCluster(), traitSet, getHints(), left, right,
        conditionExpr, getVariablesSet(), getJoinType(),
        _filterProjectRexNodes, _outputRowType, _projectVariableSet,
        _fetch, _offset);
  }

  public PinotLogicalEnrichedJoin withNewProject(FilterProjectRexNode project, RelDataType outputRowType,
      Set<CorrelationId> projectVariableSet) {
    List<FilterProjectRexNode> filterProjectRexNodes = new ArrayList<>(_filterProjectRexNodes.size() + 1);
    filterProjectRexNodes.addAll(_filterProjectRexNodes);
    filterProjectRexNodes.add(project);
    return new PinotLogicalEnrichedJoin(getCluster(), getTraitSet(), getHints(), left, right,
        getCondition(), getVariablesSet(), getJoinType(),
        filterProjectRexNodes, outputRowType, projectVariableSet,
        _fetch, _offset);
  }

  public PinotLogicalEnrichedJoin withNewFilter(FilterProjectRexNode filter) {
    List<FilterProjectRexNode> filterProjectRexNodes = new ArrayList<>(_filterProjectRexNodes.size() + 1);
    filterProjectRexNodes.addAll(_filterProjectRexNodes);
    filterProjectRexNodes.add(filter);
    return new PinotLogicalEnrichedJoin(getCluster(), getTraitSet(), getHints(), left, right,
        getCondition(), getVariablesSet(), getJoinType(),
        filterProjectRexNodes, _outputRowType, _projectVariableSet,
        _fetch, _offset);
  }

  public PinotLogicalEnrichedJoin withNewFetchOffset(@Nullable RexNode fetch, @Nullable RexNode offset) {
    return new PinotLogicalEnrichedJoin(getCluster(), getTraitSet(), getHints(), left, right,
        getCondition(), getVariablesSet(), getJoinType(),
        _filterProjectRexNodes, _outputRowType, _projectVariableSet,
        fetch, offset);
  }

  @Override
  protected RelDataType deriveRowType() {
    return checkNotNull(_outputRowType);
  }

  public final RelDataType getJoinRowType() {
    return SqlValidatorUtil.deriveJoinRowType(left.getRowType(),
        right.getRowType(), joinType, getCluster().getTypeFactory(), null,
        getSystemFieldList());
  }

  public List<FilterProjectRexNode> getFilterProjectRexNodes() {
    return _filterProjectRexNodes;
  }

  public List<RexNode> getProjects() {
    return _squashedProjects == null ? Collections.emptyList() : _squashedProjects;
  }

  /// Combine all projects in _filterProjectRexNodes into a single project
  @Nullable
  private List<RexNode> squashProjects() {
    List<RexNode> prevProject = null;
    for (FilterProjectRexNode node : _filterProjectRexNodes) {
      if (node.getType() == FilterProjectRexNodeType.FILTER) {
        continue;
      }
      assert node.getProjectAndResultRowType() != null;
      List<RexNode> project = node.getProjectAndResultRowType().getProject();
      if (prevProject == null) {
        prevProject = project;
        continue;
      }
      // combine project
      prevProject = combineProjects(project, prevProject);
    }
    return prevProject;
  }

  /// Adopted from {@link org.apache.calcite.plan.RelOptUtil#pushPastProject}
  private static List<RexNode> combineProjects(List<RexNode> upper, List<RexNode> lower) {
    return new RexShuttle() {
      @Override
      public RexNode visitInputRef(RexInputRef ref) {
        return lower.get(ref.getIndex());
      }
    }.visitList(upper);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("filterProjectRex", _filterProjectRexNodes)
        .itemIf("limit", _fetch, _fetch != null)
        .itemIf("offset", _offset, _offset != null);
  }

  public enum FilterProjectRexNodeType {
    FILTER,
    PROJECT
  }

  public static class ProjectAndResultRowType {
    private final List<RexNode> _project;
    private final RelDataType _dataType;

    public ProjectAndResultRowType(List<RexNode> project, RelDataType resultRowType) {
      _project = project;
      _dataType = resultRowType;
    }

    public List<RexNode> getProject() {
      return _project;
    }

    public RelDataType getDataType() {
      return _dataType;
    }
  }

  public static class FilterProjectRexNode {
    private final FilterProjectRexNodeType _type;
    @Nullable
    private final RexNode _filter;
    @Nullable
    private final ProjectAndResultRowType _projectAndResultRowType;

    @Override
    public String toString() {
      if (_type == FilterProjectRexNodeType.FILTER) {
        assert _filter != null;
        return "Filter: " + _filter;
      } else {
        assert _projectAndResultRowType != null;
        return "Project: " + _projectAndResultRowType.getProject().toString();
      }
    }

    public FilterProjectRexNode(RexNode filter) {
      _type = FilterProjectRexNodeType.FILTER;
      _filter = filter;
      _projectAndResultRowType = null;
    }

    public FilterProjectRexNode(List<RexNode> project, RelDataType resultDataType) {
      _type = FilterProjectRexNodeType.PROJECT;
      _filter = null;
      _projectAndResultRowType = new ProjectAndResultRowType(project, resultDataType);
    }

    public FilterProjectRexNodeType getType() {
      return _type;
    }

    @Nullable
    public RexNode getFilter() {
      return _filter;
    }

    @Nullable
    public ProjectAndResultRowType getProjectAndResultRowType() {
      return _projectAndResultRowType;
    }
  }

  @Nullable
  public RexNode getFetch() {
    return _fetch;
  }

  @Nullable
  public RexNode getOffset() {
    return _offset;
  }
}
