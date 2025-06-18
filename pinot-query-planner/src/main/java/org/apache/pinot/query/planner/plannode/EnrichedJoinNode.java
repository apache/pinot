package org.apache.pinot.query.planner.plannode;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;


public class EnrichedJoinNode extends JoinNode {
  // TODO: add filter, project, sort, limit info here
  @Nullable
  private final RexExpression _filterCondition;
  @Nullable
  private final List<RexExpression> _projects;
  @Nullable
  private final List<RelFieldCollation> _collations;
  private final int _fetch;
  private final int _offset;
  // output schema of the join
  private final DataSchema _joinResultSchema;
  // output schema after projection, same as _joinResultSchema if no projection
  private final DataSchema _projectResultSchema;

  public EnrichedJoinNode(int stageId, DataSchema joinResultSchema, DataSchema projectResultSchema, NodeHint nodeHint, List<PlanNode> inputs,
      JoinRelType joinType, List<Integer> leftKeys, List<Integer> rightKeys, List<RexExpression> nonEquiConditions,
      JoinStrategy joinStrategy, RexExpression matchCondition,
      RexExpression filterCondition, List<RexExpression> projects, List<RelFieldCollation> collations, int fetch, int offset) {
    super(stageId, projectResultSchema, nodeHint, inputs, joinType, leftKeys, rightKeys, nonEquiConditions, joinStrategy,
        matchCondition);
    _joinResultSchema = joinResultSchema;
    _projectResultSchema = projectResultSchema;

    _filterCondition = filterCondition;
    _projects = projects;
    _collations = collations;
    _fetch = fetch;
    _offset = offset;
  }

  public DataSchema getJoinResultSchema() {
    return _joinResultSchema;
  }

  public DataSchema getProjectResultSchema() {
    return _projectResultSchema;
  }

  public int getFetch() {
    return _fetch;
  }

  public int getOffset() {
    return _offset;
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

  @Nullable
  public List<RelFieldCollation> getCollations() {
    return _collations;
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitEnrichedJoin(this, context);
  }

  @Override
  public String explain() {
    return "ENRICHED_JOIN" +
        (_filterCondition == null ? " WITH FILTER" : "") +
        (_projects == null ? " WITH PROJECT" : "") +
        (_collations == null ? " WITH SORT" : "") +
        (_fetch == 0 ? " WITH LIMIT" : "") +
        (_offset == 0 ? " WITH OFFSET" :  "");
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new EnrichedJoinNode(_stageId, _joinResultSchema, _projectResultSchema, _nodeHint, inputs, getJoinType(), getLeftKeys(), getRightKeys(), getNonEquiConditions(),
        getJoinStrategy(), getMatchCondition(), _filterCondition, _projects, _collations, _fetch, _offset);
  }
}
