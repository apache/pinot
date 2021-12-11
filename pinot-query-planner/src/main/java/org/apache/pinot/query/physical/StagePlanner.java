package org.apache.pinot.query.physical;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.pinot.query.planner.QueryContext;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.QueryStage;


/**
 * QueryPlanMaker walks top-down from {@link RelRoot} and construct a partial tree as a {@link QueryStage}.
 *
 */
public class StagePlanner {
  private final List<QueryStage> _queryStageList;
  private final QueryContext _queryContext;

  public StagePlanner(QueryContext queryContext) {
    _queryContext = queryContext;
    _queryStageList = new ArrayList<>();
  }

  public QueryPlan makePlan(RelNode relRoot) {
    // clear up space for planning
    _queryStageList.clear();
    StagePlanContext stagePlanContext = new StagePlanContext();
    walkRelPlan(relRoot, stagePlanContext);
    return constructQueryPlan(stagePlanContext);
  }

  private QueryPlan constructQueryPlan(StagePlanContext stagePlanContext) {
    return null;
  }

  // non-threadsafe
  private void walkRelPlan(RelNode node, StagePlanContext stagePlanContext) {
    // set context to current node.
    stagePlanContext._currentNode = node;
    List<RelNode> inputs = node.getInputs();
    for (RelNode input : inputs) {
      walkRelPlan(input, stagePlanContext);
    }
    // if exchange node, split into mailboxSend/mailboxReceive.
    if (isExchangeNode(node)) {
      node.getInputs();
    }
  }

  private boolean isExchangeNode(RelNode node) {
    return false;
  }

  private static class StagePlanContext {
    public List<QueryStage> _stageList = new ArrayList<>();
    public RelNode _currentStageRoot;
    public RelNode _currentNode;
  }
}
