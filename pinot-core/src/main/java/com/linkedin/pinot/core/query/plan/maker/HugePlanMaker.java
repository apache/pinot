package com.linkedin.pinot.core.query.plan.maker;

import com.linkedin.pinot.common.query.request.Query;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.query.plan.HugeWorkerPlanNode;
import com.linkedin.pinot.core.query.plan.ResultPlanNode;


/**
 * Make the huge plan, root is always ResultPlanNode, the child of it is a hugh
 * plan node which will take the segment and query, then do everything.
 * 
 * @author xiafu
 *
 */
public class HugePlanMaker implements PlanMaker {

  @Override
  public PlanNode makePlan(IndexSegment indexSegment, Query query) {
    ResultPlanNode rootNode = new ResultPlanNode();
    PlanNode hugeWorkNode = new HugeWorkerPlanNode(indexSegment, query);
    rootNode.addPlanNode(hugeWorkNode);
    return rootNode;
  }

}
