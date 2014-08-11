package com.linkedin.pinot.core.plan.maker;

import java.util.List;
import java.util.concurrent.ExecutorService;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.AggregationAndSelectionPlanNode;
import com.linkedin.pinot.core.plan.AggregationPlanNode;
import com.linkedin.pinot.core.plan.CombinePlanNode;
import com.linkedin.pinot.core.plan.GlobalPlanImplV0;
import com.linkedin.pinot.core.plan.InstanceResponsePlanNode;
import com.linkedin.pinot.core.plan.Plan;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.plan.SelectionPlanNode;


/**
 * Make the huge plan, root is always ResultPlanNode, the child of it is a hugh
 * plan node which will take the segment and query, then do everything.
 * 
 * @author xiafu
 *
 */
public class InstancePlanMakerImpl implements PlanMaker {

  @Override
  public PlanNode makeInnerSegmentPlan(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    if (brokerRequest.isSetAggregationsInfo() && brokerRequest.isSetSelections()) {
      PlanNode aggregationAndSelectionPlanNode = new AggregationAndSelectionPlanNode(indexSegment, brokerRequest);
      return aggregationAndSelectionPlanNode;
    } else {
      // Only Aggregation
      if (brokerRequest.isSetAggregationsInfo()) {
        PlanNode aggregationPlanNode = new AggregationPlanNode(indexSegment, brokerRequest);
        return aggregationPlanNode;
      }
      // Only Selection
      if (brokerRequest.isSetSelections()) {
        PlanNode selectionPlanNode = new SelectionPlanNode(indexSegment, brokerRequest);
        return selectionPlanNode;
      }
    }
    throw new UnsupportedOperationException("The query contains no aggregation or selection!");
  }

  @Override
  public Plan makeInterSegmentPlan(List<IndexSegment> indexSegmentList, BrokerRequest brokerRequest,
      ExecutorService executorService) {
    InstanceResponsePlanNode rootNode = new InstanceResponsePlanNode();
    CombinePlanNode combinePlanNode = new CombinePlanNode(brokerRequest, executorService);
    rootNode.setPlanNode(combinePlanNode);
    for (IndexSegment indexSegment : indexSegmentList) {
      combinePlanNode.addPlanNode(makeInnerSegmentPlan(indexSegment, brokerRequest));
    }
    return new GlobalPlanImplV0(rootNode);
  }

}
