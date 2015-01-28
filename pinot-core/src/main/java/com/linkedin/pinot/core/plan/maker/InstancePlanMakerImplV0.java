package com.linkedin.pinot.core.plan.maker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.AggregationGroupByOperatorPlanNode;
import com.linkedin.pinot.core.plan.AggregationGroupByOperatorPlanNode.AggregationGroupByImplementationType;
import com.linkedin.pinot.core.plan.AggregationPlanNode;
import com.linkedin.pinot.core.plan.CombinePlanNode;
import com.linkedin.pinot.core.plan.GlobalPlanImplV0;
import com.linkedin.pinot.core.plan.InstanceResponsePlanNode;
import com.linkedin.pinot.core.plan.Plan;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.plan.SelectionPlanNode;


/**
 * Make the huge plan, root is always ResultPlanNode, the child of it is a huge
 * plan node which will take the segment and query, then do everything.
 * 
 * @author xiafu
 *
 */
public class InstancePlanMakerImplV0 implements PlanMaker {

  @Override
  public PlanNode makeInnerSegmentPlan(IndexSegment indexSegment, BrokerRequest brokerRequest) {

    if (brokerRequest.isSetAggregationsInfo()) {
      if (!brokerRequest.isSetGroupBy()) {
        // Only Aggregation
        PlanNode aggregationPlanNode = new AggregationPlanNode(indexSegment, brokerRequest);
        return aggregationPlanNode;
      } else {
        // Aggregation GroupBy
        PlanNode aggregationGroupByPlanNode =
            new AggregationGroupByOperatorPlanNode(indexSegment, brokerRequest,
                AggregationGroupByImplementationType.NoDictionary);
        return aggregationGroupByPlanNode;
      }
    }
    // Only Selection
    if (brokerRequest.isSetSelections()) {
      PlanNode selectionPlanNode = new SelectionPlanNode(indexSegment, brokerRequest);
      return selectionPlanNode;
    }
    throw new UnsupportedOperationException("The query contains no aggregation or selection!");
  }

  @Override
  public Plan makeInterSegmentPlan(List<IndexSegment> indexSegmentList, BrokerRequest brokerRequest,
      ExecutorService executorService, long timeOutMs) {
    InstanceResponsePlanNode rootNode = new InstanceResponsePlanNode();
    CombinePlanNode combinePlanNode = new CombinePlanNode(brokerRequest, executorService, timeOutMs);
    rootNode.setPlanNode(combinePlanNode);
    for (IndexSegment indexSegment : indexSegmentList) {
      combinePlanNode.addPlanNode(makeInnerSegmentPlan(indexSegment, brokerRequest));
    }
    return new GlobalPlanImplV0(rootNode);
  }

}
