package com.linkedin.pinot.core.plan.maker;

import java.util.List;
import java.util.concurrent.ExecutorService;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.chunk.index.ColumnarChunk;
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
public class InstancePlanMakerImplV1 implements PlanMaker {
  private final long _timeOutMs;

  public InstancePlanMakerImplV1() {
    _timeOutMs = 150000;
  }

  public InstancePlanMakerImplV1(long timeOutMs) {
    _timeOutMs = timeOutMs;
  }

  @Override
  public PlanNode makeInnerSegmentPlan(IndexSegment indexSegment, BrokerRequest brokerRequest) {

    if (brokerRequest.isSetAggregationsInfo()) {
      if (!brokerRequest.isSetGroupBy()) {
        // Only Aggregation
        final PlanNode aggregationPlanNode = new AggregationPlanNode(indexSegment, brokerRequest);
        return aggregationPlanNode;
      } else {
        // Aggregation GroupBy
        PlanNode aggregationGroupByPlanNode;
        if (indexSegment instanceof ColumnarChunk) {
          aggregationGroupByPlanNode =
              new AggregationGroupByOperatorPlanNode(indexSegment, brokerRequest,
                  AggregationGroupByImplementationType.DictionaryAndTrie);
        } else {
          aggregationGroupByPlanNode =
              new AggregationGroupByOperatorPlanNode(indexSegment, brokerRequest,
                  AggregationGroupByImplementationType.NoDictionary);
        }
        return aggregationGroupByPlanNode;
      }
    }
    // Only Selection
    if (brokerRequest.isSetSelections()) {
      final PlanNode selectionPlanNode = new SelectionPlanNode(indexSegment, brokerRequest);
      return selectionPlanNode;
    }
    throw new UnsupportedOperationException("The query contains no aggregation or selection!");
  }

  @Override
  public Plan makeInterSegmentPlan(List<IndexSegment> indexSegmentList, BrokerRequest brokerRequest,
      ExecutorService executorService) {
    final InstanceResponsePlanNode rootNode = new InstanceResponsePlanNode();
    final CombinePlanNode combinePlanNode = new CombinePlanNode(brokerRequest, executorService, _timeOutMs);
    rootNode.setPlanNode(combinePlanNode);
    for (final IndexSegment indexSegment : indexSegmentList) {
      combinePlanNode.addPlanNode(makeInnerSegmentPlan(indexSegment, brokerRequest));
    }
    return new GlobalPlanImplV0(rootNode);
  }

}
