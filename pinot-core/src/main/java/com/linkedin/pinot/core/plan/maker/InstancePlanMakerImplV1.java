/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;


/**
 * Make the huge plan, root is always ResultPlanNode, the child of it is a huge
 * plan node which will take the segment and query, then do everything.
 *
 *
 */
public class InstancePlanMakerImplV1 implements PlanMaker {

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
        if (indexSegment instanceof IndexSegmentImpl) {
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
      ExecutorService executorService, long timeOutMs) {
    final InstanceResponsePlanNode rootNode = new InstanceResponsePlanNode();
    final CombinePlanNode combinePlanNode = new CombinePlanNode(brokerRequest, executorService, timeOutMs);
    rootNode.setPlanNode(combinePlanNode);
    for (final IndexSegment indexSegment : indexSegmentList) {
      combinePlanNode.addPlanNode(makeInnerSegmentPlan(indexSegment, brokerRequest));
    }
    return new GlobalPlanImplV0(rootNode);
  }

}
