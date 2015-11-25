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

import java.util.List;
import java.util.concurrent.ExecutorService;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.Plan;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.query.aggregation.groupby.BitHacks;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;

/**
 * Make the huge plan, root is always ResultPlanNode, the child of it is a huge
 * plan node which will take the segment and query, then do everything.
 */
public class InstancePlanMakerImplV3 implements PlanMaker {

  @Override
  public PlanNode makeInnerSegmentPlan(IndexSegment indexSegment, BrokerRequest brokerRequest) {

    throw new UnsupportedOperationException("The query contains no aggregation or selection!");
  }

  @Override
  public Plan makeInterSegmentPlan(BrokerRequest brokerRequest, List<SegmentDataManager> segmentDataManagers,
      ExecutorService executorService, long timeOutMs) {
    throw new UnsupportedOperationException("The query contains no aggregation or selection!");
  }

  @Override
  public Plan makeInterSegmentPlan(List<IndexSegment> indexSegmentList, BrokerRequest brokerRequest,
      ExecutorService executorService, long timeOutMs) {
    throw new UnsupportedOperationException("The query contains no aggregation or selection!");
  }

  private boolean isGroupKeyFitForLong(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    final IndexSegmentImpl columnarSegment = (IndexSegmentImpl) indexSegment;
    int totalBitSet = 0;
    for (final String column : brokerRequest.getGroupBy().getColumns()) {
      totalBitSet += BitHacks.findLogBase2(columnarSegment.getDictionaryFor(column).length()) + 1;
    }
    if (totalBitSet > 64) {
      return false;
    }
    return true;
  }

  private boolean isConjunctiveAggregateQuery(BrokerRequest brokerRequest) {
    // All must be sum
    for (AggregationInfo aggregationInfo : brokerRequest.getAggregationsInfo()) {
      if (!aggregationInfo.getAggregationType().equalsIgnoreCase("sum")) {
        return false;
      }
    }

    // If filter defined, check
    if (brokerRequest.isSetFilterQuery()) {
      FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);
      return isSimpleConjunction(filterQueryTree);
    }

    return true;
  }

  /**
   * Returns true if the filter query consists only of equality statements, conjoined by AND.
   * <p>
   * e.g. WHERE d1 = d1v1 AND d2 = d2v2 AND d3 = d3v3
   * </p>
   */
  private boolean isSimpleConjunction(FilterQueryTree tree) {
    if (tree.getChildren() == null) {
      return FilterOperator.EQUALITY.equals(tree.getOperator()) && tree.getValue().size() == 1;
    } else {
      boolean res = FilterOperator.AND.equals(tree.getOperator());
      for (FilterQueryTree subTree : tree.getChildren()) {
        res &= isSimpleConjunction(subTree);
      }
      return res;
    }
  }
}
