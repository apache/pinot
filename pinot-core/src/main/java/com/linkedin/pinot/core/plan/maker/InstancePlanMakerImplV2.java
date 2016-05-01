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

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.AggregationGroupByImplementationType;
import com.linkedin.pinot.core.plan.AggregationGroupByOperatorPlanNode;
import com.linkedin.pinot.core.plan.AggregationGroupByPlanNode;
import com.linkedin.pinot.core.plan.AggregationOperatorPlanNode;
import com.linkedin.pinot.core.plan.AggregationPlanNode;
import com.linkedin.pinot.core.plan.CombinePlanNode;
import com.linkedin.pinot.core.plan.GlobalPlanImplV0;
import com.linkedin.pinot.core.plan.InstanceResponsePlanNode;
import com.linkedin.pinot.core.plan.Plan;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.plan.SelectionPlanNode;
import com.linkedin.pinot.core.query.aggregation.groupby.BitHacks;
import com.linkedin.pinot.core.query.config.QueryExecutorConfig;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Make the huge plan, root is always ResultPlanNode, the child of it is a huge
 * plan node which will take the segment and query, then do everything.
 *
 *
 */
public class InstancePlanMakerImplV2 implements PlanMaker {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstancePlanMakerImplV2.class);
  private static final String NEW_AGGREGATION_GROUPBY_STRING = "new.aggregation.groupby";
  private boolean _enableNewAggreagationGroupBy = false;

  /**
   * Default constructor.
   */
  public InstancePlanMakerImplV2() {
  }

  /**
   * Constructor for usage when client requires to pass queryExecutorConfig to this class.
   * Sets flag to indicate whether to enable new implementation of AggregationGroupBy operator,
   * based on the queryExecutorConfig.
   *
   * @param queryExecutorConfig
   */
  public InstancePlanMakerImplV2(QueryExecutorConfig queryExecutorConfig) {
    _enableNewAggreagationGroupBy = queryExecutorConfig.getConfig().getBoolean(NEW_AGGREGATION_GROUPBY_STRING, false);
    LOGGER.info("New AggregationGroupBy operator: {}", (_enableNewAggreagationGroupBy) ? "Enabled" : "Disabled");
  }

  @Override
  public PlanNode makeInnerSegmentPlan(IndexSegment indexSegment, BrokerRequest brokerRequest) {

    if (brokerRequest.isSetAggregationsInfo()) {
      if (!brokerRequest.isSetGroupBy()) {
        // Only Aggregation
        if (useNewAggregationOperator(brokerRequest)) {
          return new AggregationPlanNode(indexSegment, brokerRequest);
        } else {
          return new AggregationOperatorPlanNode(indexSegment, brokerRequest);
        }
      } else {
        // Aggregation GroupBy
        PlanNode aggregationGroupByPlanNode;
        if (indexSegment instanceof IndexSegmentImpl) {

          // AggregationGroupByPlanNode is the new implementation of group-by aggregations, and is currently turned OFF.
          // Once all feature and perf testing is performed, the code will be turned ON, and this 'if' check will
          // be removed.
          if (_enableNewAggreagationGroupBy) {
            aggregationGroupByPlanNode = new AggregationGroupByPlanNode(indexSegment, brokerRequest,
                AggregationGroupByImplementationType.Dictionary);
          } else {
            if (isGroupKeyFitForLong(indexSegment, brokerRequest)) {
              aggregationGroupByPlanNode = new AggregationGroupByOperatorPlanNode(indexSegment, brokerRequest,
                  AggregationGroupByImplementationType.Dictionary);
            } else {
              aggregationGroupByPlanNode = new AggregationGroupByOperatorPlanNode(indexSegment, brokerRequest,
                  AggregationGroupByImplementationType.DictionaryAndTrie);
            }
          }
        } else {
          aggregationGroupByPlanNode =
              new AggregationGroupByOperatorPlanNode(indexSegment, brokerRequest, AggregationGroupByImplementationType.NoDictionary);
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

  /**
   * Temporary method to check if the new implementation of Aggregation can be used.
   * This method will be removed once the new implementation of AggregationOperator is turned
   * ON by default.
   *
   * @param brokerRequest
   * @return
   */
  private boolean useNewAggregationOperator(BrokerRequest brokerRequest) {
    return _enableNewAggreagationGroupBy && AggregationPlanNode.isFitForAggregationFastAggregation(brokerRequest);
  }

  @Override
  public Plan makeInterSegmentPlan(List<SegmentDataManager> segmentDataManagers, BrokerRequest brokerRequest,
      ExecutorService executorService, long timeOutMs) {
    final InstanceResponsePlanNode rootNode = new InstanceResponsePlanNode();
    final CombinePlanNode combinePlanNode = new CombinePlanNode(brokerRequest, executorService, timeOutMs);
    rootNode.setPlanNode(combinePlanNode);
    for (final SegmentDataManager segmentDataManager : segmentDataManagers) {
      combinePlanNode.addPlanNode(makeInnerSegmentPlan(segmentDataManager.getSegment(), brokerRequest));
    }
    return new GlobalPlanImplV0(rootNode);
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
}
