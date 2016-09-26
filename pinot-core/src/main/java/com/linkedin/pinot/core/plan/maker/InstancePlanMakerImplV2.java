/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
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
  private static final String ENABLE_NEW_AGGREGATION_GROUP_BY_CFG = "new.aggregation.groupby";
  private boolean _enableNewAggregationGroupByCfg = false;

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
    _enableNewAggregationGroupByCfg =
        queryExecutorConfig.getConfig().getBoolean(ENABLE_NEW_AGGREGATION_GROUP_BY_CFG, true);
    LOGGER.info("New AggregationGroupBy operator: {}", (_enableNewAggregationGroupByCfg) ? "Enabled" : "Disabled");
  }

  @Override
  public PlanNode makeInnerSegmentPlan(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    return makeInnerSegmentPlan(indexSegment, brokerRequest, _enableNewAggregationGroupByCfg);
  }

  public PlanNode makeInnerSegmentPlan(IndexSegment indexSegment, BrokerRequest brokerRequest,
      boolean enableNewAggregationGroupBy) {
    // Aggregation
    if (brokerRequest.isSetAggregationsInfo()) {
      if (!brokerRequest.isSetGroupBy()) {
        // Only Aggregation
        if (enableNewAggregationGroupBy) {
          return new AggregationPlanNode(indexSegment, brokerRequest);
        } else {
          return new AggregationOperatorPlanNode(indexSegment, brokerRequest);
        }
      } else {
        // Aggregation GroupBy
        if (enableNewAggregationGroupBy) {
          // New implementation of group-by aggregations
          return new AggregationGroupByPlanNode(indexSegment, brokerRequest);
        } else {
          // Old implementation of group-by aggregations
          if (isGroupKeyFitForLong(indexSegment, brokerRequest)) {
            return new AggregationGroupByOperatorPlanNode(indexSegment, brokerRequest,
                AggregationGroupByImplementationType.Dictionary);
          } else {
            return new AggregationGroupByOperatorPlanNode(indexSegment, brokerRequest,
                AggregationGroupByImplementationType.DictionaryAndTrie);
          }
        }
      }
    }
    // Selection
    if (brokerRequest.isSetSelections()) {
      return new SelectionPlanNode(indexSegment, brokerRequest);
    }
    throw new UnsupportedOperationException("The query contains no aggregation or selection!");
  }

  @Override
  public Plan makeInterSegmentPlan(List<SegmentDataManager> segmentDataManagers, BrokerRequest brokerRequest,
      ExecutorService executorService, long timeOutMs) {
    final InstanceResponsePlanNode rootNode = new InstanceResponsePlanNode();

    final CombinePlanNode combinePlanNode = new CombinePlanNode(brokerRequest, executorService, timeOutMs,
        _enableNewAggregationGroupByCfg);
    rootNode.setPlanNode(combinePlanNode);

    for (SegmentDataManager segmentDataManager : segmentDataManagers) {
      IndexSegment segment = segmentDataManager.getSegment();
      combinePlanNode.addPlanNode(makeInnerSegmentPlan(segment, brokerRequest, _enableNewAggregationGroupByCfg));
    }
    return new GlobalPlanImplV0(rootNode);
  }

  private boolean isGroupKeyFitForLong(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    int totalBitSet = 0;
    for (final String column : brokerRequest.getGroupBy().getColumns()) {
      Dictionary dictionary = indexSegment.getDataSource(column).getDictionary();
      totalBitSet += BitHacks.findLogBase2(dictionary.length()) + 1;
    }
    if (totalBitSet > 64) {
      return false;
    }
    return true;
  }
}
