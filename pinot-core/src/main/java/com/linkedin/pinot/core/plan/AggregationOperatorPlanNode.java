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
package com.linkedin.pinot.core.plan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.query.BAggregationFunctionOperator;
import com.linkedin.pinot.core.operator.query.MAggregationOperator;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionUtils;


/**
 * AggregationOperatorPlanNode takes care of how to apply an aggregation query to an IndexSegment.
 */
public class AggregationOperatorPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger("QueryPlanLog");
  private final IndexSegment _indexSegment;
  private final BrokerRequest _brokerRequest;
  private final List<AggregationFunctionPlanNode> _aggregationFunctionPlanNodes =
      new ArrayList<AggregationFunctionPlanNode>();
  private final ProjectionPlanNode _projectionPlanNode;

  public AggregationOperatorPlanNode(IndexSegment indexSegment, BrokerRequest query) {
    _indexSegment = indexSegment;
    _brokerRequest = query;
    _projectionPlanNode = new ProjectionPlanNode(_indexSegment, getAggregationRelatedColumns(),
        new DocIdSetPlanNode(_indexSegment, _brokerRequest));
    for (int i = 0; i < _brokerRequest.getAggregationsInfo().size(); ++i) {
      AggregationInfo aggregationInfo = _brokerRequest.getAggregationsInfo().get(i);
      AggregationFunctionUtils.ensureAggregationColumnsAreSingleValued(aggregationInfo, _indexSegment);
      boolean hasDictionary = AggregationFunctionUtils.isAggregationFunctionWithDictionary(aggregationInfo, _indexSegment);
      _aggregationFunctionPlanNodes.add(new AggregationFunctionPlanNode(aggregationInfo, _projectionPlanNode, hasDictionary));
    }
  }

  /**
   * Returns an array of aggregation columns.
   * @return
   */
  private String[] getAggregationRelatedColumns() {
    Set<String> aggregationRelatedColumns = new HashSet<String>();
    for (AggregationInfo aggregationInfo : _brokerRequest.getAggregationsInfo()) {
      if (!aggregationInfo.getAggregationType().equalsIgnoreCase("count")) {
        String columns = aggregationInfo.getAggregationParams().get("column").trim();
        aggregationRelatedColumns.addAll(Arrays.asList(columns.split(",")));
      }
    }
    return aggregationRelatedColumns.toArray(new String[0]);
  }

  /**
   * Returns the MAggregationOperator that performs aggregation.
   * @return
   */
  @Override
  public Operator run() {
    List<BAggregationFunctionOperator> aggregationFunctionOperatorList = new ArrayList<BAggregationFunctionOperator>();
    for (AggregationFunctionPlanNode aggregationFunctionPlanNode : _aggregationFunctionPlanNodes) {
      aggregationFunctionOperatorList.add((BAggregationFunctionOperator) aggregationFunctionPlanNode.run());
    }
    return new MAggregationOperator(_indexSegment, _brokerRequest.getAggregationsInfo(),
        (MProjectionOperator) _projectionPlanNode.run(), aggregationFunctionOperatorList);
  }

  /**
   * Debug method to show the query plan.
   * @param prefix
   */
  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Inner-Segment Plan Node :");
    LOGGER.debug(prefix + "Operator: MAggregationOperator");
    LOGGER.debug(prefix + "Argument 0: Projection - ");
    _projectionPlanNode.showTree(prefix + "    ");
    for (int i = 0; i < _brokerRequest.getAggregationsInfo().size(); ++i) {
      LOGGER.debug(prefix + "Argument " + (i + 1) + ": Aggregation  - ");
      _aggregationFunctionPlanNodes.get(i).showTree(prefix + "    ");
    }

  }

}
