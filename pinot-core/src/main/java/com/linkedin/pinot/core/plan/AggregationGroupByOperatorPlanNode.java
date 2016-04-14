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
import com.linkedin.pinot.core.operator.query.AggregationFunctionGroupByOperator;
import com.linkedin.pinot.core.operator.query.MAggregationGroupByOperator;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionUtils;

/**
 * AggregationGroupByOperatorPlanNode takes care of how to apply multiple aggregation
 * functions and groupBy query to an IndexSegment.
 */
public class AggregationGroupByOperatorPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger("QueryPlanLog");
  private final IndexSegment _indexSegment;
  private final BrokerRequest _brokerRequest;
  private final ProjectionPlanNode _projectionPlanNode;
  private final List<AggregationFunctionGroupByPlanNode> _aggregationFunctionGroupByPlanNodes =
      new ArrayList<AggregationFunctionGroupByPlanNode>();
  private final AggregationGroupByImplementationType _aggregationGroupByImplementationType;

  public AggregationGroupByOperatorPlanNode(IndexSegment indexSegment, BrokerRequest query,
      AggregationGroupByImplementationType aggregationGroupByImplementationType) {
    _indexSegment = indexSegment;
    _brokerRequest = query;
    _aggregationGroupByImplementationType = aggregationGroupByImplementationType;
    _projectionPlanNode = new ProjectionPlanNode(_indexSegment,
        getAggregationGroupByRelatedColumns(), new DocIdSetPlanNode(_indexSegment, _brokerRequest));
    for (int i = 0; i < _brokerRequest.getAggregationsInfo().size(); ++i) {
      AggregationInfo aggregationInfo = _brokerRequest.getAggregationsInfo().get(i);
      AggregationFunctionUtils.ensureAggregationColumnsAreSingleValued(aggregationInfo, _indexSegment);
      boolean hasDictionary = AggregationFunctionUtils
          .isAggregationFunctionWithDictionary(aggregationInfo, _indexSegment);
      _aggregationFunctionGroupByPlanNodes
          .add(new AggregationFunctionGroupByPlanNode(aggregationInfo, _brokerRequest.getGroupBy(),
              _projectionPlanNode, _aggregationGroupByImplementationType, hasDictionary));
    }
  }

  private String[] getAggregationGroupByRelatedColumns() {
    Set<String> aggregationGroupByRelatedColumns = new HashSet<String>();
    for (AggregationInfo aggregationInfo : _brokerRequest.getAggregationsInfo()) {
      if (aggregationInfo.getAggregationType().equalsIgnoreCase("count")) {
        continue;
      }
      String columns = aggregationInfo.getAggregationParams().get("column").trim();
      aggregationGroupByRelatedColumns.addAll(Arrays.asList(columns.split(",")));
    }
    aggregationGroupByRelatedColumns.addAll(_brokerRequest.getGroupBy().getColumns());
    return aggregationGroupByRelatedColumns.toArray(new String[0]);
  }

  @Override
  public Operator run() {
    List<AggregationFunctionGroupByOperator> aggregationFunctionOperatorList =
        new ArrayList<AggregationFunctionGroupByOperator>();
    for (AggregationFunctionGroupByPlanNode aggregationFunctionGroupByPlanNode : _aggregationFunctionGroupByPlanNodes) {
      aggregationFunctionOperatorList
          .add((AggregationFunctionGroupByOperator) aggregationFunctionGroupByPlanNode.run());
    }
    return new MAggregationGroupByOperator(_indexSegment, _brokerRequest.getAggregationsInfo(),
        _brokerRequest.getGroupBy(), _projectionPlanNode.run(), aggregationFunctionOperatorList);
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Inner-Segment Plan Node :");
    LOGGER.debug(prefix + "Operator: MAggregationGroupByOperator");
    LOGGER.debug(prefix + "Argument 0: Projection - ");
    _projectionPlanNode.showTree(prefix + "    ");
    for (int i = 0; i < _brokerRequest.getAggregationsInfo().size(); ++i) {
      LOGGER.debug(prefix + "Argument " + (i + 1) + ": AggregationGroupBy  - ");
      _aggregationFunctionGroupByPlanNodes.get(i).showTree(prefix + "    ");
    }
  }

}
