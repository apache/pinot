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

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.query.AggregationFunctionGroupByOperator;
import com.linkedin.pinot.core.operator.query.MAggregationGroupByOperator;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class BaseAggregationGroupByOperatorPlanNode implements PlanNode {
  protected static final Logger LOGGER = LoggerFactory.getLogger("QueryPlanLog");
  protected static final int DEFAULT_NUM_DOCS = 10000;

  protected final IndexSegment indexSegment;
  protected final BrokerRequest brokerRequest;
  protected final AggregationGroupByImplementationType implementationType;
  protected final BaseProjectionPlanNode projectionPlanNode;
  protected final List<BaseAggregationFunctionGroupByPlanNode> aggregationFunctionGroupByPlanNodes;

  public BaseAggregationGroupByOperatorPlanNode(
      IndexSegment indexSegment,
      BrokerRequest brokerRequest,
      AggregationGroupByImplementationType implementationType) {
    this.indexSegment = indexSegment;
    this.brokerRequest = brokerRequest;
    this.implementationType = implementationType;
    this.projectionPlanNode = getProjectionPlanNode();
    this.aggregationFunctionGroupByPlanNodes = new ArrayList<>();

    for (int i = 0; i < brokerRequest.getAggregationsInfo().size(); ++i) {
      AggregationInfo aggregationInfo = brokerRequest.getAggregationsInfo().get(i);
      boolean hasDictionary = AggregationFunctionUtils.isAggregationFunctionWithDictionary(aggregationInfo, indexSegment);
      aggregationFunctionGroupByPlanNodes.add(new BaseAggregationFunctionGroupByPlanNode(aggregationInfo, brokerRequest.getGroupBy(), projectionPlanNode,
          implementationType, hasDictionary));
    }
  }

  @Override
  public Operator run() {
    List<AggregationFunctionGroupByOperator> aggregationFunctionOperatorList =
        new ArrayList<AggregationFunctionGroupByOperator>();
    for (BaseAggregationFunctionGroupByPlanNode aggregationFunctionGroupByPlanNode : aggregationFunctionGroupByPlanNodes) {
      aggregationFunctionOperatorList
          .add((AggregationFunctionGroupByOperator) aggregationFunctionGroupByPlanNode.run());
    }
    return new MAggregationGroupByOperator(indexSegment, brokerRequest.getAggregationsInfo(),
        brokerRequest.getGroupBy(), projectionPlanNode.run(), aggregationFunctionOperatorList);
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Inner-Segment Plan Node :");
    LOGGER.debug(prefix + "Operator: MAggregationGroupByOperator");
    LOGGER.debug(prefix + "Argument 0: Projection - ");
    projectionPlanNode.showTree(prefix + "    ");
    for (int i = 0; i < brokerRequest.getAggregationsInfo().size(); ++i) {
      LOGGER.debug(prefix + "Argument " + (i + 1) + ": AggregationGroupBy  - ");
      aggregationFunctionGroupByPlanNodes.get(i).showTree(prefix + "    ");
    }
  }

  protected abstract BaseProjectionPlanNode getProjectionPlanNode();

  protected String[] getAggregationGroupByRelatedColumns() {
    Set<String> aggregationGroupByRelatedColumns = new HashSet<String>();
    for (AggregationInfo aggregationInfo : brokerRequest.getAggregationsInfo()) {
      if (aggregationInfo.getAggregationType().equalsIgnoreCase("count")) {
        continue;
      }
      String columns = aggregationInfo.getAggregationParams().get("column").trim();
      aggregationGroupByRelatedColumns.addAll(Arrays.asList(columns.split(",")));
    }
    aggregationGroupByRelatedColumns.addAll(brokerRequest.getGroupBy().getColumns());
    return aggregationGroupByRelatedColumns.toArray(new String[0]);
  }

  public enum AggregationGroupByImplementationType {
    NoDictionary,
    Dictionary,
    DictionaryAndTrie
  }
}
