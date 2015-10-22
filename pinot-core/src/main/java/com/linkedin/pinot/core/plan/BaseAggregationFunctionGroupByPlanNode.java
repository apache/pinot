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
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.UReplicatedProjectionOperator;
import com.linkedin.pinot.core.operator.query.MAggregationFunctionGroupByWithDictionaryAndTrieTreeOperator;
import com.linkedin.pinot.core.operator.query.MAggregationFunctionGroupByWithDictionaryOperator;
import com.linkedin.pinot.core.operator.query.MDefaultAggregationFunctionGroupByOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseAggregationFunctionGroupByPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger("QueryPlanLog");

  private final AggregationInfo aggregationInfo;
  private final GroupBy groupBy;
  private final BaseAggregationGroupByOperatorPlanNode.AggregationGroupByImplementationType implementationType;
  private final BaseProjectionPlanNode projectionPlanNode;
  private final boolean hasDictionary;

  public BaseAggregationFunctionGroupByPlanNode(
      AggregationInfo aggregationInfo,
      GroupBy groupBy,
      BaseProjectionPlanNode projectionPlanNode,
      BaseAggregationGroupByOperatorPlanNode.AggregationGroupByImplementationType implementationType,
      boolean hasDictionary) {
    this.aggregationInfo = aggregationInfo;
    this.groupBy = groupBy;
    this.projectionPlanNode = projectionPlanNode;
    this.implementationType = implementationType;
    this.hasDictionary = hasDictionary;
  }

  @Override
  public Operator run() {
    switch (implementationType) {
      case NoDictionary:
        return new MDefaultAggregationFunctionGroupByOperator(aggregationInfo, groupBy, new UReplicatedProjectionOperator(
            (MProjectionOperator) projectionPlanNode.run()), hasDictionary);
      case Dictionary:
        return new MAggregationFunctionGroupByWithDictionaryOperator(aggregationInfo, groupBy,
            new UReplicatedProjectionOperator((MProjectionOperator) projectionPlanNode.run()), hasDictionary);
      case DictionaryAndTrie:
        return new MAggregationFunctionGroupByWithDictionaryAndTrieTreeOperator(aggregationInfo, groupBy,
            new UReplicatedProjectionOperator((MProjectionOperator) projectionPlanNode.run()), hasDictionary);
      default:
        throw new UnsupportedOperationException("Not Support AggregationGroupBy implmentation: "
            + implementationType);
    }
  }

  @Override
  public void showTree(String prefix) {
    switch (implementationType) {
      case NoDictionary:
        LOGGER.debug(prefix + "Operator: MAggregationFunctionGroupByOperator");
        break;
      case Dictionary:
        LOGGER.debug(prefix + "Operator: MAggregationFunctionGroupByWithDictionaryOperator");
        break;
      case DictionaryAndTrie:
        LOGGER.debug(prefix + "Operator: MAggregationFunctionGroupByWithDictionaryAndTrieTreeOperator");
        break;
      default:
        throw new UnsupportedOperationException("Not Support AggregationGroupBy implmentation: "
            + implementationType);
    }

    LOGGER.debug(prefix + "Argument 0: Aggregation  - " + aggregationInfo);
    LOGGER.debug(prefix + "Argument 1: GroupBy  - " + groupBy);
    LOGGER.debug(prefix + "Argument 2: Projection - Shown Above");
  }
}
