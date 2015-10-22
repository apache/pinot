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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.UReplicatedProjectionOperator;
import com.linkedin.pinot.core.operator.query.BAggregationFunctionOperator;


/**
 * AggregationFunctionPlanNode takes care of how to apply one aggregation
 * function for given data sources.
 *
 *
 * This logic is refactored into another class used by {@link com.linkedin.pinot.core.plan.maker.InstancePlanMakerImplV3}
 * @see BaseAggregationFunctionPlanNode
 */
@Deprecated
public class AggregationFunctionPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger("QueryPlanLog");
  private final AggregationInfo _aggregationInfo;
  private final ProjectionPlanNode _projectionPlanNode;
  private final boolean _hasDictionary;

  public AggregationFunctionPlanNode(AggregationInfo aggregationInfo, ProjectionPlanNode projectionPlanNode, boolean hasDictionary) {
    _aggregationInfo = aggregationInfo;
    _projectionPlanNode = projectionPlanNode;
    _hasDictionary = hasDictionary;
  }

  @Override
  public Operator run() {
    return new BAggregationFunctionOperator(_aggregationInfo, new UReplicatedProjectionOperator(
        (MProjectionOperator) _projectionPlanNode.run()), _hasDictionary);
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Operator: BAggregationFunctionOperator");
    LOGGER.debug(prefix + "Argument 0: Aggregation  - " + _aggregationInfo);
    LOGGER.debug(prefix + "Argument 1: Projection - Shown Above");
  }

}
