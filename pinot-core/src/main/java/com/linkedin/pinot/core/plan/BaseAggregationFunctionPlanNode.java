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
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.UReplicatedProjectionOperator;
import com.linkedin.pinot.core.operator.query.BAggregationFunctionOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: No need to make abstract yet
public class BaseAggregationFunctionPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger("QueryPlanLog");
  protected final AggregationInfo aggregationInfo;
  protected final BaseProjectionPlanNode projectionPlanNode;
  protected final boolean hasDictionary;

  public BaseAggregationFunctionPlanNode(AggregationInfo aggregationInfo,
                                         BaseProjectionPlanNode projectionPlanNode,
                                         boolean hasDictionary) {
    this.aggregationInfo = aggregationInfo;
    this.projectionPlanNode = projectionPlanNode;
    this.hasDictionary = hasDictionary;
  }

  @Override
  public Operator run() {
    return new BAggregationFunctionOperator(aggregationInfo, new UReplicatedProjectionOperator(
        (MProjectionOperator) projectionPlanNode.run()), hasDictionary);
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Operator: BAggregationFunctionOperator");
    LOGGER.debug(prefix + "Argument 0: Aggregation  - " + aggregationInfo);
    LOGGER.debug(prefix + "Argument 1: Projection - Shown Above");
  }
}
