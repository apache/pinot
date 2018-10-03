/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.startree.plan;

import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.core.operator.transform.TransformOperator;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.startree.v2.AggregationFunctionColumnPair;
import com.linkedin.pinot.core.startree.v2.StarTreeV2;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StarTreeTransformPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(StarTreeTransformPlanNode.class);

  private final Set<TransformExpressionTree> _groupByExpressions;
  private final StarTreeProjectionPlanNode _starTreeProjectionPlanNode;

  public StarTreeTransformPlanNode(StarTreeV2 starTreeV2,
      Set<AggregationFunctionColumnPair> aggregationFunctionColumnPairs,
      @Nullable Set<TransformExpressionTree> groupByExpressions, @Nullable FilterQueryTree rootFilterNode,
      @Nullable Map<String, String> debugOptions) {
    Set<String> projectionColumns = new HashSet<>();
    for (AggregationFunctionColumnPair aggregationFunctionColumnPair : aggregationFunctionColumnPairs) {
      projectionColumns.add(aggregationFunctionColumnPair.toColumnName());
    }
    Set<String> groupByColumns;
    if (groupByExpressions != null) {
      _groupByExpressions = groupByExpressions;
      groupByColumns = new HashSet<>();
      for (TransformExpressionTree groupByExpression : groupByExpressions) {
        groupByExpression.getColumns(groupByColumns);
      }
      projectionColumns.addAll(groupByColumns);
    } else {
      _groupByExpressions = Collections.emptySet();
      groupByColumns = null;
    }
    _starTreeProjectionPlanNode =
        new StarTreeProjectionPlanNode(starTreeV2, projectionColumns, rootFilterNode, groupByColumns, debugOptions);
  }

  @Override
  public TransformOperator run() {
    return new TransformOperator(_starTreeProjectionPlanNode.run(), _groupByExpressions);
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "StarTree Transform Plan Node:");
    LOGGER.debug(prefix + "Operator: TransformOperator");
    LOGGER.debug(prefix + "Argument 0: Group-by Expressions - " + _groupByExpressions);
    LOGGER.debug(prefix + "Argument 1: StarTreeProjectionPlanNode -");
    _starTreeProjectionPlanNode.showTree(prefix + "    ");
  }
}
