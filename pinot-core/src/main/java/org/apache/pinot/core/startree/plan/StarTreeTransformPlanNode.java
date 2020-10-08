/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.startree.plan;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.startree.v2.AggregationFunctionColumnPair;
import org.apache.pinot.core.startree.v2.StarTreeV2;


public class StarTreeTransformPlanNode implements PlanNode {
  private final List<ExpressionContext> _groupByExpressions;
  private final StarTreeProjectionPlanNode _starTreeProjectionPlanNode;

  public StarTreeTransformPlanNode(StarTreeV2 starTreeV2,
      AggregationFunctionColumnPair[] aggregationFunctionColumnPairs, @Nullable ExpressionContext[] groupByExpressions,
      Map<String, List<PredicateEvaluator>> predicateEvaluatorsMap, @Nullable Map<String, String> debugOptions) {
    Set<String> projectionColumns = new HashSet<>();
    for (AggregationFunctionColumnPair aggregationFunctionColumnPair : aggregationFunctionColumnPairs) {
      projectionColumns.add(aggregationFunctionColumnPair.toColumnName());
    }
    Set<String> groupByColumns;
    if (groupByExpressions != null) {
      _groupByExpressions = Arrays.asList(groupByExpressions);
      groupByColumns = new HashSet<>();
      for (ExpressionContext groupByExpression : groupByExpressions) {
        groupByExpression.getColumns(groupByColumns);
      }
      projectionColumns.addAll(groupByColumns);
    } else {
      _groupByExpressions = Collections.emptyList();
      groupByColumns = null;
    }
    _starTreeProjectionPlanNode =
        new StarTreeProjectionPlanNode(starTreeV2, projectionColumns, predicateEvaluatorsMap, groupByColumns,
            debugOptions);
  }

  @Override
  public TransformOperator run() {
    // NOTE: Here we do not put aggregation expressions into TransformOperator based on the following assumptions:
    //       - They are all columns (not functions or constants), where no transform is required
    //       - We never call TransformOperator.getResultMetadata() or TransformOperator.getDictionary() on them
    return new TransformOperator(_starTreeProjectionPlanNode.run(), _groupByExpressions);
  }
}
