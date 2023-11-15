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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.DocIdSetOperator;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.ProjectionOperatorUtils;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.startree.CompositePredicateEvaluator;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;


public class StarTreeProjectPlanNode implements PlanNode {
  private final QueryContext _queryContext;
  private final StarTreeV2 _starTreeV2;
  private final AggregationFunctionColumnPair[] _aggregationFunctionColumnPairs;
  private final ExpressionContext[] _groupByExpressions;
  private final Map<String, List<CompositePredicateEvaluator>> _predicateEvaluatorsMap;

  public StarTreeProjectPlanNode(QueryContext queryContext, StarTreeV2 starTreeV2,
      AggregationFunctionColumnPair[] aggregationFunctionColumnPairs, @Nullable ExpressionContext[] groupByExpressions,
      Map<String, List<CompositePredicateEvaluator>> predicateEvaluatorsMap) {
    _queryContext = queryContext;
    _starTreeV2 = starTreeV2;
    _aggregationFunctionColumnPairs = aggregationFunctionColumnPairs;
    _groupByExpressions = groupByExpressions;
    _predicateEvaluatorsMap = predicateEvaluatorsMap;
  }

  @Override
  public BaseProjectOperator<?> run() {
    Set<String> projectionColumns = new HashSet<>();
    boolean hasNonIdentifierExpression = false;
    for (AggregationFunctionColumnPair aggregationFunctionColumnPair : _aggregationFunctionColumnPairs) {
      projectionColumns.add(aggregationFunctionColumnPair.toColumnName());
    }
    Set<String> groupByColumns;
    if (_groupByExpressions != null) {
      groupByColumns = new HashSet<>();
      for (ExpressionContext expression : _groupByExpressions) {
        expression.getColumns(groupByColumns);
        if (expression.getType() != ExpressionContext.Type.IDENTIFIER) {
          hasNonIdentifierExpression = true;
        }
      }
      projectionColumns.addAll(groupByColumns);
    } else {
      groupByColumns = null;
    }
    DocIdSetOperator docIdSetOperator =
        new StarTreeDocIdSetPlanNode(_queryContext, _starTreeV2, _predicateEvaluatorsMap, groupByColumns).run();
    Map<String, DataSource> dataSourceMap = new HashMap<>(HashUtil.getHashMapCapacity(projectionColumns.size()));
    projectionColumns.forEach(column -> dataSourceMap.put(column, _starTreeV2.getDataSource(column)));
    ProjectionOperator projectionOperator =
        ProjectionOperatorUtils.getProjectionOperator(dataSourceMap, docIdSetOperator);
    // NOTE: Here we do not put aggregation expressions into TransformOperator based on the following assumptions:
    //       - They are all columns (not functions or constants), where no transform is required
    //       - We never call TransformOperator.getResultColumnContext() on them
    return hasNonIdentifierExpression ? new TransformOperator(_queryContext, projectionOperator,
        Arrays.asList(_groupByExpressions)) : projectionOperator;
  }
}
