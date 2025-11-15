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
package org.apache.pinot.core.plan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.DocIdSetOperator;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.ProjectionOperatorUtils;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.transform.ArrayJoinOperator;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.request.context.ArrayJoinContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.datasource.DataSource;


/**
 * The <code>ProjectPlanNode</code> provides the execution plan for fetching column values on a single segment.
 */
public class ProjectPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final SegmentContext _segmentContext;
  private final QueryContext _queryContext;
  private final Collection<ExpressionContext> _expressions;
  private final int _maxDocsPerCall;
  private final BaseFilterOperator _filterOperator;

  public ProjectPlanNode(SegmentContext segmentContext, QueryContext queryContext,
      Collection<ExpressionContext> expressions, int maxDocsPerCall, BaseFilterOperator filterOperator) {
    _indexSegment = segmentContext.getIndexSegment();
    _segmentContext = segmentContext;
    _queryContext = queryContext;
    _expressions = expressions;
    _maxDocsPerCall = maxDocsPerCall;
    _filterOperator = filterOperator;
  }

  public ProjectPlanNode(SegmentContext segmentContext, QueryContext queryContext,
      Collection<ExpressionContext> expressions, int maxDocsPerCall) {
    this(segmentContext, queryContext, expressions, maxDocsPerCall,
        new FilterPlanNode(segmentContext, queryContext).run());
  }

  @Override
  public BaseProjectOperator<?> run() {
    Set<ExpressionContext> expressionSet = new HashSet<>();
    List<ExpressionContext> projectionExpressions = new ArrayList<>(_expressions.size());
    for (ExpressionContext expression : _expressions) {
      if (expressionSet.add(expression)) {
        projectionExpressions.add(expression);
      }
    }
    Map<String, ExpressionContext> arrayJoinAliases = new HashMap<>();
    if (!_queryContext.getArrayJoinContexts().isEmpty()) {
      for (ArrayJoinContext arrayJoinContext : _queryContext.getArrayJoinContexts()) {
        for (ArrayJoinContext.Operand operand : arrayJoinContext.getOperands()) {
          ExpressionContext expression = operand.getExpression();
          if (expressionSet.add(expression)) {
            projectionExpressions.add(expression);
          }
          if (operand.getAlias() != null) {
            arrayJoinAliases.put(operand.getAlias(), expression);
          }
        }
      }
    }
    if (shouldProjectFilterExpressions(_queryContext)) {
      addFilterExpressions(_queryContext.getFilter(), expressionSet, projectionExpressions);
    }

    Set<String> projectionColumns = new HashSet<>();
    boolean hasNonIdentifierExpression = false;
    for (ExpressionContext expression : projectionExpressions) {
      if (expression.getType() == ExpressionContext.Type.IDENTIFIER
          && arrayJoinAliases.containsKey(expression.getIdentifier())) {
        continue;
      }
      expression.getColumns(projectionColumns);
      if (expression.getType() != ExpressionContext.Type.IDENTIFIER) {
        hasNonIdentifierExpression = true;
      }
    }
    Map<String, DataSource> dataSourceMap = new HashMap<>(HashUtil.getHashMapCapacity(projectionColumns.size()));
    projectionColumns.forEach(
        column -> dataSourceMap.put(column, _indexSegment.getDataSource(column, _queryContext.getSchema())));
    // NOTE: Skip creating DocIdSetOperator when maxDocsPerCall is 0 (for selection query with LIMIT 0)
    DocIdSetOperator docIdSetOperator = _maxDocsPerCall > 0
        ? new DocIdSetPlanNode(_segmentContext, _queryContext, _maxDocsPerCall, _filterOperator).run()
        : null;

    // TODO: figure out a way to close this operator, as it may hold reader context
    ProjectionOperator projectionOperator =
        ProjectionOperatorUtils.getProjectionOperator(dataSourceMap, docIdSetOperator, _queryContext);
    BaseProjectOperator<?> baseOperator =
        hasNonIdentifierExpression ? new TransformOperator(_queryContext, projectionOperator, projectionExpressions)
            : projectionOperator;
    if (!_queryContext.getArrayJoinContexts().isEmpty()) {
      baseOperator = new ArrayJoinOperator(_queryContext, baseOperator);
    }
    return baseOperator;
  }

  private static boolean shouldProjectFilterExpressions(QueryContext queryContext) {
    if (queryContext.getFilter() == null) {
      return false;
    }
    List<ArrayJoinContext> arrayJoinContexts = queryContext.getArrayJoinContexts();
    if (arrayJoinContexts == null || arrayJoinContexts.isEmpty()) {
      return false;
    }
    Set<String> arrayJoinColumns = new HashSet<>();
    for (ArrayJoinContext context : arrayJoinContexts) {
      for (ArrayJoinContext.Operand operand : context.getOperands()) {
        operand.getExpression().getColumns(arrayJoinColumns);
        String alias = operand.getAlias();
        if (alias != null) {
          arrayJoinColumns.add(alias);
        }
      }
    }
    if (arrayJoinColumns.isEmpty()) {
      return false;
    }
    Set<String> filterColumns = new HashSet<>();
    queryContext.getFilter().getColumns(filterColumns);
    for (String column : filterColumns) {
      if (arrayJoinColumns.contains(column)) {
        return true;
      }
    }
    return false;
  }

  private static void addFilterExpressions(FilterContext filter,
      Set<ExpressionContext> expressionSet, List<ExpressionContext> projectionExpressions) {
    if (filter == null) {
      return;
    }
    switch (filter.getType()) {
      case AND:
      case OR:
        for (FilterContext child : filter.getChildren()) {
          addFilterExpressions(child, expressionSet, projectionExpressions);
        }
        break;
      case NOT:
        addFilterExpressions(filter.getChildren().get(0), expressionSet, projectionExpressions);
        break;
      case PREDICATE:
        ExpressionContext lhs = filter.getPredicate().getLhs();
        if (expressionSet.add(lhs)) {
          projectionExpressions.add(lhs);
        }
        break;
      case CONSTANT:
        break;
      default:
        throw new IllegalStateException("Unsupported filter type: " + filter.getType());
    }
  }
}
