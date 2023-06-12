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
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.DocIdSetOperator;
import org.apache.pinot.core.operator.LocalJoinOperator;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.request.context.JoinContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;


/**
 * The <code>ProjectPlanNode</code> provides the execution plan for fetching column values on a single segment.
 */
public class ProjectPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;
  private final Collection<ExpressionContext> _expressions;
  private final int _maxDocsPerCall;
  private final BaseFilterOperator _filterOperator;

  public ProjectPlanNode(IndexSegment indexSegment, QueryContext queryContext,
      Collection<ExpressionContext> expressions, int maxDocsPerCall, @Nullable BaseFilterOperator filterOperator) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
    _expressions = expressions;
    _maxDocsPerCall = maxDocsPerCall;
    _filterOperator = filterOperator;
  }

  public ProjectPlanNode(IndexSegment indexSegment, QueryContext queryContext,
      Collection<ExpressionContext> expressions, int maxDocsPerCall) {
    this(indexSegment, queryContext, expressions, maxDocsPerCall, null);
  }

  @Override
  public BaseProjectOperator<?> run() {
    return _queryContext.hasJoin() ? createProjectOperatorWithJoin() : createProjectOperatorWithoutJoin();
  }

  private BaseProjectOperator<?> createProjectOperatorWithoutJoin() {
    Set<String> projectionColumns = new HashSet<>();
    boolean hasNonIdentifierExpression = false;
    for (ExpressionContext expression : _expressions) {
      expression.getColumns(projectionColumns);
      if (expression.getType() != ExpressionContext.Type.IDENTIFIER) {
        hasNonIdentifierExpression = true;
      }
    }
    ProjectionOperator projectionOperator = createProjectionOperator(projectionColumns);
    return hasNonIdentifierExpression ? new TransformOperator(_queryContext, projectionOperator, _expressions)
        : projectionOperator;
  }

  private BaseProjectOperator<?> createProjectOperatorWithJoin() {
    Set<String> joinProjectColumns = new HashSet<>();
    boolean hasNonIdentifierExpressionAfterJoin = false;
    for (ExpressionContext expression : _expressions) {
      expression.getColumns(joinProjectColumns);
      if (expression.getType() != ExpressionContext.Type.IDENTIFIER) {
        hasNonIdentifierExpressionAfterJoin = true;
      }
    }

    JoinContext join = _queryContext.getDataSource().getJoin();
    String columnPrefix = join.getRawLeftTableName() + ".";
    int prefixLength = columnPrefix.length();
    Set<String> projectionColumns = new HashSet<>();
    for (String column : joinProjectColumns) {
      if (column.startsWith(columnPrefix)) {
        projectionColumns.add(column.substring(prefixLength));
      }
    }

    BaseProjectOperator<?> projectOperatorBeforeJoin;
    ExpressionContext leftJoinKey = join.getLeftJoinKey();
    if (leftJoinKey.getType() == ExpressionContext.Type.IDENTIFIER) {
      // No transform before JOIN
      projectionColumns.add(leftJoinKey.getIdentifier());
      projectOperatorBeforeJoin = createProjectionOperator(projectionColumns);
    } else {
      // Need transform before JOIN
      List<ExpressionContext> expressionsBeforeJoin = new ArrayList<>(projectionColumns.size() + 1);
      expressionsBeforeJoin.add(leftJoinKey);
      for (String column : projectionColumns) {
        expressionsBeforeJoin.add(ExpressionContext.forIdentifier(column));
      }
      leftJoinKey.getColumns(projectionColumns);
      ProjectionOperator projectionOperator = createProjectionOperator(projectionColumns);
      projectOperatorBeforeJoin = new TransformOperator(_queryContext, projectionOperator, expressionsBeforeJoin);
    }

    LocalJoinOperator localJoinOperator =
        new LocalJoinOperator(_queryContext, projectOperatorBeforeJoin, join.getRawLeftTableName(),
            join.getRightTableName(), leftJoinKey, join.getRightJoinKey(), joinProjectColumns.toArray(new String[0]));
    return hasNonIdentifierExpressionAfterJoin ? new TransformOperator(_queryContext, localJoinOperator, _expressions)
        : localJoinOperator;
  }

  private ProjectionOperator createProjectionOperator(Set<String> projectionColumns) {
    Map<String, DataSource> dataSourceMap = new HashMap<>(HashUtil.getHashMapCapacity(projectionColumns.size()));
    for (String column : projectionColumns) {
      dataSourceMap.put(column, _indexSegment.getDataSource(column));
    }
    // NOTE: Skip creating DocIdSetOperator when maxDocsPerCall is 0 (for selection query with LIMIT 0)
    DocIdSetOperator docIdSetOperator =
        _maxDocsPerCall > 0 ? new DocIdSetPlanNode(_indexSegment, _queryContext, _maxDocsPerCall, _filterOperator).run()
            : null;
    return new ProjectionOperator(dataSourceMap, docIdSetOperator);
  }
}
