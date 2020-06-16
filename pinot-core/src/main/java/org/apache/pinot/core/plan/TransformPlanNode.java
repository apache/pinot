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

import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;


/**
 * The <code>TransformPlanNode</code> class provides the execution plan for transforms on a single segment.
 */
public class TransformPlanNode implements PlanNode {
  private final ProjectionPlanNode _projectionPlanNode;
  private final Set<TransformExpressionTree> _expressions;
  private int _maxDocPerNextCall = DocIdSetPlanNode.MAX_DOC_PER_CALL;

  public TransformPlanNode(IndexSegment indexSegment, QueryContext queryContext,
      Set<TransformExpressionTree> expressionsToPlan) {
    setMaxDocsForSelection(queryContext);
    Set<String> projectionColumns = new HashSet<>();
    extractProjectionColumns(expressionsToPlan, projectionColumns);

    _expressions = expressionsToPlan;
    _projectionPlanNode = new ProjectionPlanNode(indexSegment, projectionColumns,
        new DocIdSetPlanNode(indexSegment, queryContext, _maxDocPerNextCall));
  }

  private void extractProjectionColumns(Set<TransformExpressionTree> expressionsToPlan, Set<String> projectionColumns) {
    for (TransformExpressionTree expression : expressionsToPlan) {
      extractProjectionColumns(expression, projectionColumns);
    }
  }

  private void extractProjectionColumns(TransformExpressionTree expression, Set<String> projectionColumns) {
    TransformExpressionTree.ExpressionType expressionType = expression.getExpressionType();
    switch (expressionType) {
      case FUNCTION:
        for (TransformExpressionTree child : expression.getChildren()) {
          extractProjectionColumns(child, projectionColumns);
        }
        break;

      case IDENTIFIER:
        projectionColumns.add(expression.getValue());
        break;

      case LITERAL:
        // Do nothing.
        break;

      default:
        throw new UnsupportedOperationException("Unsupported expression type: " + expressionType);
    }
  }

  /**
   * Helper method to set the max number of docs to return for selection queries
   */
  private void setMaxDocsForSelection(QueryContext queryContext) {
    if (!QueryContextUtils.isAggregationQuery(queryContext)) {
      // Selection queries
      if (queryContext.getLimit() > 0) {
        if (queryContext.getOrderByExpressions() == null) {
          // For selection-only queries, select minimum number of documents
          _maxDocPerNextCall = Math.min(queryContext.getLimit(), _maxDocPerNextCall);
        }
      } else {
        // For LIMIT 0 queries, fetch at least 1 document per DocIdSetPlanNode's requirement
        // TODO: Skip the filtering phase and document fetching for LIMIT 0 case
        _maxDocPerNextCall = 1;
      }
    }
  }

  @Override
  public TransformOperator run() {
    return new TransformOperator(_projectionPlanNode.run(), _expressions);
  }
}
