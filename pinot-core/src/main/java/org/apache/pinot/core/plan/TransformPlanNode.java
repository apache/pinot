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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.transform.PassThroughTransformOperator;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * The <code>TransformPlanNode</code> class provides the execution plan for transforms on a single segment.
 */
public class TransformPlanNode implements PlanNode {
  private final Collection<ExpressionContext> _expressions;
  private final ProjectionPlanNode _projectionPlanNode;
  private boolean _areAllExpressionsIdentifiers = true;

  public TransformPlanNode(IndexSegment indexSegment, QueryContext queryContext,
      Collection<ExpressionContext> expressions, int maxDocsPerCall) {
    _expressions = expressions;
    Set<String> projectionColumns = new HashSet<>();
    for (ExpressionContext expression : expressions) {
      expression.getColumns(projectionColumns);
      if(expression.getType() != ExpressionContext.Type.IDENTIFIER) _areAllExpressionsIdentifiers = false;
    }
    // NOTE: Skip creating DocIdSetPlanNode when maxDocsPerCall is 0 (for selection query with LIMIT 0).
    DocIdSetPlanNode docIdSetPlanNode =
        maxDocsPerCall > 0 ? new DocIdSetPlanNode(indexSegment, queryContext, maxDocsPerCall) : null;
    _projectionPlanNode = new ProjectionPlanNode(indexSegment, projectionColumns, docIdSetPlanNode);
  }

  @Override
  public TransformOperator run() {
    if(!_areAllExpressionsIdentifiers) {
      return new TransformOperator(_projectionPlanNode.run(), _expressions);
    } else {
      return new PassThroughTransformOperator(_projectionPlanNode.run(), _expressions);
    }
  }
}
