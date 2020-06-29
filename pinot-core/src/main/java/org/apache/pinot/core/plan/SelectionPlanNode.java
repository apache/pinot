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

import java.util.List;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.EmptySelectionOperator;
import org.apache.pinot.core.operator.query.SelectionOnlyOperator;
import org.apache.pinot.core.operator.query.SelectionOrderByOperator;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;


/**
 * The <code>SelectionPlanNode</code> class provides the execution plan for selection query on a single segment.
 */
public class SelectionPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;
  private final List<ExpressionContext> _expressions;
  private final TransformPlanNode _transformPlanNode;

  public SelectionPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
    _expressions = SelectionOperatorUtils.extractExpressions(queryContext, indexSegment);
    _transformPlanNode = new TransformPlanNode(_indexSegment, queryContext, _expressions);
  }

  @Override
  public Operator<IntermediateResultsBlock> run() {
    TransformOperator transformOperator = _transformPlanNode.run();
    if (_queryContext.getLimit() > 0) {
      if (_queryContext.getOrderByExpressions() == null) {
        return new SelectionOnlyOperator(_indexSegment, _queryContext, _expressions, transformOperator);
      } else {
        return new SelectionOrderByOperator(_indexSegment, _queryContext, _expressions, transformOperator);
      }
    } else {
      return new EmptySelectionOperator(_indexSegment, _expressions, transformOperator);
    }
  }
}
