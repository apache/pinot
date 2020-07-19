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

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.operator.streaming.StreamingSelectionOnlyOperator;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;


/**
 * The {@code StreamingSelectionPlanNode} class provides the execution plan for streaming selection query on a single
 * segment.
 * <p>NOTE: ORDER-BY is ignored for streaming selection query.
 */
public class StreamingSelectionPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;
  private final List<ExpressionContext> _expressions;
  private final TransformPlanNode _transformPlanNode;

  public StreamingSelectionPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    Preconditions
        .checkState(queryContext.getOrderByExpressions() == null, "Selection order-by is not supported for streaming");
    _indexSegment = indexSegment;
    _queryContext = queryContext;
    _expressions = SelectionOperatorUtils.extractExpressions(queryContext, indexSegment);
    _transformPlanNode = new TransformPlanNode(_indexSegment, queryContext, _expressions,
        Math.min(queryContext.getLimit(), DocIdSetPlanNode.MAX_DOC_PER_CALL));
  }

  @Override
  public StreamingSelectionOnlyOperator run() {
    return new StreamingSelectionOnlyOperator(_indexSegment, _queryContext, _expressions, _transformPlanNode.run());
  }
}
