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
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.streaming.StreamingSelectionOnlyOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * The {@code StreamingSelectionPlanNode} class provides the execution plan for streaming selection query on a single
 * segment.
 * <p>NOTE: ORDER-BY is ignored for streaming selection query.
 */
public class StreamingSelectionPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;

  public StreamingSelectionPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    Preconditions.checkState(queryContext.getOrderByExpressions() == null,
        "Selection order-by is not supported for streaming");
    _indexSegment = indexSegment;
    _queryContext = queryContext;
  }

  @Override
  public StreamingSelectionOnlyOperator run() {
    List<ExpressionContext> expressions = SelectionOperatorUtils.extractExpressions(_queryContext, _indexSegment);
    BaseProjectOperator<?> projectOperator = new ProjectPlanNode(_indexSegment, _queryContext, expressions,
        Math.min(_queryContext.getLimit(), DocIdSetPlanNode.MAX_DOC_PER_CALL)).run();
    return new StreamingSelectionOnlyOperator(_indexSegment, _queryContext, expressions, projectOperator);
  }
}
