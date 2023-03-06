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
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;


public class InvertedIndexFastCountStarGroupByPlanNode implements PlanNode {

  private final ExpressionContext _groupByExpression;
  private final QueryContext _queryContext;
  private final InvertedIndexFastCountStarGroupByProjectionPlanNode
      _invertedIndexFastCountStarGroupByProjectionPlanNode;

  public InvertedIndexFastCountStarGroupByPlanNode(QueryContext queryContext, ExpressionContext groupByExpression,
      DataSource dataSource) {
    _queryContext = queryContext;
    _groupByExpression = groupByExpression;

    _invertedIndexFastCountStarGroupByProjectionPlanNode =
        new InvertedIndexFastCountStarGroupByProjectionPlanNode(dataSource);
  }

  @Override
  public TransformOperator run() {
    return new TransformOperator(_queryContext, _invertedIndexFastCountStarGroupByProjectionPlanNode.run(),
        List.of(_groupByExpression));
  }
}
