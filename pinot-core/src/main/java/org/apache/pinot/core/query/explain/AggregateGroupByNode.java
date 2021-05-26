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

package org.apache.pinot.core.query.explain;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.config.table.TableConfig;


/**
 * AggregateGroupByNode for the output of EXPLAIN PLAN queries
 */
public class AggregateGroupByNode implements ExplainPlanTreeNode {

  private static final String _NAME = "AGGREGATE_GROUPBY";
  private List<String> _groupKeys;
  private Set<String> _aggregations = new HashSet<>();
  private ExplainPlanTreeNode[] _childNodes = new ExplainPlanTreeNode[1];

  public AggregateGroupByNode(QueryContext queryContext, Set<String> transforms, TableConfig tableConfig) {

    assert (queryContext.getGroupByExpressions() != null);
    assert (queryContext.getAggregationFunctionIndexMap() != null);
    _groupKeys = new ArrayList<>();
    for (ExpressionContext groupByExpression : queryContext.getGroupByExpressions()) {
      _groupKeys.add(groupByExpression.toString());
    }

    for (FunctionContext aggregationFunc : queryContext.getAggregationFunctionIndexMap().keySet()) {
      _aggregations.add(aggregationFunc.toString());
    }

    if (!transforms.isEmpty()) {
      // transform
      _childNodes[0] = new ApplyTransformNode(queryContext, transforms, tableConfig);
    } else {
      // project
      _childNodes[0] = new ProjectNode(queryContext, tableConfig);
    }
  }

  @Override
  public ExplainPlanTreeNode[] getChildNodes() {
    return _childNodes;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder(_NAME).append("(groupKeys:");
    for (int i = 0; i < _groupKeys.size(); i++) {
      stringBuilder.append(_groupKeys.get(i)).append(',');
    }

    stringBuilder.append("aggregations:");
    int count = 0;
    for (String func : _aggregations) {
      if (count == _aggregations.size() - 1) {
        stringBuilder.append(func);
      } else {
        stringBuilder.append(func).append(", ");
      }
      count++;
    }
    return stringBuilder.append(')').toString();
  }
}
