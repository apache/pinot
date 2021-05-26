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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.config.table.TableConfig;


/**
 * ProjectNode for the output of EXPLAIN PLAN queries
 */
public class ProjectNode implements ExplainPlanTreeNode {

  private String _name = "PROJECT";
  private QueryContext _queryContext;
  private Set<String> _projectedCols = new HashSet<>();
  private ExplainPlanTreeNode[] _childNodes; // either filter or scan

  public ProjectNode(QueryContext queryContext, TableConfig tableConfig) {
    _queryContext = queryContext;
    List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();

    if (selectExpressions.size() == 1 && "*".equals(selectExpressions.get(0).getIdentifier())) {
      _projectedCols.add("ALL");
    } else {
      // do not project columns that are only in the filter
      _projectedCols.addAll(queryContext.getProjectCols());
    }
    if (_projectedCols.isEmpty()) {
      // edge case for count(*)
      _projectedCols.add("ALL");
    }
    _childNodes = new ExplainPlanTreeNode[1];
    if (_queryContext.getFilter() != null) {
      _childNodes[0] = new FilterNode(_queryContext, _queryContext.getFilter(), tableConfig);
    } else {
      _childNodes[0] = new ScanNode(queryContext, "ALL", tableConfig.getTableName(), "FULL_SCAN");
    }
  }

  @Override
  public ExplainPlanTreeNode[] getChildNodes() {
    return _childNodes;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder(_name).append('(');
    int count = 0;
    for (String col : _projectedCols) {
      if (count == _projectedCols.size() - 1) {
        stringBuilder.append(col);
      } else {
        stringBuilder.append(col).append(", ");
      }
      count++;
    }
    return stringBuilder.append(')').toString();
  }
}
