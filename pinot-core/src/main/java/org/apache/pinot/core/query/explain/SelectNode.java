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
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.config.table.TableConfig;


/**
 * SelectNode for the output of EXPLAIN PLAN queries
 */
public class SelectNode implements ExplainPlanTreeNode {

  private static final String _NAME = "SELECT";
  private ExplainPlanTreeNode[] _childNodes = new ExplainPlanTreeNode[1];
  private List<String> _selectList = new ArrayList<>();

  public SelectNode(QueryContext queryContext, TableConfig tableConfig) {

    List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
    List<String> aliasList = queryContext.getAliasList();

    if (selectExpressions.size() == 1 && "*".equals(selectExpressions.get(0).getIdentifier())) {
      _selectList.add("ALL");
      _childNodes[0] = new BrokerReduceNode(queryContext, tableConfig);
      return;
    }

    List<String> originalCols = new ArrayList<>();
    List<String> aliasCols = new ArrayList<>();
    for (int i = 0; i < selectExpressions.size(); i++) {
      ExpressionContext selectExpression = selectExpressions.get(i);
      if (aliasList.get(i) != null) {
        _selectList.add(aliasList.get(i));
        // TODO: ask how distinct works with alias
        originalCols.add(selectExpression.toString());
        aliasCols.add(aliasList.get(i));
      } else {
        if (selectExpression.getType() == ExpressionContext.Type.FUNCTION && selectExpression.getFunction()
            .getFunctionName().equals("distinct")) {
          // because distinct is an aggregation function, here we need to extract its arguments(columns)
          List<ExpressionContext> distinctArgs = selectExpression.getFunction().getArguments();
          for (int j = 0; j < distinctArgs.size(); j++) {
            _selectList.add(distinctArgs.get(j).toString());
          }
        } else {
          _selectList.add(selectExpressions.get(i).toString());
        }
      }
    }
    assert (originalCols.size() == aliasCols.size());

    if (originalCols.isEmpty()) {
      // no alias update
      _childNodes[0] = new BrokerReduceNode(queryContext, tableConfig);
    } else {
      _childNodes[0] = new UpdateAliasNode(queryContext, originalCols, aliasCols, tableConfig);
    }
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder(_NAME).append("(selectList:");
    if (!_selectList.isEmpty()) {
      stringBuilder.append(_selectList.get(0));
      for (int i = 1; i < _selectList.size(); i++) {
        stringBuilder.append(", ").append(_selectList.get(i));
      }
    }
    return stringBuilder.append(')').toString();
  }

  @Override
  public ExplainPlanTreeNode[] getChildNodes() {
    return _childNodes;
  }
}
