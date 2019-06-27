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
package org.apache.pinot.pql.parsers.pql2.ast;

import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.pql.parsers.Pql2CompilationException;


/**
 * AST node for ORDER BY clauses.
 */
public class OrderByAstNode extends BaseAstNode {
  @Override
  public void updateBrokerRequest(BrokerRequest brokerRequest) {
    Selection selections = brokerRequest.getSelections();

    for (AstNode astNode : getChildren()) {
      if (astNode instanceof OrderByExpressionAstNode) {
        OrderByExpressionAstNode node = (OrderByExpressionAstNode) astNode;
        SelectionSort elem = new SelectionSort();
        TransformExpressionTree transformExpressionTree =
            TransformExpressionTree.compileToExpressionTree(node.getColumn());
        elem.setColumn(transformExpressionTree.toString());
        elem.setIsAsc("asc".equalsIgnoreCase(node.getOrdering()));
        selections.addToSelectionSortSequence(elem);
      } else {
        throw new Pql2CompilationException("Child node of ORDER BY node is not an expression node");
      }
    }
  }

  @Override
  public void updatePinotQuery(PinotQuery pinotQuery) {
    for (AstNode astNode : getChildren()) {
      if (astNode instanceof OrderByExpressionAstNode) {
        OrderByExpressionAstNode node = (OrderByExpressionAstNode) astNode;
        String ordering = "desc";
        if ("asc".equalsIgnoreCase(node.getOrdering())) {
          ordering = "asc";
        }
        Expression orderByExpression = RequestUtils.createFunctionExpression(ordering);
        Function orderByFunc = orderByExpression.getFunctionCall();
        Expression colExpr = RequestUtils.createIdentifierExpression(node.getColumn());
        orderByFunc.addToOperands(colExpr);
        pinotQuery.addToOrderByList(orderByExpression);
      } else {
        throw new Pql2CompilationException("Child node of ORDER BY node is not an expression node");
      }
    }
  }
}
