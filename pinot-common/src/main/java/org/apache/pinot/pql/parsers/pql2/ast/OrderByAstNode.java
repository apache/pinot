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

import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.pql.parsers.Pql2CompilationException;


/**
 * AST node for ORDER BY clauses.
 */
public class OrderByAstNode extends BaseAstNode {
  public static final String ASCENDING_ORDER = "asc";
  public static final String DESCENDING_ORDER = "desc";

  @Override
  public void updateBrokerRequest(BrokerRequest brokerRequest) {
    Set<TransformExpressionTree> orderByExpressions = new HashSet<>();
    for (AstNode astNode : getChildren()) {
      if (astNode instanceof OrderByExpressionAstNode) {
        OrderByExpressionAstNode orderByExpressionAstNode = (OrderByExpressionAstNode) astNode;
        TransformExpressionTree orderByExpression =
            TransformExpressionTree.compileToExpressionTree(orderByExpressionAstNode.getColumn());

        // Deduplicate the order-by expressions
        if (orderByExpressions.add(orderByExpression)) {
          SelectionSort selectionSort = new SelectionSort();
          selectionSort.setColumn(orderByExpression.toString());
          selectionSort.setIsAsc(orderByExpressionAstNode.getOrdering().equalsIgnoreCase(ASCENDING_ORDER));
          brokerRequest.addToOrderBy(selectionSort);

          // TODO: Change selection to directly use Order-by. Won't be required if move to PinotQuery happens before that.
          if (brokerRequest.getSelections() != null) {
            brokerRequest.getSelections().addToSelectionSortSequence(selectionSort);
          }
        }
      } else {
        throw new Pql2CompilationException("Child node of ORDER BY node is not an expression node");
      }
    }
  }

  @Override
  public void updatePinotQuery(PinotQuery pinotQuery) {
    Set<TransformExpressionTree> orderByExpressions = new HashSet<>();
    for (AstNode astNode : getChildren()) {
      if (astNode instanceof OrderByExpressionAstNode) {
        OrderByExpressionAstNode orderByExpressionAstNode = (OrderByExpressionAstNode) astNode;
        TransformExpressionTree orderByExpression =
            TransformExpressionTree.compileToExpressionTree(orderByExpressionAstNode.getColumn());

        // Deduplicate the order-by expressions
        if (orderByExpressions.add(orderByExpression)) {
          String ordering = ASCENDING_ORDER.equalsIgnoreCase(orderByExpressionAstNode.getOrdering()) ? ASCENDING_ORDER
              : DESCENDING_ORDER;
          Expression orderByFunctionExpression = RequestUtils.getFunctionExpression(ordering);
          // TODO: Support order-by transform expressions, which should be converted to another FUNCTION expression
          orderByFunctionExpression.getFunctionCall()
              .addToOperands(RequestUtils.createIdentifierExpression(orderByExpression.toString()));
          pinotQuery.addToOrderByList(orderByFunctionExpression);
        }
      } else {
        throw new Pql2CompilationException("Child node of ORDER BY node is not an expression node");
      }
    }
  }
}
