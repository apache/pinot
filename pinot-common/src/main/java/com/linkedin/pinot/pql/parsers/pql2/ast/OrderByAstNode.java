/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.pql.parsers.pql2.ast;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.pql.parsers.Pql2CompilationException;


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
        elem.setColumn(node.getColumn());
        elem.setIsAsc("asc".equalsIgnoreCase(node.getOrdering()));
        selections.addToSelectionSortSequence(elem);
      } else {
        throw new Pql2CompilationException("Child node of ORDER BY node is not an expression node");
      }
    }
  }
}
