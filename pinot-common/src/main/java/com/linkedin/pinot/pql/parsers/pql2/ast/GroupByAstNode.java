/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.common.request.GroupBy;


/**
 * AST node for GROUP BY clauses.
 */
public class GroupByAstNode extends BaseAstNode {

  @Override
  public void updateBrokerRequest(BrokerRequest brokerRequest) {
    GroupBy groupBy = new GroupBy();
    for (AstNode astNode : getChildren()) {
      if (astNode instanceof IdentifierAstNode) {
        IdentifierAstNode node = (IdentifierAstNode) astNode;
        String groupByColumnName = node.getName();
        groupBy.addToColumns(groupByColumnName);

        // List of expression contains columns as well as expressions to maintain ordering of group by columns.
        groupBy.addToExpressions(groupByColumnName);
      } else {
        FunctionCallAstNode functionCallAstNode = (FunctionCallAstNode) astNode;

        // Remove all white-space until we start compiling expressions on broker side.
        groupBy.addToExpressions(functionCallAstNode.getExpression());
      }
    }
    brokerRequest.setGroupBy(groupBy);
  }
}
