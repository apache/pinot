/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.pql.parsers.Pql2CompilationException;


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
        groupBy.addToColumns(node.getName());
      } else {
        throw new Pql2CompilationException("Child of group by clause is not an identifier.");
      }
    }
    brokerRequest.setGroupBy(groupBy);
  }
}
