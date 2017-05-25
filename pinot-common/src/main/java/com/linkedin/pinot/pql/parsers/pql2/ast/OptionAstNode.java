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
import com.linkedin.pinot.pql.parsers.Pql2CompilationException;


/**
 * AST node for a query option.
 */
public class OptionAstNode extends BaseAstNode {
  @Override
  public void updateBrokerRequest(BrokerRequest brokerRequest) {
    if (getChildren().size() != 2) {
      throw new Pql2CompilationException("Expected exactly two children for OptionAstNode");
    }

    String left;
    AstNode leftNode = getChildren().get(0);

    String right;
    AstNode rightNode = getChildren().get(1);

    if (leftNode instanceof IdentifierAstNode) {
      left = ((IdentifierAstNode) leftNode).getName();
    } else {
      throw new Pql2CompilationException("Expected left child node of OptionAstNode to be an identifier");
    }

    if (rightNode instanceof IdentifierAstNode) {
      right = ((IdentifierAstNode) rightNode).getName();
    } else if (rightNode instanceof LiteralAstNode) {
      right = ((LiteralAstNode) rightNode).getValueAsString();
    } else {
      throw new Pql2CompilationException("Right node of OptionAstNode is neither an identifier nor a literal");
    }

    brokerRequest.getQueryOptions().put(left, right);
  }
}
