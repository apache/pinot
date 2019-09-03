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
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.pql.parsers.Pql2CompilationException;


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
      left = ((IdentifierAstNode) leftNode).getExpression();
    } else {
      throw new Pql2CompilationException("Expected left child node of OptionAstNode to be an identifier");
    }

    if (rightNode instanceof IdentifierAstNode) {
      right = ((IdentifierAstNode) rightNode).getExpression();
    } else if (rightNode instanceof LiteralAstNode) {
      right = ((LiteralAstNode) rightNode).getValueAsString();
    } else {
      throw new Pql2CompilationException("Right node of OptionAstNode is neither an identifier nor a literal");
    }

    brokerRequest.getQueryOptions().put(left, right);
  }

  @Override
  public void updatePinotQuery(PinotQuery pinotQuery) {
    if (getChildren().size() != 2) {
      throw new Pql2CompilationException("Expected exactly two children for OptionAstNode");
    }

    String left;
    AstNode leftNode = getChildren().get(0);

    String right;
    AstNode rightNode = getChildren().get(1);

    if (leftNode instanceof IdentifierAstNode) {
      left = ((IdentifierAstNode) leftNode).getExpression();
    } else {
      throw new Pql2CompilationException("Expected left child node of OptionAstNode to be an identifier");
    }

    if (rightNode instanceof IdentifierAstNode) {
      right = ((IdentifierAstNode) rightNode).getExpression();
    } else if (rightNode instanceof LiteralAstNode) {
      right = ((LiteralAstNode) rightNode).getValueAsString();
    } else {
      throw new Pql2CompilationException("Right node of OptionAstNode is neither an identifier nor a literal");
    }

    pinotQuery.getQueryOptions().put(left, right);
  }
}
