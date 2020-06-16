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

import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.pql.parsers.Pql2CompilationException;


public class RegexpLikePredicateAstNode extends PredicateAstNode {
  private String _identifier;

  @Override
  public void addChild(AstNode childNode) {
    if (childNode instanceof IdentifierAstNode) {
      if (_identifier != null) {
        throw new Pql2CompilationException("REGEXP_LIKE predicate has more than one column/function");
      }
      _identifier = ((IdentifierAstNode) childNode).getName();
    } else if (childNode instanceof FunctionCallAstNode) {
      if (_identifier != null) {
        throw new Pql2CompilationException("REGEXP_LIKE predicate has more than one column/function");
      }
      _identifier = TransformExpressionTree.getStandardExpression(childNode);
    } else {
      super.addChild(childNode);
    }
  }

  @Override
  public FilterQueryTree buildFilterQueryTree() {
    if (_identifier == null) {
      throw new Pql2CompilationException("REGEXP_LIKE predicate has no identifier");
    }
    List<? extends AstNode> children = getChildren();
    if (children.size() > 1) {
      throw new Pql2CompilationException("Matching more than one regex is NOT supported currently");
    }
    String value = ((LiteralAstNode) children.get(0)).getValueAsString();
    return new FilterQueryTree(_identifier, Collections.singletonList(value), FilterOperator.REGEXP_LIKE, null);
  }

  @Override
  public Expression buildFilterExpression() {
    if (_identifier == null) {
      throw new Pql2CompilationException("REGEXP_LIKE predicate has no identifier");
    }
    Expression expression = RequestUtils.getFunctionExpression(FilterKind.REGEXP_LIKE.name());
    expression.getFunctionCall().addToOperands(RequestUtils.createIdentifierExpression(_identifier));
    if (getChildren().size() > 1) {
      throw new Pql2CompilationException("Matching more than one regex is NOT supported currently");
    }
    for (AstNode astNode : getChildren()) {
      expression.getFunctionCall().addToOperands(RequestUtils.getExpression(astNode));
    }
    return expression;
  }
}
