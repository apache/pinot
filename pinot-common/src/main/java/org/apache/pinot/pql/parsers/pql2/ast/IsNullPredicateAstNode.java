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
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.pql.parsers.Pql2CompilationException;


/**
 * AST node for IS predicates (foo IS NULL, foo IS NOT NULL).
 */
public class IsNullPredicateAstNode extends PredicateAstNode {

  private final boolean _isNegation;

  public IsNullPredicateAstNode(boolean notNodeExists) {
    _isNegation = notNodeExists;
  }

  @Override
  public void addChild(AstNode childNode) {
    if (childNode instanceof IdentifierAstNode) {
      if (_identifier != null) {
        throw new Pql2CompilationException("IS predicate has more than one column/function");
      }
      _identifier = ((IdentifierAstNode) childNode).getName();
    } else if (childNode instanceof FunctionCallAstNode) {
      throw new Pql2CompilationException("IS predicate cannot be applied to function");
    } else {
      super.addChild(childNode);
    }
  }

  @Override
  public FilterQueryTree buildFilterQueryTree() {
    if (_identifier == null) {
      throw new Pql2CompilationException("IS predicate has no identifier");
    }
    if (_isNegation) {
      return new FilterQueryTree(_identifier, Collections.emptyList(), FilterOperator.IS_NOT_NULL, null);
    }
    return new FilterQueryTree(_identifier, Collections.emptyList(), FilterOperator.IS_NULL, null);
  }

  @Override
  public Expression buildFilterExpression() {
    if (_identifier == null) {
      throw new Pql2CompilationException("IS predicate has no identifier");
    }
    String filterName = _isNegation ? FilterKind.IS_NOT_NULL.name() : FilterKind.IS_NULL.name();
    Expression expression = RequestUtils.getFunctionExpression(filterName);
    expression.getFunctionCall().addToOperands(RequestUtils.createIdentifierExpression(_identifier));
    return expression;
  }
}
