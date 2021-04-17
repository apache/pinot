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
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.pql.parsers.Pql2CompilationException;
import org.apache.pinot.spi.utils.CommonConstants.Query.Range;


/**
 * AST for the BETWEEN PQL clause.
 */
public class BetweenPredicateAstNode extends PredicateAstNode {

  @Override
  public void addChild(AstNode childNode) {
    if (childNode instanceof IdentifierAstNode) {
      if (_identifier != null) {
        throw new Pql2CompilationException("BETWEEN predicate has more than one column/function");
      }
      _identifier = ((IdentifierAstNode) childNode).getName();
    } else if (childNode instanceof FunctionCallAstNode) {
      if (_identifier != null) {
        throw new Pql2CompilationException("BETWEEN predicate has more than one column/function");
      }
      _identifier = TransformExpressionTree.getStandardExpression(childNode);
    } else {
      super.addChild(childNode);
    }
  }

  @Override
  public FilterQueryTree buildFilterQueryTree() {
    if (_identifier == null) {
      throw new Pql2CompilationException("Between predicate has no identifier");
    }
    if (getChildren().size() == 2) {
      try {
        LiteralAstNode left = (LiteralAstNode) getChildren().get(0);
        LiteralAstNode right = (LiteralAstNode) getChildren().get(1);
        return new FilterQueryTree(_identifier, Collections.singletonList(Range.LOWER_INCLUSIVE
            + left.getValueAsString() + Range.DELIMITER + right.getValueAsString() + Range.UPPER_INCLUSIVE),
            FilterOperator.RANGE, null);
      } catch (ClassCastException e) {
        throw new Pql2CompilationException("BETWEEN clause was expecting two literal AST nodes, got "
            + getChildren().get(0) + " and " + getChildren().get(1));
      }
    } else {
      throw new Pql2CompilationException("BETWEEN clause does not have two children nodes");
    }
  }

  @Override
  public Expression buildFilterExpression() {
    if (_identifier == null) {
      throw new Pql2CompilationException("Between predicate has no identifier");
    }
    if (getChildren().size() == 2) {
      try {
        LiteralAstNode left = (LiteralAstNode) getChildren().get(0);
        LiteralAstNode right = (LiteralAstNode) getChildren().get(1);

        final Expression betweenExpr = RequestUtils.getFunctionExpression(FilterKind.BETWEEN.name());
        final Function rangeFuncCall = betweenExpr.getFunctionCall();
        rangeFuncCall.addToOperands(RequestUtils.createIdentifierExpression(_identifier));
        rangeFuncCall.addToOperands(RequestUtils.createLiteralExpression(left));
        rangeFuncCall.addToOperands(RequestUtils.createLiteralExpression(right));
        return betweenExpr;
      } catch (ClassCastException e) {
        throw new Pql2CompilationException("BETWEEN clause was expecting two literal AST nodes, got " + getChildren());
      }
    } else {
      throw new Pql2CompilationException("BETWEEN clause does not have two children nodes");
    }
  }
}
