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
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.HavingQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.pql.parsers.Pql2CompilationException;


/**
 * AST for the BETWEEN PQL clause.
 */
public class BetweenPredicateAstNode extends PredicateAstNode {
  @Override
  public void addChild(AstNode childNode) {
    if (childNode instanceof IdentifierAstNode) {
      IdentifierAstNode node = (IdentifierAstNode) childNode;
      _identifier = node.getName();
    } else if (childNode instanceof FunctionCallAstNode) {
      _function = (FunctionCallAstNode) childNode;
    } else {
      super.addChild(childNode);
    }
  }

  public String getLeftValue() {
    return ((LiteralAstNode) getChildren().get(0)).getValueAsString();
  }

  public String getRightValue() {
    return ((LiteralAstNode) getChildren().get(1)).getValueAsString();
  }

  @Override
  public String toString() {
    if (_identifier != null) {
      return "BetweenPredicateAstNode{" + "_identifier='" + _identifier + '\'' + '}';
    } else if (_function != null) {
      return "BetweenPredicateAstNode{" + "_function='" + _function.toString() + '\'' + '}';
    } else {
      return "BetweenPredicateAstNode{_identifier/_function= null}";
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
        return new FilterQueryTree(_identifier,
            Collections.singletonList("[" + left.getValueAsString() + "\t\t" + right.getValueAsString() + "]"),
            FilterOperator.RANGE, null);
      } catch (ClassCastException e) {
        throw new Pql2CompilationException(
            "BETWEEN clause was expecting two literal AST nodes, got " + getChildren().get(0) + " and " + getChildren()
                .get(1));
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

        final Expression betweenExpr = RequestUtils.createFunctionExpression(FilterKind.BETWEEN.name());
        final Function rangeFuncCall = betweenExpr.getFunctionCall();
        rangeFuncCall.addToOperands(RequestUtils.createIdentifierExpression(_identifier));
        rangeFuncCall.addToOperands(RequestUtils.createLiteralExpression(left));
        rangeFuncCall.addToOperands(RequestUtils.createLiteralExpression(right));
        return betweenExpr;
      } catch (ClassCastException e) {
        throw new Pql2CompilationException(
            "BETWEEN clause was expecting two literal AST nodes, got " + getChildren());
      }
    } else {
      throw new Pql2CompilationException("BETWEEN clause does not have two children nodes");
    }
  }

  @Override
  public HavingQueryTree buildHavingQueryTree() {
    if (_function == null) {
      throw new Pql2CompilationException("Between predicate has no function call specified");
    }
    if (getChildren().size() == 2) {
      try {
        LiteralAstNode left = (LiteralAstNode) getChildren().get(0);
        LiteralAstNode right = (LiteralAstNode) getChildren().get(1);
        return new HavingQueryTree(_function.buildAggregationInfo(),
            Collections.singletonList("[" + left.getValueAsString() + "\t\t" + right.getValueAsString() + "]"),
            FilterOperator.RANGE, null);
      } catch (ClassCastException e) {
        throw new Pql2CompilationException(
            "BETWEEN clause was expecting two literal AST nodes, got " + getChildren());
      }
    } else {
      throw new Pql2CompilationException("BETWEEN clause does not have two children nodes");
    }
  }
}
