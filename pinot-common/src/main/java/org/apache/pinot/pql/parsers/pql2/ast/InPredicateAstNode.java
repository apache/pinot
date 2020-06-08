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

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.HavingQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.pql.parsers.Pql2CompilationException;


/**
 * AST node for IN predicates.
 */
public class InPredicateAstNode extends PredicateAstNode {
  private final boolean _isNotInClause;

  public InPredicateAstNode(boolean isNotInClause) {
    _isNotInClause = isNotInClause;
  }

  public boolean isNotInClause() {
    return _isNotInClause;
  }

  public ArrayList<String> getValues() {
    ArrayList<String> values = new ArrayList<>();
    for (AstNode astNode : getChildren()) {
      if (astNode instanceof LiteralAstNode) {
        LiteralAstNode node = (LiteralAstNode) astNode;
        values.add(node.getValueAsString());
      }
    }
    return values;
  }

  @Override
  public void addChild(AstNode childNode) {
    if (childNode instanceof IdentifierAstNode) {
      if (_identifier == null && _function == null) {
        IdentifierAstNode node = (IdentifierAstNode) childNode;
        _identifier = node.getName();
      } else if (_identifier != null) {
        throw new Pql2CompilationException("IN predicate has more than one identifier.");
      } else {
        throw new Pql2CompilationException("IN predicate has both identifier and function.");
      }
    } else if (childNode instanceof FunctionCallAstNode) {
      if (_function == null && _identifier == null) {
        _function = (FunctionCallAstNode) childNode;
        _identifier = TransformExpressionTree.getStandardExpression(childNode);
      } else if (_function != null) {
        throw new Pql2CompilationException("IN predicate has more than one function.");
      } else {
        throw new Pql2CompilationException("IN predicate has both identifier and function.");
      }
    } else {
      super.addChild(childNode);
    }
  }

  @Override
  public String toString() {
    if (_identifier != null) {
      return "InPredicateAstNode{" + "_identifier='" + _identifier + '\'' + '}';
    } else if (_function != null) {
      return "InPredicateAstNode{" + "_function='" + _function.toString() + '\'' + '}';
    } else {
      return "InPredicateAstNode{_identifier/_function= null";
    }
  }

  @Override
  public FilterQueryTree buildFilterQueryTree() {
    if (_identifier == null) {
      throw new Pql2CompilationException("IN predicate has no identifier");
    }

    List<? extends AstNode> children = getChildren();
    int numChildren = children.size();
    List<String> values = new ArrayList<>(numChildren);
    for (AstNode child : children) {
      if (child instanceof LiteralAstNode) {
        values.add(((LiteralAstNode) child).getValueAsString());
      }
    }

    FilterOperator filterOperator = _isNotInClause ? FilterOperator.NOT_IN : FilterOperator.IN;
    return new FilterQueryTree(_identifier, values, filterOperator, null);
  }

  @Override
  public Expression buildFilterExpression() {
    if (_identifier == null) {
      throw new Pql2CompilationException("IN predicate has no identifier");
    }

    List<? extends AstNode> children = getChildren();
    int numChildren = children.size();
    List<Expression> operands = new ArrayList<>(numChildren + 1);
    operands.add(RequestUtils.createIdentifierExpression(_identifier));
    for (AstNode child : children) {
      if (child instanceof LiteralAstNode) {
        operands.add(RequestUtils.createLiteralExpression((LiteralAstNode) child));
      }
    }

    FilterKind filterKind = _isNotInClause ? FilterKind.NOT_IN : FilterKind.IN;
    Expression functionExpression = RequestUtils.getFunctionExpression(filterKind.name());
    functionExpression.getFunctionCall().setOperands(operands);
    return functionExpression;
  }

  @Override
  public HavingQueryTree buildHavingQueryTree() {
    if (_function == null) {
      throw new Pql2CompilationException("IN predicate has no function");
    }

    TreeSet<String> values = new TreeSet<>();
    for (AstNode astNode : getChildren()) {
      if (astNode instanceof LiteralAstNode) {
        LiteralAstNode node = (LiteralAstNode) astNode;
        values.add(node.getValueAsString());
      }
    }

    FilterOperator filterOperator = _isNotInClause ? FilterOperator.NOT_IN : FilterOperator.IN;
    return new HavingQueryTree(_function.buildAggregationInfo(), new ArrayList<>(values), filterOperator, null);
  }
}
