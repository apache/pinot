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
import org.apache.pinot.common.utils.request.HavingQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.pql.parsers.Pql2CompilationException;


/**
 * AST node for comparison predicates.
 */
public class ComparisonPredicateAstNode extends PredicateAstNode {
  private String _operand;
  private LiteralAstNode _literal;

  public ComparisonPredicateAstNode(String operand) {
    _operand = operand;
  }

  public String getOperand() {
    return _operand;
  }

  public String getValue() {
    return _literal.getValueAsString();
  }

  @Override
  public void addChild(AstNode childNode) {
    if (childNode instanceof IdentifierAstNode) {
      if (_identifier == null && _function == null) {
        _identifier = ((IdentifierAstNode) childNode).getName();
      } else if (_identifier != null) {
        throw new Pql2CompilationException("Comparison between two columns is not supported.");
      } else {
        throw new Pql2CompilationException("Comparison between function and column is not supported.");
      }
    } else if (childNode instanceof FunctionCallAstNode) {
      if (_function == null && _identifier == null) {
        _function = (FunctionCallAstNode) childNode;
      } else if (_function != null) {
        throw new Pql2CompilationException("Comparison between two functions is not supported.");
      } else {
        throw new Pql2CompilationException("Comparison between column and function is not supported.");
      }
    } else if (childNode instanceof LiteralAstNode) {
      LiteralAstNode node = (LiteralAstNode) childNode;
      if (_literal == null) {
        _literal = node;
      } else {
        throw new Pql2CompilationException("Comparison between two constants is not supported.");
      }
    }

    // Add the child nonetheless
    super.addChild(childNode);
  }

  @Override
  public String toString() {
    return "ComparisonPredicateAstNode{" + "_operand='" + _operand + '\'' + '}';
  }

  /**
   * This function creates a standard unified shaped string for the value and operand side of the comparison
   *
   * @returns A String containing the range representation of the predicate
   */
  private String createRangeStringForComparison() {
    String comparison = null;
    String value = _literal.getValueAsString();

    boolean identifierIsOnLeft = true;
    if (getChildren().get(0) instanceof LiteralAstNode) {
      identifierIsOnLeft = false;
    }
    if ("<".equals(_operand)) {
      if (identifierIsOnLeft) {
        comparison = "(*\t\t" + value + ")";
      } else {
        comparison = "(" + value + "\t\t*)";
      }
    } else if ("<=".equals(_operand)) {
      if (identifierIsOnLeft) {
        comparison = "(*\t\t" + value + "]";
      } else {
        comparison = "[" + value + "\t\t*)";
      }
    } else if (">".equals(_operand)) {
      if (identifierIsOnLeft) {
        comparison = "(" + value + "\t\t*)";
      } else {
        comparison = "(*\t\t" + value + "*)";
      }
    } else if (">=".equals(_operand)) {
      if (identifierIsOnLeft) {
        comparison = "[" + value + "\t\t*)";
      } else {
        comparison = "(*\t\t" + value + "*)";
      }
    }
    return comparison;
  }

  @Override
  public FilterQueryTree buildFilterQueryTree() {
    if (_identifier == null) {
      throw new Pql2CompilationException("Comparison predicate has no identifier");
    }

    if ("=".equals(_operand)) {
      if (_identifier != null && _literal != null) {
        return new FilterQueryTree(_identifier, Collections.singletonList(_literal.getValueAsString()),
            FilterOperator.EQUALITY, null);
      } else {
        throw new Pql2CompilationException("Comparison is not between a column and a constant");
      }
    } else if ("<>".equals(_operand) || "!=".equals(_operand)) {
      if (_identifier != null && _literal != null) {
        return new FilterQueryTree(_identifier, Collections.singletonList(_literal.getValueAsString()),
            FilterOperator.NOT, null);
      } else {
        throw new Pql2CompilationException("Comparison is not between a column and a constant");
      }
    } else {
      String comparison = createRangeStringForComparison();
      if (comparison == null) {
        throw new Pql2CompilationException("The comparison operator is not valid/is not supported for HAVING query");
      }

      if (_identifier != null) {
        return new FilterQueryTree(_identifier, Collections.singletonList(comparison), FilterOperator.RANGE, null);
      } else {
        throw new Pql2CompilationException("One column is needed for comparison.");
      }
    }
  }

  @Override
  public Expression buildFilterExpression() {
    if (_identifier == null) {
      throw new Pql2CompilationException("Comparison predicate has no identifier");
    }

    if ("=".equals(_operand)) {
      if (_identifier != null && _literal != null) {
        Expression expr = RequestUtils.getFunctionExpression(FilterOperator.EQUALITY.name());
        expr.getFunctionCall().addToOperands(RequestUtils.getIdentifierExpression(_identifier));
        expr.getFunctionCall().addToOperands(RequestUtils.getLiteralExpression(_literal.getValueAsString()));
        return expr;
      } else {
        throw new Pql2CompilationException("Comparison is not between a column and a constant");
      }
    } else if ("<>".equals(_operand) || "!=".equals(_operand)) {
      if (_identifier != null && _literal != null) {
        Expression expr = RequestUtils.getFunctionExpression(FilterOperator.NOT.name());
        expr.getFunctionCall().addToOperands(RequestUtils.getIdentifierExpression(_identifier));
        expr.getFunctionCall().addToOperands(RequestUtils.getLiteralExpression(_literal.getValueAsString()));
        return expr;
      } else {
        throw new Pql2CompilationException("Comparison is not between a column and a constant");
      }
    } else {
      String comparison = createRangeStringForComparison();
      if (comparison == null) {
        throw new Pql2CompilationException("The comparison operator is not valid/is not supported for HAVING query");
      }
      if (_identifier != null) {
        Expression expr = RequestUtils.getFunctionExpression(FilterOperator.RANGE.name());
        expr.getFunctionCall().addToOperands(RequestUtils.getIdentifierExpression(_identifier));
        expr.getFunctionCall().addToOperands(RequestUtils.getLiteralExpression(comparison));
        return expr;
      } else {
        throw new Pql2CompilationException("One column is needed for comparison.");
      }
    }
  }

  @Override
  public HavingQueryTree buildHavingQueryTree() {
    if (_function == null) {
      throw new Pql2CompilationException("Comparison predicate has no function");
    }

    if ("=".equals(_operand)) {
      if (_function != null && _literal != null) {
        return new HavingQueryTree(_function.buildAggregationInfo(),
            Collections.singletonList(_literal.getValueAsString()), FilterOperator.EQUALITY, null);
      } else {
        throw new Pql2CompilationException("Comparison is not between a function and a constant");
      }
    } else if ("<>".equals(_operand) || "!=".equals(_operand)) {
      if (_function != null && _literal != null) {
        return new HavingQueryTree(_function.buildAggregationInfo(),
            Collections.singletonList(_literal.getValueAsString()), FilterOperator.NOT, null);
      } else {
        throw new Pql2CompilationException("Comparison is not between a function and a constant");
      }
    } else {
      String comparison = createRangeStringForComparison();
      if (_function != null) {
        return new HavingQueryTree(_function.buildAggregationInfo(), Collections.singletonList(comparison),
            FilterOperator.RANGE, null);
      } else {
        throw new Pql2CompilationException("Function call is needed for comparison.");
      }
    }
  }
}
