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
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.pql.parsers.Pql2CompilationException;
import org.apache.pinot.spi.utils.CommonConstants.Query.Range;


/**
 * AST node for comparison predicates.
 */
public class ComparisonPredicateAstNode extends PredicateAstNode {
  private final String _operand;
  private LiteralAstNode _literal;

  public ComparisonPredicateAstNode(String operand) {
    _operand = operand;
  }

  public LiteralAstNode getLiteral() {
    return _literal;
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
      if (_identifier != null) {
        throw new Pql2CompilationException("Comparison predicate has more than one column/function");
      }
      _identifier = ((IdentifierAstNode) childNode).getName();
    } else if (childNode instanceof FunctionCallAstNode) {
      if (_identifier != null) {
        throw new Pql2CompilationException("Comparison predicate has more than one column/function");
      }
      _identifier = TransformExpressionTree.getStandardExpression(childNode);
    } else if (childNode instanceof LiteralAstNode) {
      if (_literal != null) {
        throw new Pql2CompilationException("Comparison between two constants is not supported");
      }
      _literal = (LiteralAstNode) childNode;
    }

    // Add the child nonetheless
    super.addChild(childNode);
  }

  /**
   * This function creates a standard unified shaped string for the value and operand side of the comparison
   *
   * @return A String containing the range representation of the predicate
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
        comparison = Range.LOWER_UNBOUNDED + value + Range.UPPER_EXCLUSIVE;
      } else {
        comparison = Range.LOWER_EXCLUSIVE + value + Range.UPPER_UNBOUNDED;
      }
    } else if ("<=".equals(_operand)) {
      if (identifierIsOnLeft) {
        comparison = Range.LOWER_UNBOUNDED + value + Range.UPPER_INCLUSIVE;
      } else {
        comparison = Range.LOWER_INCLUSIVE + value + Range.UPPER_UNBOUNDED;
      }
    } else if (">".equals(_operand)) {
      if (identifierIsOnLeft) {
        comparison = Range.LOWER_EXCLUSIVE + value + Range.UPPER_UNBOUNDED;
      } else {
        comparison = Range.LOWER_UNBOUNDED + value + Range.UPPER_EXCLUSIVE;
      }
    } else if (">=".equals(_operand)) {
      if (identifierIsOnLeft) {
        comparison = Range.LOWER_INCLUSIVE + value + Range.UPPER_UNBOUNDED;
      } else {
        comparison = Range.LOWER_UNBOUNDED + value + Range.UPPER_INCLUSIVE;
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
        return new FilterQueryTree(_identifier, Collections.singletonList(_literal.getValueAsString()), FilterOperator.EQUALITY, null);
      } else {
        throw new Pql2CompilationException("Comparison is not between a column and a constant");
      }
    } else if ("<>".equals(_operand) || "!=".equals(_operand)) {
      if (_identifier != null && _literal != null) {
        return new FilterQueryTree(_identifier, Collections.singletonList(_literal.getValueAsString()), FilterOperator.NOT, null);
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
        Expression expr = RequestUtils.getFunctionExpression(FilterKind.EQUALS.name());
        expr.getFunctionCall().addToOperands(RequestUtils.createIdentifierExpression(_identifier));
        expr.getFunctionCall().addToOperands(RequestUtils.createLiteralExpression(_literal));
        return expr;
      } else {
        throw new Pql2CompilationException("Comparison is not between a column and a constant");
      }
    } else if ("<>".equals(_operand) || "!=".equals(_operand)) {
      if (_identifier != null && _literal != null) {
        Expression expr = RequestUtils.getFunctionExpression(FilterKind.NOT_EQUALS.name());
        expr.getFunctionCall().addToOperands(RequestUtils.createIdentifierExpression(_identifier));
        expr.getFunctionCall().addToOperands(RequestUtils.createLiteralExpression(_literal));
        return expr;
      } else {
        throw new Pql2CompilationException("Comparison is not between a column and a constant");
      }
    } else {
      if (_identifier != null) {
        boolean identifierIsOnLeft = true;
        if (getChildren().get(0) instanceof LiteralAstNode) {
          identifierIsOnLeft = false;
        }
        Expression expr = null;
        if ("<".equals(_operand)) {
          if (identifierIsOnLeft) {
            expr = RequestUtils.getFunctionExpression(FilterKind.LESS_THAN.name());
          } else {
            expr = RequestUtils.getFunctionExpression(FilterKind.GREATER_THAN.name());
          }
        } else if ("<=".equals(_operand)) {
          if (identifierIsOnLeft) {
            expr = RequestUtils.getFunctionExpression(FilterKind.LESS_THAN_OR_EQUAL.name());
          } else {
            expr = RequestUtils.getFunctionExpression(FilterKind.GREATER_THAN_OR_EQUAL.name());
          }
        } else if (">".equals(_operand)) {
          if (identifierIsOnLeft) {
            expr = RequestUtils.getFunctionExpression(FilterKind.GREATER_THAN.name());
          } else {
            expr = RequestUtils.getFunctionExpression(FilterKind.LESS_THAN.name());
          }
        } else if (">=".equals(_operand)) {
          if (identifierIsOnLeft) {
            expr = RequestUtils.getFunctionExpression(FilterKind.GREATER_THAN_OR_EQUAL.name());
          } else {
            expr = RequestUtils.getFunctionExpression(FilterKind.LESS_THAN_OR_EQUAL.name());
          }
        }
        if (expr == null) {
          throw new Pql2CompilationException("The comparison operator is not valid/is not supported for HAVING query");
        }
        expr.getFunctionCall().addToOperands(RequestUtils.createIdentifierExpression(_identifier));
        expr.getFunctionCall().addToOperands(RequestUtils.createLiteralExpression(_literal));
        return expr;
      } else {
        throw new Pql2CompilationException("One column is needed for comparison.");
      }
    }
  }
}
