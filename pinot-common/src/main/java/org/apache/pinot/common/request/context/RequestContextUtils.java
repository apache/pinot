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
package org.apache.pinot.common.request.context;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.function.FunctionDefinitionRegistry;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.IsNotNullPredicate;
import org.apache.pinot.common.request.context.predicate.IsNullPredicate;
import org.apache.pinot.common.request.context.predicate.JsonMatchPredicate;
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.common.request.context.predicate.RegexpLikePredicate;
import org.apache.pinot.common.request.context.predicate.TextMatchPredicate;
import org.apache.pinot.common.utils.LikeToRegexpLikePatternConverterUtils;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.pql.parsers.pql2.ast.AstNode;
import org.apache.pinot.pql.parsers.pql2.ast.FilterKind;
import org.apache.pinot.pql.parsers.pql2.ast.FunctionCallAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.IdentifierAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.LiteralAstNode;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


public class RequestContextUtils {
  private RequestContextUtils() {
  }

  private static final Pql2Compiler PQL_COMPILER = new Pql2Compiler();

  /**
   * Converts the given SQL expression into an {@link ExpressionContext}.
   */
  public static ExpressionContext getExpressionFromSQL(String sqlExpression) {
    if (sqlExpression.equals("*")) {
      // For 'SELECT *' and 'SELECT COUNT(*)'
      return ExpressionContext.forIdentifier("*");
    } else {
      return getExpression(CalciteSqlParser.compileToExpression(sqlExpression));
    }
  }

  /**
   * Converts the given Thrift {@link Expression} into an {@link ExpressionContext}.
   */
  public static ExpressionContext getExpression(Expression thriftExpression) {
    switch (thriftExpression.getType()) {
      case LITERAL:
        return ExpressionContext.forLiteral(thriftExpression.getLiteral().getFieldValue().toString());
      case IDENTIFIER:
        return ExpressionContext.forIdentifier(thriftExpression.getIdentifier().getName());
      case FUNCTION:
        return ExpressionContext.forFunction(getFunction(thriftExpression.getFunctionCall()));
      default:
        throw new IllegalStateException();
    }
  }

  /**
   * Converts the given PQL expression into an {@link ExpressionContext}.
   */
  public static ExpressionContext getExpressionFromPQL(String pqlExpression) {
    if (pqlExpression.equals("*")) {
      // For 'SELECT *' and 'SELECT COUNT(*)'
      return ExpressionContext.forIdentifier("*");
    } else {
      return getExpression(PQL_COMPILER.parseToAstNode(pqlExpression));
    }
  }

  /**
   * Converts the given {@link AstNode} into an {@link ExpressionContext}.
   */
  public static ExpressionContext getExpression(AstNode astNode) {
    if (astNode instanceof IdentifierAstNode) {
      return ExpressionContext.forIdentifier(((IdentifierAstNode) astNode).getName());
    }
    if (astNode instanceof FunctionCallAstNode) {
      return ExpressionContext.forFunction(getFunction((FunctionCallAstNode) astNode));
    }
    if (astNode instanceof LiteralAstNode) {
      return ExpressionContext.forLiteral(((LiteralAstNode) astNode).getValueAsString());
    }
    throw new IllegalStateException();
  }

  /**
   * Converts the given Thrift {@link Function} into a {@link FunctionContext}.
   */
  public static FunctionContext getFunction(Function thriftFunction) {
    String functionName = thriftFunction.getOperator();
    if (functionName.equalsIgnoreCase(AggregationFunctionType.COUNT.getName())) {
      // NOTE: COUNT always take one single argument "*"
      return new FunctionContext(FunctionContext.Type.AGGREGATION, AggregationFunctionType.COUNT.getName(),
          Collections.singletonList(ExpressionContext.forIdentifier("*")));
    }
    FunctionContext.Type functionType =
        FunctionDefinitionRegistry.isAggFunc(functionName) ? FunctionContext.Type.AGGREGATION
            : FunctionContext.Type.TRANSFORM;
    List<Expression> operands = thriftFunction.getOperands();
    if (operands != null) {
      List<ExpressionContext> arguments = new ArrayList<>(operands.size());
      for (Expression operand : operands) {
        arguments.add(getExpression(operand));
      }
      return new FunctionContext(functionType, functionName, arguments);
    } else {
      return new FunctionContext(functionType, functionName, Collections.emptyList());
    }
  }

  /**
   * Converts the given {@link FunctionCallAstNode} into a {@link FunctionContext}.
   */
  public static FunctionContext getFunction(FunctionCallAstNode astNode) {
    String functionName = astNode.getName();
    if (functionName.equalsIgnoreCase(AggregationFunctionType.COUNT.getName())) {
      // NOTE: COUNT always take one single argument "*"
      return new FunctionContext(FunctionContext.Type.AGGREGATION, AggregationFunctionType.COUNT.getName(),
          Collections.singletonList(ExpressionContext.forIdentifier("*")));
    }
    FunctionContext.Type functionType =
        FunctionDefinitionRegistry.isAggFunc(functionName) ? FunctionContext.Type.AGGREGATION
            : FunctionContext.Type.TRANSFORM;
    List<? extends AstNode> children = astNode.getChildren();
    if (children != null) {
      List<ExpressionContext> arguments = new ArrayList<>(children.size());
      for (AstNode child : children) {
        arguments.add(getExpression(child));
      }
      return new FunctionContext(functionType, functionName, arguments);
    } else {
      return new FunctionContext(functionType, functionName, Collections.emptyList());
    }
  }

  /**
   * Converts the given Thrift {@link Expression} into a {@link FilterContext}.
   * <p>NOTE: Currently the query engine only accepts string literals as the right-hand side of the predicate, so we
   *          always convert the right-hand side expressions into strings.
   */
  public static FilterContext getFilter(ExpressionContext expressionContext) {
    FunctionContext thriftFunction = expressionContext.getFunction();
    FunctionContext innerFunction = thriftFunction.getArguments().get(1).getFunction();
    FilterKind filterKind = FilterKind.valueOf(innerFunction.getFunctionName().toUpperCase());
    List<ExpressionContext> operands = innerFunction.getArguments();
    int numOperands = operands.size();
    switch (filterKind) {
      case AND:
        List<FilterContext> children = new ArrayList<>(numOperands);
        for (ExpressionContext operand : operands) {
          children.add(getFilter(operand));
        }
        return new FilterContext(FilterContext.Type.AND, children, null);
      case OR:
        children = new ArrayList<>(numOperands);
        for (ExpressionContext operand : operands) {
          children.add(getFilter(operand));
        }
        return new FilterContext(FilterContext.Type.OR, children, null);
      case EQUALS:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new EqPredicate(operands.get(0), getStringValue(operands.get(1))));
      case NOT_EQUALS:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new NotEqPredicate(operands.get(0), getStringValue(operands.get(1))));
      case IN:
        List<String> values = new ArrayList<>(numOperands - 1);
        for (int i = 1; i < numOperands; i++) {
          values.add(getStringValue(operands.get(i)));
        }
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new InPredicate(operands.get(0), values));
      case NOT_IN:
        values = new ArrayList<>(numOperands - 1);
        for (int i = 1; i < numOperands; i++) {
          values.add(getStringValue(operands.get(i)));
        }
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new NotInPredicate(operands.get(0), values));
      case GREATER_THAN:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new RangePredicate(operands.get(0), false, getStringValue(operands.get(1)), false,
                RangePredicate.UNBOUNDED));
      case GREATER_THAN_OR_EQUAL:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new RangePredicate(operands.get(0), true, getStringValue(operands.get(1)), false,
                RangePredicate.UNBOUNDED));
      case LESS_THAN:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new RangePredicate(operands.get(0), false, RangePredicate.UNBOUNDED, false,
                getStringValue(operands.get(1))));
      case LESS_THAN_OR_EQUAL:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new RangePredicate(operands.get(0), false, RangePredicate.UNBOUNDED, true,
                getStringValue(operands.get(1))));
      case BETWEEN:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new RangePredicate(operands.get(0), true, getStringValue(operands.get(1)), true,
                getStringValue(operands.get(2))));
      case RANGE:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new RangePredicate(operands.get(0), getStringValue(operands.get(1))));
      case REGEXP_LIKE:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new RegexpLikePredicate(operands.get(0), getStringValue(operands.get(1))));
      case LIKE:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new RegexpLikePredicate(operands.get(0),
                LikeToRegexpLikePatternConverterUtils.processValue(getStringValue(operands.get(1)))));
      case TEXT_MATCH:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new TextMatchPredicate(operands.get(0), getStringValue(operands.get(1))));
      case JSON_MATCH:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new JsonMatchPredicate(operands.get(0), getStringValue(operands.get(1))));
      case IS_NULL:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new IsNullPredicate(operands.get(0)));
      case IS_NOT_NULL:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new IsNotNullPredicate(operands.get(0)));
      default:
        throw new IllegalStateException();
    }
  }

  /**
   * Converts the given Thrift {@link Expression} into a {@link FilterContext}.
   * <p>NOTE: Currently the query engine only accepts string literals as the right-hand side of the predicate, so we
   *          always convert the right-hand side expressions into strings.
   */
  public static FilterContext getFilter(Expression thriftExpression) {
    Function thriftFunction = thriftExpression.getFunctionCall();
    FilterKind filterKind = FilterKind.valueOf(thriftFunction.getOperator().toUpperCase());
    List<Expression> operands = thriftFunction.getOperands();
    int numOperands = operands.size();
    switch (filterKind) {
      case AND:
        List<FilterContext> children = new ArrayList<>(numOperands);
        for (Expression operand : operands) {
          children.add(getFilter(operand));
        }
        return new FilterContext(FilterContext.Type.AND, children, null);
      case OR:
        children = new ArrayList<>(numOperands);
        for (Expression operand : operands) {
          children.add(getFilter(operand));
        }
        return new FilterContext(FilterContext.Type.OR, children, null);
      case EQUALS:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new EqPredicate(getExpression(operands.get(0)), getStringValue(operands.get(1))));
      case NOT_EQUALS:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new NotEqPredicate(getExpression(operands.get(0)), getStringValue(operands.get(1))));
      case IN:
        List<String> values = new ArrayList<>(numOperands - 1);
        for (int i = 1; i < numOperands; i++) {
          values.add(getStringValue(operands.get(i)));
        }
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new InPredicate(getExpression(operands.get(0)), values));
      case NOT_IN:
        values = new ArrayList<>(numOperands - 1);
        for (int i = 1; i < numOperands; i++) {
          values.add(getStringValue(operands.get(i)));
        }
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new NotInPredicate(getExpression(operands.get(0)), values));
      case GREATER_THAN:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new RangePredicate(getExpression(operands.get(0)), false, getStringValue(operands.get(1)), false,
                RangePredicate.UNBOUNDED));
      case GREATER_THAN_OR_EQUAL:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new RangePredicate(getExpression(operands.get(0)), true, getStringValue(operands.get(1)), false,
                RangePredicate.UNBOUNDED));
      case LESS_THAN:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new RangePredicate(getExpression(operands.get(0)), false, RangePredicate.UNBOUNDED, false,
                getStringValue(operands.get(1))));
      case LESS_THAN_OR_EQUAL:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new RangePredicate(getExpression(operands.get(0)), false, RangePredicate.UNBOUNDED, true,
                getStringValue(operands.get(1))));
      case BETWEEN:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new RangePredicate(getExpression(operands.get(0)), true, getStringValue(operands.get(1)), true,
                getStringValue(operands.get(2))));
      case RANGE:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new RangePredicate(getExpression(operands.get(0)), getStringValue(operands.get(1))));
      case REGEXP_LIKE:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new RegexpLikePredicate(getExpression(operands.get(0)), getStringValue(operands.get(1))));
      case LIKE:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new RegexpLikePredicate(getExpression(operands.get(0)),
                LikeToRegexpLikePatternConverterUtils.processValue(getStringValue(operands.get(1)))));
      case TEXT_MATCH:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new TextMatchPredicate(getExpression(operands.get(0)), getStringValue(operands.get(1))));
      case JSON_MATCH:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new JsonMatchPredicate(getExpression(operands.get(0)), getStringValue(operands.get(1))));
      case IS_NULL:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new IsNullPredicate(getExpression(operands.get(0))));
      case IS_NOT_NULL:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new IsNotNullPredicate(getExpression(operands.get(0))));
      default:
        throw new IllegalStateException();
    }
  }

  private static String getStringValue(Expression thriftExpression) {
    if (thriftExpression.getType() != ExpressionType.LITERAL) {
      throw new BadQueryRequestException(
          "Pinot does not support column or function on the right-hand side of the predicate");
    }
    return thriftExpression.getLiteral().getFieldValue().toString();
  }

  private static String getStringValue(ExpressionContext expressionContext) {
    if (expressionContext.getType() != ExpressionContext.Type.LITERAL) {
      throw new BadQueryRequestException(
          "Pinot does not support column or function on the right-hand side of the predicate");
    }
    return expressionContext.getLiteral();
  }

  /**
   * Converts the given {@link FilterQueryTree} into a {@link FilterContext}.
   */
  public static FilterContext getFilter(FilterQueryTree node) {
    FilterOperator filterOperator = node.getOperator();
    switch (filterOperator) {
      case AND:
        List<FilterQueryTree> childNodes = node.getChildren();
        List<FilterContext> children = new ArrayList<>(childNodes.size());
        for (FilterQueryTree childNode : childNodes) {
          children.add(getFilter(childNode));
        }
        return new FilterContext(FilterContext.Type.AND, children, null);
      case OR:
        childNodes = node.getChildren();
        children = new ArrayList<>(childNodes.size());
        for (FilterQueryTree childNode : childNodes) {
          children.add(getFilter(childNode));
        }
        return new FilterContext(FilterContext.Type.OR, children, null);
      case EQUALITY:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new EqPredicate(getExpressionFromPQL(node.getColumn()), node.getValue().get(0)));
      case NOT:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new NotEqPredicate(getExpressionFromPQL(node.getColumn()), node.getValue().get(0)));
      case IN:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new InPredicate(getExpressionFromPQL(node.getColumn()), node.getValue()));
      case NOT_IN:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new NotInPredicate(getExpressionFromPQL(node.getColumn()), node.getValue()));
      case RANGE:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new RangePredicate(getExpressionFromPQL(node.getColumn()), node.getValue().get(0)));
      case REGEXP_LIKE:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new RegexpLikePredicate(getExpressionFromPQL(node.getColumn()), node.getValue().get(0)));
      case TEXT_MATCH:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new TextMatchPredicate(getExpressionFromPQL(node.getColumn()), node.getValue().get(0)));
      case JSON_MATCH:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new JsonMatchPredicate(getExpressionFromPQL(node.getColumn()), node.getValue().get(0)));
      case IS_NULL:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new IsNullPredicate(getExpressionFromPQL(node.getColumn())));
      case IS_NOT_NULL:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new IsNotNullPredicate(getExpressionFromPQL(node.getColumn())));
      default:
        throw new IllegalStateException();
    }
  }
}
