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
import org.apache.commons.lang3.EnumUtils;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.IsNotNullPredicate;
import org.apache.pinot.common.request.context.predicate.IsNullPredicate;
import org.apache.pinot.common.request.context.predicate.JsonMatchPredicate;
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.common.request.context.predicate.RegexpLikePredicate;
import org.apache.pinot.common.request.context.predicate.TextContainsPredicate;
import org.apache.pinot.common.request.context.predicate.TextMatchPredicate;
import org.apache.pinot.common.request.context.predicate.VectorSimilarityPredicate;
import org.apache.pinot.common.utils.RegexpPatternConverterUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


public class RequestContextUtils {
  private RequestContextUtils() {
  }

  /**
   * Converts the given string expression into an {@link ExpressionContext}.
   */
  public static ExpressionContext getExpression(String expression) {
    if (expression.equals("*")) {
      // For 'SELECT *' and 'SELECT COUNT(*)'
      return ExpressionContext.forIdentifier("*");
    } else {
      return getExpression(CalciteSqlParser.compileToExpression(expression));
    }
  }

  /**
   * Converts the given Thrift {@link Expression} into an {@link ExpressionContext}.
   */
  public static ExpressionContext getExpression(Expression thriftExpression) {
    switch (thriftExpression.getType()) {
      case LITERAL:
        return ExpressionContext.forLiteralContext(thriftExpression.getLiteral());
      case IDENTIFIER:
        return ExpressionContext.forIdentifier(thriftExpression.getIdentifier().getName());
      case FUNCTION:
        return ExpressionContext.forFunction(getFunction(thriftExpression.getFunctionCall()));
      default:
        throw new IllegalStateException();
    }
  }

  /**
   * Converts the given Thrift {@link Function} into a {@link FunctionContext}.
   */
  public static FunctionContext getFunction(Function thriftFunction) {
    String functionName = thriftFunction.getOperator();
    FunctionContext.Type functionType =
        AggregationFunctionType.isAggregationFunction(functionName) ? FunctionContext.Type.AGGREGATION
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
   * Converts the given Thrift {@link Expression} into a {@link FilterContext}.
   * <p>NOTE: Currently the query engine only accepts string literals as the right-hand side of the predicate, so we
   *          always convert the right-hand side expressions into strings. We also update boolean predicates that are
   *          missing an EQUALS filter operator.
   */
  public static FilterContext getFilter(Expression thriftExpression) {
    ExpressionType type = thriftExpression.getType();
    switch (type) {
      case FUNCTION:
        Function thriftFunction = thriftExpression.getFunctionCall();
        return getFilter(thriftFunction);
      case IDENTIFIER:
        // Convert "WHERE a" to "WHERE a = true"
        return FilterContext.forPredicate(new EqPredicate(getExpression(thriftExpression), "true"));
      case LITERAL:
        return FilterContext.forConstant(new LiteralContext(thriftExpression.getLiteral()).getBooleanValue());
      default:
        throw new IllegalStateException();
    }
  }

  public static FilterContext getFilter(Function thriftFunction) {
    String functionOperator = thriftFunction.getOperator();

    // convert "WHERE startsWith(col, 'str')" to "WHERE startsWith(col, 'str') = true"
    if (!EnumUtils.isValidEnum(FilterKind.class, functionOperator)) {
      return FilterContext.forPredicate(
          new EqPredicate(ExpressionContext.forFunction(getFunction(thriftFunction)), "true"));
    }

    FilterKind filterKind = FilterKind.valueOf(thriftFunction.getOperator().toUpperCase());
    List<Expression> operands = thriftFunction.getOperands();
    int numOperands = operands.size();
    switch (filterKind) {
      case AND: {
        List<FilterContext> children = new ArrayList<>(numOperands);
        for (Expression operand : operands) {
          FilterContext filter = getFilter(operand);
          if (!filter.isConstant()) {
            children.add(filter);
          } else {
            if (filter.isConstantFalse()) {
              return FilterContext.CONSTANT_FALSE;
            }
          }
        }
        int numChildren = children.size();
        if (numChildren == 0) {
          return FilterContext.CONSTANT_TRUE;
        } else if (numChildren == 1) {
          return children.get(0);
        } else {
          return FilterContext.forAnd(children);
        }
      }
      case OR: {
        List<FilterContext> children = new ArrayList<>(numOperands);
        for (Expression operand : operands) {
          FilterContext filter = getFilter(operand);
          if (!filter.isConstant()) {
            children.add(filter);
          } else {
            if (filter.isConstantTrue()) {
              return FilterContext.CONSTANT_TRUE;
            }
          }
        }
        int numChildren = children.size();
        if (numChildren == 0) {
          return FilterContext.CONSTANT_FALSE;
        } else if (numChildren == 1) {
          return children.get(0);
        } else {
          return FilterContext.forOr(children);
        }
      }
      case NOT: {
        assert numOperands == 1;
        FilterContext filter = getFilter(operands.get(0));
        if (!filter.isConstant()) {
          return FilterContext.forNot(filter);
        } else {
          return filter.isConstantTrue() ? FilterContext.CONSTANT_FALSE : FilterContext.CONSTANT_TRUE;
        }
      }
      case EQUALS:
        return FilterContext.forPredicate(
            new EqPredicate(getExpression(operands.get(0)), getStringValue(operands.get(1))));
      case NOT_EQUALS:
        return FilterContext.forPredicate(
            new NotEqPredicate(getExpression(operands.get(0)), getStringValue(operands.get(1))));
      case IN: {
        List<String> values = new ArrayList<>(numOperands - 1);
        for (int i = 1; i < numOperands; i++) {
          values.add(getStringValue(operands.get(i)));
        }
        return FilterContext.forPredicate(new InPredicate(getExpression(operands.get(0)), values));
      }
      case NOT_IN: {
        List<String> values = new ArrayList<>(numOperands - 1);
        for (int i = 1; i < numOperands; i++) {
          values.add(getStringValue(operands.get(i)));
        }
        return FilterContext.forPredicate(new NotInPredicate(getExpression(operands.get(0)), values));
      }
      case GREATER_THAN:
        return FilterContext.forPredicate(
            new RangePredicate(getExpression(operands.get(0)), false, getStringValue(operands.get(1)), false,
                RangePredicate.UNBOUNDED));
      case GREATER_THAN_OR_EQUAL:
        return FilterContext.forPredicate(
            new RangePredicate(getExpression(operands.get(0)), true, getStringValue(operands.get(1)), false,
                RangePredicate.UNBOUNDED));
      case LESS_THAN:
        return FilterContext.forPredicate(
            new RangePredicate(getExpression(operands.get(0)), false, RangePredicate.UNBOUNDED, false,
                getStringValue(operands.get(1))));
      case LESS_THAN_OR_EQUAL:
        return FilterContext.forPredicate(
            new RangePredicate(getExpression(operands.get(0)), false, RangePredicate.UNBOUNDED, true,
                getStringValue(operands.get(1))));
      case BETWEEN:
        return FilterContext.forPredicate(
            new RangePredicate(getExpression(operands.get(0)), true, getStringValue(operands.get(1)), true,
                getStringValue(operands.get(2))));
      case RANGE:
        return FilterContext.forPredicate(
            new RangePredicate(getExpression(operands.get(0)), getStringValue(operands.get(1))));
      case REGEXP_LIKE:
        return FilterContext.forPredicate(
            new RegexpLikePredicate(getExpression(operands.get(0)), getStringValue(operands.get(1))));
      case LIKE:
        return FilterContext.forPredicate(new RegexpLikePredicate(getExpression(operands.get(0)),
            RegexpPatternConverterUtils.likeToRegexpLike(getStringValue(operands.get(1)))));
      case TEXT_CONTAINS:
        return FilterContext.forPredicate(
            new TextContainsPredicate(getExpression(operands.get(0)), getStringValue(operands.get(1))));
      case TEXT_MATCH:
        return FilterContext.forPredicate(
            new TextMatchPredicate(getExpression(operands.get(0)), getStringValue(operands.get(1))));
      case JSON_MATCH:
        return FilterContext.forPredicate(
            new JsonMatchPredicate(getExpression(operands.get(0)), getStringValue(operands.get(1))));
      case VECTOR_SIMILARITY:
        ExpressionContext lhs = getExpression(operands.get(0));
        float[] vectorValue = getVectorValue(operands.get(1));
        int topK = VectorSimilarityPredicate.DEFAULT_TOP_K;
        if (operands.size() == 3) {
          topK = (int) operands.get(2).getLiteral().getLongValue();
        }
        return FilterContext.forPredicate(new VectorSimilarityPredicate(lhs, vectorValue, topK));
      case IS_NULL:
        return FilterContext.forPredicate(new IsNullPredicate(getExpression(operands.get(0))));
      case IS_NOT_NULL:
        return FilterContext.forPredicate(new IsNotNullPredicate(getExpression(operands.get(0))));
      default:
        throw new IllegalStateException();
    }
  }

  private static String getStringValue(Expression thriftExpression) {
    if (thriftExpression.getType() != ExpressionType.LITERAL) {
      throw new BadQueryRequestException(
          "Pinot does not support column or function on the right-hand side of the predicate");
    }
    if (thriftExpression.getLiteral().getSetField() == Literal._Fields.NULL_VALUE) {
      return "null";
    }
    return thriftExpression.getLiteral().getFieldValue().toString();
  }

  /**
   * Converts the given filter {@link ExpressionContext} into a {@link FilterContext}.
   * <p>NOTE: Currently the query engine only accepts string literals as the right-hand side of the predicate, so we
   *          always convert the right-hand side expressions into strings. We also update boolean predicates that are
   *          missing an EQUALS filter operator.
   */
  public static FilterContext getFilter(ExpressionContext filterExpression) {
    ExpressionContext.Type type = filterExpression.getType();
    switch (type) {
      case FUNCTION:
        FunctionContext filterFunction = filterExpression.getFunction();
        return getFilter(filterFunction);
      case IDENTIFIER:
        return FilterContext.forPredicate(
            new EqPredicate(filterExpression, getStringValue(RequestUtils.getLiteralExpression(true))));
      case LITERAL:
        return FilterContext.forConstant(filterExpression.getLiteral().getBooleanValue());
      default:
        throw new IllegalStateException();
    }
  }

  public static FilterContext getFilter(FunctionContext filterFunction) {
    String functionOperator = filterFunction.getFunctionName().toUpperCase();

    // convert "WHERE startsWith(col, 'str')" to "WHERE startsWith(col, 'str') = true"
    if (!EnumUtils.isValidEnum(FilterKind.class, functionOperator)) {
      return FilterContext.forPredicate(new EqPredicate(ExpressionContext.forFunction(filterFunction), "true"));
    }

    FilterKind filterKind = FilterKind.valueOf(filterFunction.getFunctionName().toUpperCase());
    List<ExpressionContext> operands = filterFunction.getArguments();
    int numOperands = operands.size();
    switch (filterKind) {
      case AND: {
        List<FilterContext> children = new ArrayList<>(numOperands);
        for (ExpressionContext operand : operands) {
          FilterContext filter = getFilter(operand);
          if (!filter.isConstant()) {
            children.add(filter);
          } else {
            if (filter.isConstantFalse()) {
              return FilterContext.CONSTANT_FALSE;
            }
          }
        }
        int numChildren = children.size();
        if (numChildren == 0) {
          return FilterContext.CONSTANT_TRUE;
        } else if (numChildren == 1) {
          return children.get(0);
        } else {
          return FilterContext.forAnd(children);
        }
      }
      case OR: {
        List<FilterContext> children = new ArrayList<>(numOperands);
        for (ExpressionContext operand : operands) {
          FilterContext filter = getFilter(operand);
          if (!filter.isConstant()) {
            children.add(filter);
          } else {
            if (filter.isConstantTrue()) {
              return FilterContext.CONSTANT_TRUE;
            }
          }
        }
        int numChildren = children.size();
        if (numChildren == 0) {
          return FilterContext.CONSTANT_FALSE;
        } else if (numChildren == 1) {
          return children.get(0);
        } else {
          return FilterContext.forOr(children);
        }
      }
      case NOT: {
        assert numOperands == 1;
        FilterContext filter = getFilter(operands.get(0));
        if (!filter.isConstant()) {
          return FilterContext.forNot(filter);
        } else {
          return filter.isConstantTrue() ? FilterContext.CONSTANT_FALSE : FilterContext.CONSTANT_TRUE;
        }
      }
      case EQUALS:
        return FilterContext.forPredicate(new EqPredicate(operands.get(0), getStringValue(operands.get(1))));
      case NOT_EQUALS:
        return FilterContext.forPredicate(new NotEqPredicate(operands.get(0), getStringValue(operands.get(1))));
      case IN: {
        List<String> values = new ArrayList<>(numOperands - 1);
        for (int i = 1; i < numOperands; i++) {
          values.add(getStringValue(operands.get(i)));
        }
        return FilterContext.forPredicate(new InPredicate(operands.get(0), values));
      }
      case NOT_IN: {
        List<String> values = new ArrayList<>(numOperands - 1);
        for (int i = 1; i < numOperands; i++) {
          values.add(getStringValue(operands.get(i)));
        }
        return FilterContext.forPredicate(new NotInPredicate(operands.get(0), values));
      }
      case GREATER_THAN:
        return FilterContext.forPredicate(
            new RangePredicate(operands.get(0), false, getStringValue(operands.get(1)), false,
                RangePredicate.UNBOUNDED));
      case GREATER_THAN_OR_EQUAL:
        return FilterContext.forPredicate(
            new RangePredicate(operands.get(0), true, getStringValue(operands.get(1)), false,
                RangePredicate.UNBOUNDED));
      case LESS_THAN:
        return FilterContext.forPredicate(new RangePredicate(operands.get(0), false, RangePredicate.UNBOUNDED, false,
            getStringValue(operands.get(1))));
      case LESS_THAN_OR_EQUAL:
        return FilterContext.forPredicate(new RangePredicate(operands.get(0), false, RangePredicate.UNBOUNDED, true,
            getStringValue(operands.get(1))));
      case BETWEEN:
        return FilterContext.forPredicate(
            new RangePredicate(operands.get(0), true, getStringValue(operands.get(1)), true,
                getStringValue(operands.get(2))));
      case RANGE:
        return FilterContext.forPredicate(new RangePredicate(operands.get(0), getStringValue(operands.get(1))));
      case REGEXP_LIKE:
        return FilterContext.forPredicate(new RegexpLikePredicate(operands.get(0), getStringValue(operands.get(1))));
      case LIKE:
        return FilterContext.forPredicate(new RegexpLikePredicate(operands.get(0),
            RegexpPatternConverterUtils.likeToRegexpLike(getStringValue(operands.get(1)))));
      case TEXT_CONTAINS:
        return FilterContext.forPredicate(new TextContainsPredicate(operands.get(0), getStringValue(operands.get(1))));
      case TEXT_MATCH:
        return FilterContext.forPredicate(new TextMatchPredicate(operands.get(0), getStringValue(operands.get(1))));
      case JSON_MATCH:
        return FilterContext.forPredicate(new JsonMatchPredicate(operands.get(0), getStringValue(operands.get(1))));
      case VECTOR_SIMILARITY:
        int topK = VectorSimilarityPredicate.DEFAULT_TOP_K;
        if (operands.size() == 3) {
          topK = (int) operands.get(2).getLiteral().getLongValue();
        }
        return FilterContext.forPredicate(
            new VectorSimilarityPredicate(operands.get(0), getVectorValue(operands.get(1)), topK));
      case IS_NULL:
        return FilterContext.forPredicate(new IsNullPredicate(operands.get(0)));
      case IS_NOT_NULL:
        return FilterContext.forPredicate(new IsNotNullPredicate(operands.get(0)));
      default:
        throw new IllegalStateException();
    }
  }

  // TODO: Pass the literal context into the Predicate so that we can read the value based on the data type. Currently
  //       literal context doesn't support float, and we cannot differentiate explicit string literal and literal
  //       without explicit type, so we always convert the literal into string.
  private static String getStringValue(ExpressionContext expressionContext) {
    if (expressionContext.getType() != ExpressionContext.Type.LITERAL) {
      throw new BadQueryRequestException(
          "Pinot does not support column or function on the right-hand side of the predicate");
    }
    return expressionContext.getLiteral().getStringValue();
  }

  private static float[] getVectorValue(ExpressionContext expressionContext) {
    if (expressionContext.getType() != ExpressionContext.Type.FUNCTION) {
      throw new BadQueryRequestException(
          "Pinot does not support column or function on the right-hand side of the predicate");
    }
    float[] vector = new float[expressionContext.getFunction().getArguments().size()];
    for (int i = 0; i < expressionContext.getFunction().getArguments().size(); i++) {
      vector[i] =
          Float.parseFloat(expressionContext.getFunction().getArguments().get(i).getLiteral().getValue().toString());
    }
    return vector;
  }

  private static float[] getVectorValue(Expression thriftExpression) {
    if (thriftExpression.getType() != ExpressionType.FUNCTION) {
      throw new BadQueryRequestException(
          "Pinot does not support column or function on the right-hand side of the predicate");
    }
    float[] vector = new float[thriftExpression.getFunctionCall().getOperandsSize()];
    for (int i = 0; i < thriftExpression.getFunctionCall().getOperandsSize(); i++) {
      vector[i] = Float.parseFloat(
          Double.toString(thriftExpression.getFunctionCall().getOperands().get(i).getLiteral().getDoubleValue()));
    }
    return vector;
  }
}
