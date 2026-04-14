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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import org.apache.commons.lang3.EnumUtils;
import org.apache.pinot.common.function.FunctionRegistry;
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
import org.apache.pinot.common.request.context.predicate.TextMatchPredicate;
import org.apache.pinot.common.request.context.predicate.VectorSimilarityPredicate;
import org.apache.pinot.common.request.context.predicate.VectorSimilarityRadiusPredicate;
import org.apache.pinot.common.utils.RegexpPatternConverterUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.UuidUtils;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


public class RequestContextUtils {
  private static final String UNSUPPORTED_RHS_MESSAGE =
      "Pinot does not support column or function on the right-hand side of the predicate";

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
        return ExpressionContext.forLiteral(thriftExpression.getLiteral());
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
    Function function = thriftExpression.getFunctionCall();
    // Trim off outer IS_TRUE function as it is redundant
    if (function != null && function.getOperator().equals("istrue")) {
      return getFilter(function.getOperands().get(0));
    } else {
      return getFilterInner(thriftExpression);
    }
  }

  private static FilterContext getFilterInner(Expression thriftExpression) {
    ExpressionType type = thriftExpression.getType();
    switch (type) {
      case FUNCTION:
        Function thriftFunction = thriftExpression.getFunctionCall();
        return getFilterInner(thriftFunction);
      case IDENTIFIER:
        // Convert "WHERE a" to "WHERE a = true"
        return FilterContext.forPredicate(new EqPredicate(getExpression(thriftExpression), "true"));
      case LITERAL:
        return FilterContext.forConstant(new LiteralContext(thriftExpression.getLiteral()).getBooleanValue());
      default:
        throw new IllegalStateException();
    }
  }

  /**
   * Returns {@code true} when the expression can be reduced to a literal value without accessing columns.
   */
  public static boolean canEvaluateLiteral(Expression thriftExpression) {
    try {
      evaluateLiteralValue(thriftExpression);
      return true;
    } catch (RuntimeException e) {
      return false;
    }
  }

  private static FilterContext getFilterInner(Function thriftFunction) {
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
          FilterContext filter = getFilterInner(operand);
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
          FilterContext filter = getFilterInner(operand);
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
        FilterContext filter = getFilterInner(operands.get(0));
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
                RangePredicate.UNBOUNDED, getLiteralType(operands.get(1))));
      case GREATER_THAN_OR_EQUAL:
        return FilterContext.forPredicate(
            new RangePredicate(getExpression(operands.get(0)), true, getStringValue(operands.get(1)), false,
                RangePredicate.UNBOUNDED, getLiteralType(operands.get(1))));
      case LESS_THAN:
        return FilterContext.forPredicate(
            new RangePredicate(getExpression(operands.get(0)), false, RangePredicate.UNBOUNDED, false,
                getStringValue(operands.get(1)), getLiteralType(operands.get(1))));
      case LESS_THAN_OR_EQUAL:
        return FilterContext.forPredicate(
            new RangePredicate(getExpression(operands.get(0)), false, RangePredicate.UNBOUNDED, true,
                getStringValue(operands.get(1)), getLiteralType(operands.get(1))));
      case BETWEEN:
        return FilterContext.forPredicate(
            new RangePredicate(getExpression(operands.get(0)), true, getStringValue(operands.get(1)), true,
                getStringValue(operands.get(2)), getLiteralType(operands.get(1))));
      case RANGE:
        return FilterContext.forPredicate(
            new RangePredicate(getExpression(operands.get(0)), getStringValue(operands.get(1))));
      case REGEXP_LIKE:
        if (operands.size() == 2) {
          return FilterContext.forPredicate(
              new RegexpLikePredicate(getExpression(operands.get(0)), getStringValue(operands.get(1))));
        } else if (operands.size() == 3) {
          return FilterContext.forPredicate(
              new RegexpLikePredicate(getExpression(operands.get(0)), getStringValue(operands.get(1)),
                  getStringValue(operands.get(2))));
        } else {
          throw new BadQueryRequestException("REGEXP_LIKE requires 2 or 3 arguments");
        }

      case LIKE:
        return FilterContext.forPredicate(new RegexpLikePredicate(getExpression(operands.get(0)),
            RegexpPatternConverterUtils.likeToRegexpLike(getStringValue(operands.get(1))), "i"));
      case TEXT_MATCH:
        String options = operands.size() > 2 ? getStringValue(operands.get(2)) : null;
        return FilterContext.forPredicate(
            new TextMatchPredicate(getExpression(operands.get(0)), getStringValue(operands.get(1)), options));
      case JSON_MATCH:
        return FilterContext.forPredicate(
            new JsonMatchPredicate(getExpression(operands.get(0)), getStringValue(operands.get(1)),
                    operands.size() == 3 ? getStringValue(operands.get(2)) : null));
      case VECTOR_SIMILARITY:
        ExpressionContext lhs = getExpression(operands.get(0));
        float[] vectorValue = getVectorValue(operands.get(1));
        int topK = VectorSimilarityPredicate.DEFAULT_TOP_K;
        if (operands.size() == 3) {
          topK = operands.get(2).getLiteral().getIntValue();
        }
        return FilterContext.forPredicate(new VectorSimilarityPredicate(lhs, vectorValue, topK));
      case VECTOR_SIMILARITY_RADIUS:
        ExpressionContext radiusLhs = getExpression(operands.get(0));
        float[] radiusVectorValue = getVectorValue(operands.get(1));
        float threshold = VectorSimilarityRadiusPredicate.DEFAULT_THRESHOLD;
        if (operands.size() == 3) {
          threshold = getFloatValue(operands.get(2));
        }
        return FilterContext.forPredicate(
            new VectorSimilarityRadiusPredicate(radiusLhs, radiusVectorValue, threshold));
      case IS_NULL:
        return FilterContext.forPredicate(new IsNullPredicate(getExpression(operands.get(0))));
      case IS_NOT_NULL:
        return FilterContext.forPredicate(new IsNotNullPredicate(getExpression(operands.get(0))));
      default:
        throw new IllegalStateException();
    }
  }

  public static String getStringValue(Expression thriftExpression) {
    return evaluateLiteralValue(thriftExpression).getStringValue();
  }

  /**
   * Converts the given filter {@link ExpressionContext} into a {@link FilterContext}.
   * <p>NOTE: Currently the query engine only accepts string literals as the right-hand side of the predicate, so we
   *          always convert the right-hand side expressions into strings. We also update boolean predicates that are
   *          missing an EQUALS filter operator.
   */
  public static FilterContext getFilter(ExpressionContext filterExpression) {
    FunctionContext function = filterExpression.getFunction();
    // Trim off outer IS_TRUE function as it is redundant
    if (function != null && function.getFunctionName().equals("istrue")) {
      return getFilter(function.getArguments().get(0));
    } else {
      return getFilterInner(filterExpression);
    }
  }

  private static FilterContext getFilterInner(ExpressionContext filterExpression) {
    ExpressionContext.Type type = filterExpression.getType();
    switch (type) {
      case FUNCTION:
        FunctionContext filterFunction = filterExpression.getFunction();
        return getFilterInner(filterFunction);
      case IDENTIFIER:
        return FilterContext.forPredicate(
            new EqPredicate(filterExpression, getStringValue(RequestUtils.getLiteralExpression(true))));
      case LITERAL:
        return FilterContext.forConstant(filterExpression.getLiteral().getBooleanValue());
      default:
        throw new IllegalStateException();
    }
  }

  private static FilterContext getFilterInner(FunctionContext filterFunction) {
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
          FilterContext filter = getFilterInner(operand);
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
          FilterContext filter = getFilterInner(operand);
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
        FilterContext filter = getFilterInner(operands.get(0));
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
            new RangePredicate(operands.get(0), false, getStringValue(operands.get(1)), false, RangePredicate.UNBOUNDED,
                getLiteralType(operands.get(1))));
      case GREATER_THAN_OR_EQUAL:
        return FilterContext.forPredicate(
            new RangePredicate(operands.get(0), true, getStringValue(operands.get(1)), false, RangePredicate.UNBOUNDED,
                getLiteralType(operands.get(1))));
      case LESS_THAN:
        return FilterContext.forPredicate(
            new RangePredicate(operands.get(0), false, RangePredicate.UNBOUNDED, false, getStringValue(operands.get(1)),
                getLiteralType(operands.get(1))));
      case LESS_THAN_OR_EQUAL:
        return FilterContext.forPredicate(
            new RangePredicate(operands.get(0), false, RangePredicate.UNBOUNDED, true, getStringValue(operands.get(1)),
                getLiteralType(operands.get(1))));
      case BETWEEN:
        return FilterContext.forPredicate(
            new RangePredicate(operands.get(0), true, getStringValue(operands.get(1)), true,
                getStringValue(operands.get(2)), getLiteralType(operands.get(1))));
      case RANGE:
        return FilterContext.forPredicate(new RangePredicate(operands.get(0), getStringValue(operands.get(1))));
      case REGEXP_LIKE:
        if (operands.size() == 2) {
          return FilterContext.forPredicate(
              new RegexpLikePredicate(operands.get(0), getStringValue(operands.get(1))));
        } else if (operands.size() == 3) {
          return FilterContext.forPredicate(
              new RegexpLikePredicate(operands.get(0), getStringValue(operands.get(1)),
                  getStringValue(operands.get(2))));
        } else {
          throw new BadQueryRequestException("REGEXP_LIKE requires 2 or 3 arguments");
        }
      case LIKE:
        return FilterContext.forPredicate(new RegexpLikePredicate(operands.get(0),
            RegexpPatternConverterUtils.likeToRegexpLike(getStringValue(operands.get(1))), "i"));
      case TEXT_MATCH:
        String options = operands.size() > 2 ? getStringValue(operands.get(2)) : null;
        return FilterContext.forPredicate(
            new TextMatchPredicate(operands.get(0), getStringValue(operands.get(1)), options));
      case JSON_MATCH:
          return FilterContext.forPredicate(new JsonMatchPredicate(operands.get(0), getStringValue(operands.get(1)),
                  operands.size() == 3?getStringValue(operands.get(2)): null));
      case VECTOR_SIMILARITY:
        int topK = VectorSimilarityPredicate.DEFAULT_TOP_K;
        if (operands.size() == 3) {
          topK = (int) operands.get(2).getLiteral().getLongValue();
        }
        return FilterContext.forPredicate(
            new VectorSimilarityPredicate(operands.get(0), getVectorValue(operands.get(1)), topK));
      case VECTOR_SIMILARITY_RADIUS:
        float radiusThreshold = VectorSimilarityRadiusPredicate.DEFAULT_THRESHOLD;
        if (operands.size() == 3) {
          radiusThreshold = Float.parseFloat(operands.get(2).getLiteral().getValue().toString());
        }
        return FilterContext.forPredicate(
            new VectorSimilarityRadiusPredicate(operands.get(0), getVectorValue(operands.get(1)), radiusThreshold));
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
    return evaluateLiteralValue(expressionContext).getStringValue();
  }

  private static DataType getLiteralType(Expression thriftExpression) {
    return evaluateLiteralValue(thriftExpression).getType();
  }

  private static DataType getLiteralType(ExpressionContext expressionContext) {
    return evaluateLiteralValue(expressionContext).getType();
  }

  private static EvaluatedLiteralValue evaluateLiteralValue(Expression thriftExpression) {
    Literal literal = thriftExpression.getLiteral();
    if (literal != null) {
      return fromLiteralContext(new LiteralContext(literal));
    }
    Function function = thriftExpression.getFunctionCall();
    if (function != null) {
      return evaluateFunctionLiteral(function.getOperator(), function.getOperands(),
          RequestContextUtils::evaluateLiteralValue);
    }
    throw new BadQueryRequestException(UNSUPPORTED_RHS_MESSAGE);
  }

  private static EvaluatedLiteralValue evaluateLiteralValue(ExpressionContext expressionContext) {
    if (expressionContext.getType() == ExpressionContext.Type.LITERAL) {
      return fromLiteralContext(expressionContext.getLiteral());
    }
    if (expressionContext.getType() == ExpressionContext.Type.FUNCTION) {
      FunctionContext function = expressionContext.getFunction();
      return evaluateFunctionLiteral(function.getFunctionName(), function.getArguments(),
          RequestContextUtils::evaluateLiteralValue);
    }
    throw new BadQueryRequestException(UNSUPPORTED_RHS_MESSAGE);
  }

  private static <T> EvaluatedLiteralValue evaluateFunctionLiteral(String functionName, List<T> operands,
      java.util.function.Function<T, EvaluatedLiteralValue> evaluator) {
    String canonicalName = FunctionRegistry.canonicalize(functionName);
    switch (canonicalName) {
      case "cast": {
        validateFunctionArity("CAST", operands, 2);
        EvaluatedLiteralValue source = evaluator.apply(operands.get(0));
        EvaluatedLiteralValue target = evaluator.apply(operands.get(1));
        return evaluateCastLiteral(source, getCastTargetType(target.getStringValue()));
      }
      case "touuid":
        validateFunctionArity("TO_UUID", operands, 1);
        return evaluateToUuidLiteral(evaluator.apply(operands.get(0)));
      case "uuidtobytes":
        validateFunctionArity("UUID_TO_BYTES", operands, 1);
        return evaluateUuidToBytesLiteral(evaluator.apply(operands.get(0)));
      case "bytestouuid":
        validateFunctionArity("BYTES_TO_UUID", operands, 1);
        return evaluateBytesToUuidLiteral(evaluator.apply(operands.get(0)));
      case "uuidtostring":
        validateFunctionArity("UUID_TO_STRING", operands, 1);
        return evaluateUuidToStringLiteral(evaluator.apply(operands.get(0)));
      case "isuuid":
        validateFunctionArity("IS_UUID", operands, 1);
        return evaluateIsUuidLiteral(evaluator.apply(operands.get(0)));
      default:
        throw new BadQueryRequestException(UNSUPPORTED_RHS_MESSAGE);
    }
  }

  private static EvaluatedLiteralValue evaluateCastLiteral(EvaluatedLiteralValue source, DataType targetType) {
    if (source.getType() == DataType.UNKNOWN) {
      return new EvaluatedLiteralValue(source.getStringValue(), targetType);
    }

    if (targetType == DataType.UUID) {
      return new EvaluatedLiteralValue(convertToCanonicalUuidString(source), DataType.UUID);
    }
    if (targetType == DataType.STRING && source.getType() == DataType.UUID) {
      return new EvaluatedLiteralValue(convertToCanonicalUuidString(source), DataType.STRING);
    }
    if (targetType == DataType.BYTES && source.getType() == DataType.UUID) {
      return new EvaluatedLiteralValue(BytesUtils.toHexString(UuidUtils.toBytes(convertToCanonicalUuidString(source))),
          DataType.BYTES);
    }

    Object converted = targetType.convert(source.getStringValue());
    return new EvaluatedLiteralValue(targetType.toString(converted), targetType);
  }

  private static EvaluatedLiteralValue evaluateToUuidLiteral(EvaluatedLiteralValue source) {
    return new EvaluatedLiteralValue(convertToCanonicalUuidString(source), DataType.UUID);
  }

  private static EvaluatedLiteralValue evaluateUuidToBytesLiteral(EvaluatedLiteralValue source) {
    Preconditions.checkArgument(source.getType() == DataType.UUID, "UUID_TO_BYTES requires a UUID operand");
    return new EvaluatedLiteralValue(BytesUtils.toHexString(UuidUtils.toBytes(source.getStringValue())),
        DataType.BYTES);
  }

  private static EvaluatedLiteralValue evaluateBytesToUuidLiteral(EvaluatedLiteralValue source) {
    Preconditions.checkArgument(source.getType() == DataType.BYTES, "BYTES_TO_UUID requires a BYTES operand");
    return new EvaluatedLiteralValue(convertToCanonicalUuidString(source), DataType.UUID);
  }

  private static EvaluatedLiteralValue evaluateUuidToStringLiteral(EvaluatedLiteralValue source) {
    Preconditions.checkArgument(source.getType() == DataType.UUID, "UUID_TO_STRING requires a UUID operand");
    return new EvaluatedLiteralValue(convertToCanonicalUuidString(source), DataType.STRING);
  }

  private static EvaluatedLiteralValue evaluateIsUuidLiteral(EvaluatedLiteralValue source) {
    boolean isUuid;
    switch (source.getType()) {
      case STRING:
        isUuid = UuidUtils.isUuid(source.getStringValue());
        break;
      case BYTES:
        isUuid = UuidUtils.isUuid(BytesUtils.toBytes(source.getStringValue()));
        break;
      case UUID:
        isUuid = true;
        break;
      case UNKNOWN:
        isUuid = false;
        break;
      default:
        throw new IllegalArgumentException("IS_UUID only supports STRING, BYTES, or UUID operands");
    }
    return new EvaluatedLiteralValue(Boolean.toString(isUuid), DataType.BOOLEAN);
  }

  private static String convertToCanonicalUuidString(EvaluatedLiteralValue source) {
    switch (source.getType()) {
      case STRING:
      case UUID:
        return UuidUtils.toString(UuidUtils.toBytes(source.getStringValue()));
      case BYTES:
        return UuidUtils.toString(BytesUtils.toBytes(source.getStringValue()));
      default:
        throw new IllegalArgumentException("Cannot convert " + source.getType() + " literal to UUID");
    }
  }

  private static DataType getCastTargetType(String targetTypeString) {
    switch (targetTypeString.toUpperCase(Locale.ROOT)) {
      case "INT":
      case "INTEGER":
        return DataType.INT;
      case "LONG":
      case "BIGINT":
        return DataType.LONG;
      case "FLOAT":
        return DataType.FLOAT;
      case "DOUBLE":
        return DataType.DOUBLE;
      case "DECIMAL":
      case "BIGDECIMAL":
      case "BIG_DECIMAL":
        return DataType.BIG_DECIMAL;
      case "BOOL":
      case "BOOLEAN":
        return DataType.BOOLEAN;
      case "TIMESTAMP":
        return DataType.TIMESTAMP;
      case "STRING":
      case "VARCHAR":
        return DataType.STRING;
      case "JSON":
        return DataType.JSON;
      case "BYTES":
      case "VARBINARY":
        return DataType.BYTES;
      case "UUID":
        return DataType.UUID;
      default:
        throw new BadQueryRequestException("Unable to cast expression to type - " + targetTypeString);
    }
  }

  private static void validateFunctionArity(String functionName, List<?> operands, int expectedArity) {
    if (operands.size() != expectedArity) {
      throw new BadQueryRequestException(functionName + " function must have exactly " + expectedArity + " operand"
          + (expectedArity == 1 ? "" : "s"));
    }
  }

  private static EvaluatedLiteralValue fromLiteralContext(LiteralContext literalContext) {
    return new EvaluatedLiteralValue(literalContext.getStringValue(), literalContext.getType());
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
    Literal literal = thriftExpression.getLiteral();
    if (literal != null) {
      Literal._Fields type = literal.getSetField();
      switch (type) {
        case INT_ARRAY_VALUE: {
          List<Integer> values = literal.getIntArrayValue();
          int numValues = values.size();
          float[] vector = new float[numValues];
          for (int i = 0; i < numValues; i++) {
            vector[i] = values.get(i);
          }
          return vector;
        }
        case LONG_ARRAY_VALUE: {
          List<Long> values = literal.getLongArrayValue();
          int numValues = values.size();
          float[] vector = new float[numValues];
          for (int i = 0; i < numValues; i++) {
            vector[i] = values.get(i);
          }
          return vector;
        }
        case FLOAT_ARRAY_VALUE: {
          List<Integer> values = literal.getFloatArrayValue();
          int numValues = values.size();
          float[] vector = new float[numValues];
          for (int i = 0; i < numValues; i++) {
            vector[i] = Float.intBitsToFloat(values.get(i));
          }
          return vector;
        }
        case DOUBLE_ARRAY_VALUE: {
          List<Double> values = literal.getDoubleArrayValue();
          int numValues = values.size();
          float[] vector = new float[numValues];
          for (int i = 0; i < numValues; i++) {
            vector[i] = values.get(i).floatValue();
          }
          return vector;
        }
        default:
          throw new IllegalStateException("Unsupported literal type: " + type);
      }
    }
    Function function = thriftExpression.getFunctionCall();
    Preconditions.checkState(function != null,
        "Unsupported right-hand side expression for vector similarity predicate: %s", thriftExpression);
    List<Expression> operands = function.getOperands();
    int numOperands = operands.size();
    float[] vector = new float[numOperands];
    for (int i = 0; i < numOperands; i++) {
      vector[i] = getFloatValue(operands.get(i));
    }
    return vector;
  }

  private static float getFloatValue(Expression thriftExpression) {
    Literal literal = thriftExpression.getLiteral();
    Preconditions.checkState(literal != null, "Expecting literal expression, got: %s", thriftExpression);
    Literal._Fields type = literal.getSetField();
    switch (type) {
      case INT_VALUE:
        return literal.getIntValue();
      case LONG_VALUE:
        return (float) literal.getLongValue();
      case FLOAT_VALUE:
        return Float.intBitsToFloat(literal.getFloatValue());
      case DOUBLE_VALUE:
        return (float) literal.getDoubleValue();
      default:
        throw new IllegalStateException("Unsupported literal type: " + type);
    }
  }

  private static final class EvaluatedLiteralValue {
    private final String _stringValue;
    private final DataType _type;

    private EvaluatedLiteralValue(String stringValue, DataType type) {
      _stringValue = stringValue;
      _type = type;
    }

    private String getStringValue() {
      return _stringValue;
    }

    private DataType getType() {
      return _type;
    }
  }
}
