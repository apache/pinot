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
package org.apache.pinot.core.query.optimizer.filter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.utils.RegexpPatternConverterUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.FilterKind;


/**
 * Merges boolean algebra and regex into a single regex. Specifically:
 * <ul>
 *   <li>regexp_like(col1, "r1") or regexp_like(col1, "r2") is merged into regexp_like(col1, "(?:r1)|(?:r2)")</li>
 *   <li>regexp_like(col1, "r1") and regexp_like(col1, "r2") is merged into regexp_like(col1, "(?=r1)(?=r2)")</li>
 *   <li>not regexp_like(col1, "r1") is merged into regexp_like(col1, "(?!r1)")</li>
 *   <li>col LIKE expr is translated to regexp_like(col1, r1), where r1 is likeToRegexpLike(expr)</li>
 * </ul>
 *
 * This optimization breaks the semantic in some advanced regex. For example, when they use backreferences. Therefore,
 * it can be enabled or disabled by changing the {@link CommonConstants.Query.Request.Optimization#FUSE_REGEX} query
 * option.
 *
 * NOTE: This optimizer follows the {@link FlattenAndOrFilterOptimizer}, so all the AND/OR filters are already
 *       flattened.
 * @see <a href="https://www.regular-expressions.info/refadv.html">https://www.regular-expressions.info/refadv.html</a>
 */
public class MergeRegexFilterOptimizer implements FilterOptimizer {

  private static final Pattern HAS_BACKREFERENCES_PATTERN =
      Pattern.compile(".*(?:(?:\\\\[0-9]+)|(?:\\\\k\\<\\w+\\>)).*");

  @Override
  public Expression optimize(Expression filterExpression, @Nullable Schema schema,
      @Nullable Map<String, String> queryOptions) {
    boolean fuseRegex = queryOptions != null && Boolean.parseBoolean(
        queryOptions.get(CommonConstants.Query.Request.Optimization.FUSE_REGEX));
    if (filterExpression.getType() != ExpressionType.FUNCTION || !fuseRegex) {
      return filterExpression;
    }
    return optimize(filterExpression);
  }

  private Expression optimize(Expression filterExpression) {
    if (filterExpression.getType() != ExpressionType.FUNCTION) {
      return filterExpression;
    }
    Function function = filterExpression.getFunctionCall();
    FilterKind operator;
    try {
      operator = FilterKind.valueOf(function.getOperator());
    } catch (IllegalArgumentException ex) {
      return filterExpression;
    }

    switch (operator) {
      case AND: {
        return optimizeAnd(filterExpression);
      }
      case OR: {
        return optimizeOr(filterExpression);
      }
      case NOT: {
        return optimizeNot(filterExpression);
      }
      case LIKE: {
        return translateLike(filterExpression);
      }
      default: {
        return filterExpression;
      }
    }
  }

  private Expression translateLike(Expression filterExpression) {
    Expression newExpr = RequestUtils.getFunctionExpression(FilterKind.REGEXP_LIKE.name());
    ArrayList<Expression> operands = new ArrayList<>(2);
    operands.add(filterExpression.getFunctionCall().getOperands().get(0));

    Expression predicateExpr = filterExpression.getFunctionCall().getOperands().get(1);

    if (predicateExpr.getType() != ExpressionType.LITERAL) {
      throw new BadQueryRequestException(
          "Pinot does not support column or function on the right-hand side of the predicate");
    }
    String predicateStr = predicateExpr.getLiteral().getFieldValue().toString();
    operands.add(RequestUtils.getLiteralExpression(RegexpPatternConverterUtils.likeToRegexpLike(predicateStr)));

    newExpr.getFunctionCall().setOperands(operands);

    return newExpr;
  }

  private Expression optimizeOr(Expression filterExpression) {
    return optimizeBinary(filterExpression, "(?:", ")", "|");
  }

  private Expression optimizeAnd(Expression filterExpression) {
    return optimizeBinary(filterExpression, "(?=", ")", "");
  }

  private Expression optimizeNot(Expression filterExpression) {
    Function function = filterExpression.getFunctionCall();
    List<Expression> operands = function.getOperands();
    if (operands.size() != 1) {
      return filterExpression;
    }
    Expression operand = optimize(operands.get(0));
    operands.set(0, operand);

    FilterKind childFilterKind = FilterKind.valueOf(operand.getFunctionCall().getOperator());
    if (childFilterKind != FilterKind.REGEXP_LIKE) {
      operands.set(0, optimize(operand));
      return filterExpression;
    }

    List<Expression> matchOperands = operand.getFunctionCall().getOperands();
    Expression lhs = matchOperands.get(0);
    Expression valueExpr = matchOperands.get(1);

    if (!valueExpr.isSetLiteral() || !isOptimizable(valueExpr.getLiteral())) {
      return filterExpression;
    }

    return createTextMatch(lhs, wrap("(?!", valueExpr, ")"));
  }

  private Expression optimizeBinary(Expression filterExpression, String prefix, String suffix, String separator) {

    Function function = filterExpression.getFunctionCall();
    List<Expression> operands = function.getOperands();

    operands.replaceAll(this::optimize);

    HashMap<String, List<Expression>> matchByCol = new HashMap<>();

    for (Expression child : operands) {
      Function functionCall = child.getFunctionCall();
      if (!functionCall.getOperator().equals(FilterKind.REGEXP_LIKE.name())) {
        continue;
      }
      List<Expression> matchChildren = functionCall.getOperands();
      if (matchChildren.size() != 2) {
        continue;
      }
      Expression lhs = matchChildren.get(0);
      if (lhs.getType() != ExpressionType.IDENTIFIER) {
        continue;
      }
      Expression predicate = matchChildren.get(1);
      if (!predicate.isSetLiteral()) {
        continue;
      }
      if (!isOptimizable(predicate.getLiteral())) {
        continue;
      }
      matchByCol.merge(lhs.getIdentifier().getName(), Collections.singletonList(child), (old, current) -> {
        ArrayList<Expression> result = new ArrayList<>(old.size() + current.size());
        result.addAll(old);
        result.addAll(current);
        return result;
      });
    }

    matchByCol.entrySet().removeIf(stringListEntry -> stringListEntry.getValue().size() <= 1);
    operands.removeAll(matchByCol.values().stream().flatMap(Collection::stream).collect(Collectors.toList()));

    for (Map.Entry<String, List<Expression>> entry : matchByCol.entrySet()) {
      String colName = entry.getKey();
      List<Expression> matches = entry.getValue();
      if (matches.size() <= 1) {
        continue;
      }

      String joinedStringPredicate = matches.stream()
          .map(matchExpr -> matchExpr.getFunctionCall().getOperands().get(1).getLiteral())
          .map(matchLiteral -> wrap(prefix, matchLiteral.getStringValue(), suffix))
          .collect(Collectors.joining(separator));

      Expression equivalentPredicate = RequestUtils.getLiteralExpression(joinedStringPredicate);

      Expression equivalent = RequestUtils.getFunctionExpression(FilterKind.REGEXP_LIKE.name());
      equivalent.getFunctionCall().setOperands(new ArrayList<>(2));
      equivalent.getFunctionCall().getOperands().add(RequestUtils.getIdentifierExpression(colName));
      equivalent.getFunctionCall().getOperands().add(equivalentPredicate);

      operands.add(equivalent);
    }
    if (operands.size() == 1) {
      return operands.get(0);
    }

    return filterExpression;
  }

  private static Expression createTextMatch(Expression lhs, Expression value) {
    Expression result = RequestUtils.getFunctionExpression(FilterKind.REGEXP_LIKE.name());
    List<Expression> operands = new ArrayList<>(2);
    operands.add(lhs);
    operands.add(value);
    result.getFunctionCall().setOperands(operands);
    return result;
  }

  private static String wrap(String prefix, String stringValue, String suffix) {
    return prefix + stringValue + suffix;
  }

  private static Expression wrap(String prefix, Expression literalExpression, String suffix) {
    Literal literal = literalExpression.getLiteral();
    String stringValue = literal.getStringValue();

    return RequestUtils.getLiteralExpression(prefix + stringValue + suffix);
  }

  /**
   * This method tries to detect expressions that may not be optimizable, likes the ones that use backreferences.
   *
   * Given that we don't have a proper way to parse the regexp in a way that it is compatible with the regexp engine,
   * this method may have false positives.
   */
  private static boolean isOptimizable(Literal literal) {
    String strValue = literal.getStringValue();
    return !HAS_BACKREFERENCES_PATTERN.matcher(strValue).matches();
  }
}
