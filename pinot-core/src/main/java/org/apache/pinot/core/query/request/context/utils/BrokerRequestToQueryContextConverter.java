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
package org.apache.pinot.core.query.request.context.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.function.FunctionDefinitionRegistry;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.query.exception.BadQueryRequestException;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.FilterContext;
import org.apache.pinot.core.query.request.context.FunctionContext;
import org.apache.pinot.core.query.request.context.OrderByExpressionContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.predicate.EqPredicate;
import org.apache.pinot.core.query.request.context.predicate.InPredicate;
import org.apache.pinot.core.query.request.context.predicate.IsNotNullPredicate;
import org.apache.pinot.core.query.request.context.predicate.IsNullPredicate;
import org.apache.pinot.core.query.request.context.predicate.NotEqPredicate;
import org.apache.pinot.core.query.request.context.predicate.NotInPredicate;
import org.apache.pinot.core.query.request.context.predicate.RangePredicate;
import org.apache.pinot.core.query.request.context.predicate.RegexpLikePredicate;
import org.apache.pinot.core.query.request.context.predicate.TextMatchPredicate;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.pql.parsers.pql2.ast.AstNode;
import org.apache.pinot.pql.parsers.pql2.ast.FilterKind;
import org.apache.pinot.pql.parsers.pql2.ast.FunctionCallAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.IdentifierAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.LiteralAstNode;


public class BrokerRequestToQueryContextConverter {
  private BrokerRequestToQueryContextConverter() {
  }

  private static final Pql2Compiler PQL_COMPILER = new Pql2Compiler();

  /**
   * Converts the given BrokerRequest to a QueryContext.
   * <p>Use PinotQuery if available to avoid the unnecessary parsing of the expression.
   * <p>TODO: We cannot use PinotQuery to generate the {@code filter} because {@code BrokerRequestOptimizer} only
   *          optimizes the BrokerRequest but not the PinotQuery.
   */
  public static QueryContext convert(BrokerRequest brokerRequest) {
    PinotQuery pinotQuery = brokerRequest.getPinotQuery();

    List<ExpressionContext> selectExpressions;
    Map<ExpressionContext, String> aliasMap;
    List<ExpressionContext> groupByExpressions = null;
    int limit;
    int offset = 0;
    if (pinotQuery != null) {
      aliasMap = new HashMap<>();
      List<Expression> selectList = pinotQuery.getSelectList();
      int numExpressions = selectList.size();
      List<ExpressionContext> aggregationExpressions = new ArrayList<>(numExpressions);
      List<ExpressionContext> nonAggregationExpressions = new ArrayList<>(numExpressions);
      for (Expression thriftExpression : selectList) {
        ExpressionContext expression;
        if (thriftExpression.getType() == ExpressionType.FUNCTION && thriftExpression.getFunctionCall().getOperator()
            .equalsIgnoreCase("AS")) {
          // Handle alias
          List<Expression> operands = thriftExpression.getFunctionCall().getOperands();
          expression = getExpression(operands.get(0));
          aliasMap.put(expression, operands.get(1).getIdentifier().getName());
        } else {
          expression = getExpression(thriftExpression);
        }
        if (expression.getType() == ExpressionContext.Type.FUNCTION
            && expression.getFunction().getType() == FunctionContext.Type.AGGREGATION) {
          aggregationExpressions.add(expression);
        } else {
          nonAggregationExpressions.add(expression);
        }
      }
      if (aggregationExpressions.isEmpty()) {
        // NOTE: Pinot ignores the GROUP-BY clause when there is no aggregation expressions in the SELECT clause.
        selectExpressions = nonAggregationExpressions;
      } else {
        // NOTE: Pinot ignores the non-aggregation expressions when there are aggregation expressions in the SELECT
        //       clause. E.g. SELECT a, SUM(b) -> SELECT SUM(b).
        selectExpressions = aggregationExpressions;

        List<Expression> groupByList = pinotQuery.getGroupByList();
        if (CollectionUtils.isNotEmpty(groupByList)) {
          groupByExpressions = new ArrayList<>(groupByList.size());
          for (Expression thriftExpression : groupByList) {
            groupByExpressions.add(getExpression(thriftExpression));
          }
        }
      }
      limit = pinotQuery.getLimit();
      offset = pinotQuery.getOffset();
    } else {
      // NOTE: Alias is not supported for PQL queries.
      aliasMap = Collections.emptyMap();
      Selection selections = brokerRequest.getSelections();
      if (selections != null) {
        // Selection query
        List<String> selectionColumns = selections.getSelectionColumns();
        selectExpressions = new ArrayList<>(selectionColumns.size());
        for (String expression : selectionColumns) {
          selectExpressions.add(getExpression(expression));
        }
        // NOTE: Pinot ignores the GROUP-BY clause for selection queries.
        groupByExpressions = null;
        limit = brokerRequest.getLimit();
        offset = selections.getOffset();
      } else {
        // Aggregation query
        List<AggregationInfo> aggregationsInfo = brokerRequest.getAggregationsInfo();
        selectExpressions = new ArrayList<>(aggregationsInfo.size());
        for (AggregationInfo aggregationInfo : aggregationsInfo) {
          String functionName = aggregationInfo.getAggregationType();
          List<String> stringExpressions = aggregationInfo.getExpressions();
          int numArguments = stringExpressions.size();
          List<ExpressionContext> arguments = new ArrayList<>(numArguments);
          if (functionName.equalsIgnoreCase(AggregationFunctionType.DISTINCTCOUNTTHETASKETCH.getName())) {
            // NOTE: For DistinctCountThetaSketch, because of the legacy behavior of PQL compiler treating string
            //       literal as identifier in aggregation, here we treat all expressions except for the first one as
            //       string literal.
            arguments.add(getExpression(stringExpressions.get(0)));
            for (int i = 1; i < numArguments; i++) {
              arguments.add(ExpressionContext.forLiteral(stringExpressions.get(i)));
            }
          } else {
            for (String expression : stringExpressions) {
              arguments.add(getExpression(expression));
            }
          }
          FunctionContext function = new FunctionContext(FunctionContext.Type.AGGREGATION, functionName, arguments);
          selectExpressions.add(ExpressionContext.forFunction(function));
        }

        GroupBy groupBy = brokerRequest.getGroupBy();
        if (groupBy != null) {
          // Aggregation group-by query
          List<String> stringExpressions = groupBy.getExpressions();
          groupByExpressions = new ArrayList<>(stringExpressions.size());
          for (String stringExpression : stringExpressions) {
            groupByExpressions.add(getExpression(stringExpression));
          }

          // NOTE: Use TOP in GROUP-BY clause as LIMIT for backward-compatibility.
          limit = (int) groupBy.getTopN();
        } else {
          limit = brokerRequest.getLimit();
        }
      }
    }

    List<OrderByExpressionContext> orderByExpressions = null;
    if (pinotQuery != null) {
      List<Expression> orderByList = pinotQuery.getOrderByList();
      if (CollectionUtils.isNotEmpty(orderByList)) {
        orderByExpressions = new ArrayList<>(orderByList.size());
        for (Expression orderBy : orderByList) {
          // NOTE: Order-by is always a Function with the ordering of the Expression
          Function thriftFunction = orderBy.getFunctionCall();
          boolean isAsc = thriftFunction.getOperator().equalsIgnoreCase("ASC");
          ExpressionContext expression = getExpression(thriftFunction.getOperands().get(0));
          orderByExpressions.add(new OrderByExpressionContext(expression, isAsc));
        }
      }
    } else {
      List<SelectionSort> orderBy = brokerRequest.getOrderBy();
      if (CollectionUtils.isNotEmpty(orderBy)) {
        orderByExpressions = new ArrayList<>(orderBy.size());
        for (SelectionSort selectionSort : orderBy) {
          orderByExpressions
              .add(new OrderByExpressionContext(getExpression(selectionSort.getColumn()), selectionSort.isIsAsc()));
        }
      }
    }

    // NOTE: Always use BrokerRequest to generate filter because BrokerRequestOptimizer only optimizes the BrokerRequest
    //       but not the PinotQuery.
    FilterContext filter = null;
    FilterQueryTree root = RequestUtils.generateFilterQueryTree(brokerRequest);
    if (root != null) {
      filter = getFilter(root);
    }

    // NOTE: Always use PinotQuery to generate HAVING filter because PQL does not support HAVING clause.
    FilterContext havingFilter = null;
    if (pinotQuery != null) {
      Expression havingExpression = pinotQuery.getHavingExpression();
      if (havingExpression != null) {
        havingFilter = getFilter(havingExpression);
      }
    }

    return new QueryContext.Builder().setSelectExpressions(selectExpressions).setAliasMap(aliasMap).setFilter(filter)
        .setGroupByExpressions(groupByExpressions).setOrderByExpressions(orderByExpressions)
        .setHavingFilter(havingFilter).setLimit(limit).setOffset(offset)
        .setQueryOptions(brokerRequest.getQueryOptions()).setDebugOptions(brokerRequest.getDebugOptions())
        .setBrokerRequest(brokerRequest).build();
  }

  private static ExpressionContext getExpression(Expression thriftExpression) {
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

  private static FunctionContext getFunction(Function thriftFunction) {
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
    List<ExpressionContext> arguments = new ArrayList<>(operands.size());
    for (Expression operand : operands) {
      arguments.add(getExpression(operand));
    }
    return new FunctionContext(functionType, functionName, arguments);
  }

  private static ExpressionContext getExpression(String stringExpression) {
    if (stringExpression.equals("*")) {
      // For 'SELECT *' and 'SELECT COUNT(*)'
      return ExpressionContext.forIdentifier("*");
    } else {
      return getExpression(PQL_COMPILER.parseToAstNode(stringExpression));
    }
  }

  private static ExpressionContext getExpression(AstNode astNode) {
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

  private static FunctionContext getFunction(FunctionCallAstNode astNode) {
    String functionName = astNode.getName();
    FunctionContext.Type functionType =
        FunctionDefinitionRegistry.isAggFunc(functionName) ? FunctionContext.Type.AGGREGATION
            : FunctionContext.Type.TRANSFORM;
    List<? extends AstNode> children = astNode.getChildren();
    List<ExpressionContext> arguments = new ArrayList<>(children.size());
    for (AstNode child : children) {
      arguments.add(getExpression(child));
    }
    return new FunctionContext(functionType, functionName, arguments);
  }

  private static FilterContext getFilter(FilterQueryTree node) {
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
            new EqPredicate(getExpression(node.getColumn()), node.getValue().get(0)));
      case NOT:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new NotEqPredicate(getExpression(node.getColumn()), node.getValue().get(0)));
      case IN:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new InPredicate(getExpression(node.getColumn()), node.getValue()));
      case NOT_IN:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new NotInPredicate(getExpression(node.getColumn()), node.getValue()));
      case RANGE:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new RangePredicate(getExpression(node.getColumn()), node.getValue().get(0)));
      case REGEXP_LIKE:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new RegexpLikePredicate(getExpression(node.getColumn()), node.getValue().get(0)));
      case TEXT_MATCH:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new TextMatchPredicate(getExpression(node.getColumn()), node.getValue().get(0)));
      case IS_NULL:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new IsNullPredicate(getExpression(node.getColumn())));
      case IS_NOT_NULL:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new IsNotNullPredicate(getExpression(node.getColumn())));
      default:
        throw new IllegalStateException();
    }
  }

  /**
   * NOTE: Currently the query engine only accepts string literals as the right-hand side of the predicate, so we always
   *       convert the right-hand side expressions into strings.
   */
  private static FilterContext getFilter(Expression thriftExpression) {
    Function thriftFunction = thriftExpression.getFunctionCall();
    FilterKind filterKind = FilterKind.valueOf(thriftFunction.getOperator());
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
      case REGEXP_LIKE:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new RegexpLikePredicate(getExpression(operands.get(0)), getStringValue(operands.get(1))));
      case TEXT_MATCH:
        return new FilterContext(FilterContext.Type.PREDICATE, null,
            new TextMatchPredicate(getExpression(operands.get(0)), getStringValue(operands.get(1))));
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
          "Pinot does not support column or expression on the right-hand side of the predicate");
    }
    return thriftExpression.getLiteral().getFieldValue().toString();
  }
}
