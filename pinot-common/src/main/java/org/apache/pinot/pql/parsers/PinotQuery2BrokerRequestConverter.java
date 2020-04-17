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
package org.apache.pinot.pql.parsers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.function.FunctionDefinitionRegistry;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.FilterQuery;
import org.apache.pinot.common.request.FilterQueryMap;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.pql.parsers.pql2.ast.FilterKind;
import org.apache.pinot.pql.parsers.pql2.ast.OrderByAstNode;


public class PinotQuery2BrokerRequestConverter {

  static Map<FilterKind, FilterOperator> filterOperatorMapping;

  public BrokerRequest convert(PinotQuery pinotQuery) {
    BrokerRequest brokerRequest = new BrokerRequest();

    //Query Source
    QuerySource querySource = new QuerySource();
    querySource.setTableName(pinotQuery.getDataSource().getTableName());
    brokerRequest.setQuerySource(querySource);

    convertFilter(pinotQuery, brokerRequest);

    //Handle select list
    convertSelectList(pinotQuery, brokerRequest);

    //Handle order by
    convertOrderBy(pinotQuery, brokerRequest);

    //Handle group by
    convertGroupBy(pinotQuery, brokerRequest);

    //TODO: these should not be part of the query?
    //brokerRequest.setEnableTrace();
    brokerRequest.setDebugOptions(pinotQuery.getDebugOptions());
    brokerRequest.setQueryOptions(pinotQuery.getQueryOptions());
    //brokerRequest.setBucketHashKey();
    //brokerRequest.setDuration();
    brokerRequest.setLimit(pinotQuery.getLimit());
    brokerRequest.setPinotQuery(pinotQuery);

    return brokerRequest;
  }

  private void convertOrderBy(PinotQuery pinotQuery, BrokerRequest brokerRequest) {
    if (pinotQuery.getOrderByList() == null) {
      return;
    }
    List<SelectionSort> sortSequenceList = new ArrayList<>();
    final List<Expression> orderByList = pinotQuery.getOrderByList();
    for (Expression orderByExpr : orderByList) {
      SelectionSort selectionSort = new SelectionSort();
      //order by is always a function (ASC or DESC)
      Function functionCall = orderByExpr.getFunctionCall();
      selectionSort.setIsAsc(functionCall.getOperator().equalsIgnoreCase(OrderByAstNode.ASCENDING_ORDER));
      selectionSort.setColumn(standardizeExpression(functionCall.getOperands().get(0), true));
      sortSequenceList.add(selectionSort);
    }
    if (!sortSequenceList.isEmpty()) {
      if (brokerRequest.getSelections() != null) {
        brokerRequest.getSelections().setSelectionSortSequence(sortSequenceList);
      }
      brokerRequest.setOrderBy(sortSequenceList);
    }
  }

  private void convertGroupBy(PinotQuery pinotQuery, BrokerRequest brokerRequest) {
    List<Expression> groupByList = pinotQuery.getGroupByList();
    if (groupByList != null && groupByList.size() > 0) {
      GroupBy groupBy = new GroupBy();
      for (Expression expression : groupByList) {
        String expressionStr = standardizeExpression(expression, true);
        groupBy.addToExpressions(expressionStr);
      }
      groupBy.setTopN(pinotQuery.getLimit());
      brokerRequest.setGroupBy(groupBy);
    }
  }

  private void convertSelectList(PinotQuery pinotQuery, BrokerRequest brokerRequest) {
    Selection selection = null;
    List<AggregationInfo> aggregationInfoList = null;
    for (Expression expression : pinotQuery.getSelectList()) {
      ExpressionType type = expression.getType();
      if (type == ExpressionType.FUNCTION && expression.getFunctionCall().getOperator()
          .equalsIgnoreCase(SqlKind.AS.toString())) {
        expression = expression.getFunctionCall().getOperands().get(0);
        type = expression.getType();
      }
      switch (type) {
        case LITERAL:
          if (selection == null) {
            selection = new Selection();
          }
          selection.addToSelectionColumns(expression.getLiteral().getStringValue());
          break;
        case IDENTIFIER:
          if (selection == null) {
            selection = new Selection();
          }
          selection.addToSelectionColumns(expression.getIdentifier().getName());
          break;
        case FUNCTION:
          Function functionCall = expression.getFunctionCall();
          String functionName = functionCall.getOperator();
          if (FunctionDefinitionRegistry.isAggFunc(functionName)) {
            AggregationInfo aggInfo = buildAggregationInfo(functionCall);
            if (aggregationInfoList == null) {
              aggregationInfoList = new ArrayList<>();
            }
            aggregationInfoList.add(aggInfo);
          } else {
            if (selection == null) {
              selection = new Selection();
            }
            selection.addToSelectionColumns(standardizeExpression(expression, false));
          }
          break;
      }
    }

    if (aggregationInfoList != null && aggregationInfoList.size() > 0) {
      brokerRequest.setAggregationsInfo(aggregationInfoList);
    } else if (selection != null) {
      if (pinotQuery.isSetOffset()) {
        selection.setOffset(pinotQuery.getOffset());
      }
      if (pinotQuery.isSetLimit()) {
        selection.setSize(pinotQuery.getLimit());
      }
      brokerRequest.setSelections(selection);
    }
  }

  private void convertFilter(PinotQuery pinotQuery, BrokerRequest brokerRequest) {
    Expression filterExpression = pinotQuery.getFilterExpression();

    //Handle filter
    if (filterExpression != null) {
      FilterQuery filterQuery;
      FilterQueryMap filterSubQueryMap = new FilterQueryMap();
      filterQuery = traverseFilterExpression(filterExpression, filterSubQueryMap);
      brokerRequest.setFilterQuery(filterQuery);
      brokerRequest.setFilterSubQueryMap(filterSubQueryMap);
    }
  }

  private String standardizeExpression(Expression expression, boolean treatLiteralAsIdentifier) {
    return standardizeExpression(expression, treatLiteralAsIdentifier, false);
  }

  private String standardizeExpression(Expression expression, boolean treatLiteralAsIdentifier,
      boolean forceSingleQuoteOnNonStringLiteral) {
    switch (expression.getType()) {
      case LITERAL:
        Literal literal = expression.getLiteral();
        // Force single quote on non-string literal inside a function.
        if (forceSingleQuoteOnNonStringLiteral && !literal.isSetStringValue()) {
          return "'" + literal.getFieldValue() + "'";
        }
        if (treatLiteralAsIdentifier || !literal.isSetStringValue()) {
          return literal.getFieldValue().toString();
        } else {
          return "'" + literal.getFieldValue() + "'";
        }
      case IDENTIFIER:
        return expression.getIdentifier().getName();
      case FUNCTION:
        Function functionCall = expression.getFunctionCall();
        return standardizeFunction(functionCall);
      default:
        throw new UnsupportedOperationException("Unknown Expression type: " + expression.getType());
    }
  }

  private String standardizeFunction(Function functionCall) {
    StringBuilder sb = new StringBuilder();
    sb.append(functionCall.getOperator().toLowerCase());
    sb.append("(");
    String delim = "";
    for (Expression operand : functionCall.getOperands()) {
      sb.append(delim);
      sb.append(standardizeExpression(operand, false, true));
      delim = ",";
    }
    sb.append(")");
    return sb.toString();
  }

  private AggregationInfo buildAggregationInfo(Function function) {
    List<Expression> operands = function.getOperands();
    if (operands == null || operands.isEmpty()) {
      throw new Pql2CompilationException("Aggregation function expects non null argument");
    }

    List<String> args = new ArrayList<>(operands.size());
    String functionName = function.getOperator();

    if (functionName.equalsIgnoreCase(AggregationFunctionType.COUNT.getName())) {
      args = Collections.singletonList("*");
    } else {
      // Need to de-dup columns for distinct.
      if (functionName.equalsIgnoreCase(AggregationFunctionType.DISTINCT.getName())) {
        Set<String> expressionSet = new HashSet<>();

        for (Expression operand : operands) {
          String expression = getColumnExpression(operand);
          if (expressionSet.add(expression)) {
            args.add(expression);
          }
        }
      } else {
        for (Expression operand : operands) {
          args.add(getColumnExpression(operand));
        }
      }
    }

    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(functionName);
    aggregationInfo.setAggregationFunctionArgs(args);
    aggregationInfo.setIsInSelectList(true);
    return aggregationInfo;
  }

  private String getColumnExpression(Expression functionParam) {
    switch (functionParam.getType()) {
      case LITERAL:
        return functionParam.getLiteral().getStringValue();
      case IDENTIFIER:
        return functionParam.getIdentifier().getName();
      case FUNCTION:
        return standardizeExpression(functionParam, false, true);
      default:
        throw new UnsupportedOperationException("Unrecognized functionParamType:" + functionParam.getType());
    }
  }

  private FilterQuery traverseFilterExpression(Expression filterExpression, FilterQueryMap filterSubQueryMap) {
    FilterQuery filterQuery = new FilterQuery();
    int id = filterSubQueryMap.getFilterQueryMapSize();
    filterQuery.setId(id);
    filterSubQueryMap.putToFilterQueryMap(id, filterQuery);
    List<Integer> childFilterIds = new ArrayList<>();
    switch (filterExpression.getType()) {
      case LITERAL:
      case IDENTIFIER:
        break;
      case FUNCTION:
        Function functionCall = filterExpression.getFunctionCall();
        String operator = functionCall.getOperator();
        FilterKind filterKind = FilterKind.valueOf(operator);
        FilterOperator filterOperator = filterOperatorMapping.get(filterKind);
        filterQuery.setOperator(filterOperator);
        List<Expression> operands = functionCall.getOperands();
        switch (filterOperator) {
          case AND:
          case OR:
            for (Expression operand : operands) {
              FilterQuery childFilter = traverseFilterExpression(operand, filterSubQueryMap);
              childFilterIds.add(childFilter.getId());
            }
            break;
          case EQUALITY:
          case NOT:
          case REGEXP_LIKE:
          case NOT_IN:
          case IN:
          case TEXT_MATCH:
            //first operand is the always the column
            String column = null;
            //remaining operands are arguments to the function
            List<String> valueList = new ArrayList<>();
            for (int i = 0; i < operands.size(); i++) {
              Expression operand = operands.get(i);
              if (i == 0) {
                column = standardizeExpression(operand, false);
              } else {
                valueList.add(standardizeExpression(operand, true));
              }
            }
            filterQuery.setColumn(column);
            filterQuery.setValue(valueList);
            break;
          case RANGE:
            handleRange(filterQuery, filterKind, operands);
            break;
          default:
            throw new UnsupportedOperationException("Filter UDF not supported");
        }
        break;
    }
    filterQuery.setNestedFilterQueryIds(childFilterIds);
    return filterQuery;
  }

  private void handleRange(FilterQuery filterQuery, FilterKind filterKind, List<Expression> operands) {

    filterQuery.setColumn(standardizeExpression(operands.get(0), false));

    String rangeExpression;
    //PQL does not quote the string literals when we create expression
    boolean treatLiteralAsIdentifier = true;

    if (FilterKind.LESS_THAN == filterKind) {

      String value = standardizeExpression(operands.get(1), treatLiteralAsIdentifier);
      rangeExpression = "(*\t\t" + value + ")";
    } else if (FilterKind.LESS_THAN_OR_EQUAL == filterKind) {

      String value = standardizeExpression(operands.get(1), treatLiteralAsIdentifier);
      rangeExpression = "(*\t\t" + value + "]";
    } else if (FilterKind.GREATER_THAN == filterKind) {

      String value = standardizeExpression(operands.get(1), treatLiteralAsIdentifier);
      rangeExpression = "(" + value + "\t\t*)";
    } else if (FilterKind.GREATER_THAN_OR_EQUAL == filterKind) {

      String value = standardizeExpression(operands.get(1), treatLiteralAsIdentifier);
      rangeExpression = "[" + value + "\t\t*)";
    } else if (FilterKind.BETWEEN == filterKind) {

      String left = standardizeExpression(operands.get(1), treatLiteralAsIdentifier);
      String right = standardizeExpression(operands.get(2), treatLiteralAsIdentifier);
      rangeExpression = "[" + left + "\t\t" + right + "]";
    } else {
      throw new UnsupportedOperationException("Unknown Filter Kind:" + filterKind);
    }
    List<String> valueList = new ArrayList<>();
    valueList.add(rangeExpression);
    filterQuery.setValue(valueList);
  }

  static {
    filterOperatorMapping = new HashMap<>();
    filterOperatorMapping.put(FilterKind.AND, FilterOperator.AND);
    filterOperatorMapping.put(FilterKind.OR, FilterOperator.OR);
    filterOperatorMapping.put(FilterKind.EQUALS, FilterOperator.EQUALITY);
    filterOperatorMapping.put(FilterKind.NOT_EQUALS, FilterOperator.NOT);
    filterOperatorMapping.put(FilterKind.GREATER_THAN, FilterOperator.RANGE);
    filterOperatorMapping.put(FilterKind.LESS_THAN, FilterOperator.RANGE);
    filterOperatorMapping.put(FilterKind.GREATER_THAN_OR_EQUAL, FilterOperator.RANGE);
    filterOperatorMapping.put(FilterKind.LESS_THAN_OR_EQUAL, FilterOperator.RANGE);
    filterOperatorMapping.put(FilterKind.BETWEEN, FilterOperator.RANGE);
    filterOperatorMapping.put(FilterKind.IN, FilterOperator.IN);
    filterOperatorMapping.put(FilterKind.NOT_IN, FilterOperator.NOT_IN);
    filterOperatorMapping.put(FilterKind.REGEXP_LIKE, FilterOperator.REGEXP_LIKE);
    filterOperatorMapping.put(FilterKind.TEXT_MATCH, FilterOperator.TEXT_MATCH);
  }
}
