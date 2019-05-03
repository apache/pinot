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
import java.util.List;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.FilterQuery;
import org.apache.pinot.common.request.FilterQueryMap;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.common.request.QueryType;
import org.apache.pinot.common.request.Selection;

public class PinotQuery2BrokerRequestConverter {

  public BrokerRequest convert(PinotQuery pinotQuery) {
    BrokerRequest brokerRequest = new BrokerRequest();

    //Query Source
    QuerySource querySource = new QuerySource();
    querySource.setTableName(pinotQuery.getDataSource().getTableName());
    brokerRequest.setQuerySource(querySource);

    handleFilter(pinotQuery, brokerRequest);

    //Handle select list
    handleSelectList(pinotQuery, brokerRequest);

    //Handle group by
    GroupBy groupBy = handleGroupBy(pinotQuery);

    //Query Type
    QueryType queryType = new QueryType();
    if (brokerRequest.getAggregationsInfo() != null
        && brokerRequest.getAggregationsInfo().size() > 0) {
      if (groupBy != null) {
        queryType.setHasGroup_by(true);
      } else {
        queryType.setHasAggregation(true);
      }
    } else {
      queryType.setHasSelection(true);
    }
    brokerRequest.setQueryType(queryType);

    //TODO: these should not be part of the query?
    //brokerRequest.setEnableTrace();
    //brokerRequest.setDebugOptions();
    //brokerRequest.setQueryOptions();
    //brokerRequest.setBucketHashKey();
    //brokerRequest.setDuration();

    return brokerRequest;
  }

  private GroupBy handleGroupBy(PinotQuery pinotQuery) {
    GroupBy groupBy = null;
    List<Expression> groupByList = pinotQuery.getGroupByList();
    if (groupByList != null && groupByList.size() > 0) {

      groupBy = new GroupBy();
      for (Expression expression : groupByList) {
        String expressionStr = standardizeExpression(expression);
        groupBy.addToExpressions(expressionStr);
      }
      groupBy.setTopN(pinotQuery.getLimit());
    }
    return groupBy;
  }

  private void handleSelectList(PinotQuery pinotQuery, BrokerRequest brokerRequest) {
    Selection selection = null;
    List<AggregationInfo> aggregationInfoList = null;
    for (Expression expression : pinotQuery.getSelectList()) {
      ExpressionType type = expression.getType();
      switch (type) {
        case LITERAL:
          if (selection == null) {
            selection = new Selection();
          }
          selection.addToSelectionColumns(expression.getLiteral().getValue());
          break;
        case IDENTIFIER:
          if (selection == null) {
            selection = new Selection();
          }
          selection.addToSelectionColumns(expression.getIdentifier().getName());
          break;
        case FUNCTION:
          AggregationInfo aggInfo = buildAggregationInfo(expression.getFunctionCall());
          if (aggregationInfoList == null) {
            aggregationInfoList = new ArrayList<>();
          }
          aggregationInfoList.add(aggInfo);
          break;
      }
    }

    if (selection != null) {
      selection.setOffset(pinotQuery.getOffset());
      selection.setSize(pinotQuery.getLimit());
      brokerRequest.setSelections(selection);
    }

    if (aggregationInfoList != null && aggregationInfoList.size() > 0) {
      brokerRequest.setAggregationsInfo(aggregationInfoList);
    }
  }

  private void handleFilter(PinotQuery pinotQuery, BrokerRequest brokerRequest) {
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

  private String standardizeExpression(Expression expression) {
    switch (expression.getType()) {
      case LITERAL:
        return expression.getLiteral().getValue();
      case IDENTIFIER:
        return expression.getIdentifier().getName();
      case FUNCTION:
        Function functionCall = expression.getFunctionCall();
        StringBuilder sb = new StringBuilder();
        sb.append(functionCall.getOperator());
        sb.append("(");
        for (Expression operand : functionCall.getOperands()) {
          sb.append(standardizeExpression(operand));
        }
        sb.append(")");
        return sb.toString();
      default:
        throw new UnsupportedOperationException("Unknown Expression type: " + expression.getType());
    }
  }

  private AggregationInfo buildAggregationInfo(Function function) {
    List<Expression> operands = function.getOperands();
    if (operands == null || operands.size() != 1) {
      throw new Pql2CompilationException(
          "Aggregation function" + function.getOperator() + " expects 1 argument. found: "
              + operands);

    }
    String functionName = function.getOperator();
    String columnName;
    if (functionName.equalsIgnoreCase("count")) {
      columnName = "*";
    } else {
      Expression functionParam = operands.get(0);

      switch (functionParam.getType()) {
        case LITERAL:
          columnName = functionParam.getLiteral().getValue();
          break;
        case IDENTIFIER:
          columnName = functionParam.getIdentifier().getName();
          break;
        case FUNCTION:
        default:
          throw new UnsupportedOperationException(
              "Aggregation function does not support functions as arguments");
      }
    }
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(functionName);
    aggregationInfo.putToAggregationParams("column", columnName);
    aggregationInfo.setIsInSelectList(true);
    return aggregationInfo;
  }

  private FilterQuery traverseFilterExpression(Expression filterExpression,
      FilterQueryMap filterSubQueryMap) {
    FilterQuery filterQuery = new FilterQuery();
    int id = filterSubQueryMap.getFilterQueryMapSize();
    filterQuery.setId(id);
    filterSubQueryMap.putToFilterQueryMap(id, filterQuery);
    List<Integer> childFilterIds = new ArrayList<>();
    switch (filterExpression.getType()) {
      case LITERAL:
        break;
      case IDENTIFIER:
        break;
      case FUNCTION:
        Function functionCall = filterExpression.getFunctionCall();
        String operator = functionCall.getOperator();
        FilterOperator filterOperator = FilterOperator.valueOf(operator);
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
          case RANGE:
          case REGEXP_LIKE:
          case NOT_IN:
          case IN:
            //first operand is the always the column
            String column = null;
            //remaining operands are arguments to the function
            List<String> valueList = new ArrayList<>();
            for (int i = 0; i < operands.size(); i++) {
              Expression operand = operands.get(i);
              if (i == 0) {
                column = standardizeExpression(operand);
              } else {
                valueList.add(standardizeExpression(operand));
              }
            }
            filterQuery.setColumn(column);
            filterQuery.setValue(valueList);
            break;
          default:
            throw new UnsupportedOperationException("Filter UDF not supported");
        }
        break;
    }
    if (childFilterIds.size() > 0) {
      filterQuery.setNestedFilterQueryIds(childFilterIds);
    }
    return filterQuery;
  }


}
