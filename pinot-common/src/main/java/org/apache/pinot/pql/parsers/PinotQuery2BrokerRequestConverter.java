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
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.function.FunctionDefinitionRegistry;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.FilterQuery;
import org.apache.pinot.common.request.FilterQueryMap;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.parsers.CompilerConstants;
import org.apache.pinot.parsers.utils.ParserUtils;
import org.apache.pinot.pql.parsers.pql2.ast.FilterKind;
import org.apache.pinot.pql.parsers.pql2.ast.OrderByAstNode;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class PinotQuery2BrokerRequestConverter {

  public BrokerRequest convert(PinotQuery pinotQuery) {
    BrokerRequest brokerRequest = new BrokerRequest();

    //Query Source
    DataSource dataSource = pinotQuery.getDataSource();
    if (dataSource != null) {
      QuerySource querySource = new QuerySource();
      querySource.setTableName(dataSource.getTableName());
      brokerRequest.setQuerySource(querySource);
    }

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
      selectionSort.setColumn(ParserUtils.standardizeExpression(functionCall.getOperands().get(0), true));
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
        String expressionStr = ParserUtils.standardizeExpression(expression, true);
        groupBy.addToExpressions(expressionStr);
      }
      groupBy.setTopN(pinotQuery.getLimit());
      brokerRequest.setGroupBy(groupBy);
    }
  }

  private void convertSelectList(PinotQuery pinotQuery, BrokerRequest brokerRequest) {
    Selection selection = null;
    List<AggregationInfo> aggregationInfoList = null;
    for (Expression selectExpression : pinotQuery.getSelectList()) {
      ExpressionType type = selectExpression.getType();
      Expression expression;
      if (type == ExpressionType.FUNCTION && selectExpression.getFunctionCall().getOperator()
          .equalsIgnoreCase(SqlKind.AS.toString())) {
        expression = selectExpression.getFunctionCall().getOperands().get(0);
        type = expression.getType();
      } else {
        expression = selectExpression;
      }
      switch (type) {
        case LITERAL:
          if (selection == null) {
            selection = new Selection();
          }
          selection.addToSelectionColumns('\'' + expression.getLiteral().getFieldValue().toString() + '\'');
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
            selection.addToSelectionColumns(ParserUtils.standardizeExpression(expression, false));
          }
          break;
        default:
          throw new IllegalArgumentException("Unrecognized expression type - " + type);
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
        Set<String> expressionSet = new TreeSet<>();

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
    aggregationInfo.setExpressions(args);
    aggregationInfo.setIsInSelectList(true);

    // For backward compatibility (new broker - old server), also set the old way.
    // TODO: remove with a major version change.
    aggregationInfo.putToAggregationParams(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO,
        String.join(CompilerConstants.AGGREGATION_FUNCTION_ARG_SEPARATOR, args));

    return aggregationInfo;
  }

  private String getColumnExpression(Expression functionParam) {
    switch (functionParam.getType()) {
      case LITERAL:
        return functionParam.getLiteral().getFieldValue().toString();
      case IDENTIFIER:
        return functionParam.getIdentifier().getName();
      case FUNCTION:
        return ParserUtils.standardizeExpression(functionParam, false, true);
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
    final ExpressionType filterExpressionType = filterExpression.getType();
    switch (filterExpressionType) {
      case LITERAL:
      case IDENTIFIER:
        break;
      case FUNCTION:
        FilterKind filterKind = ParserUtils.getFilterKind(filterExpression);
        FilterOperator filterOperator = ParserUtils.filterKindToOperator(filterKind);
        filterQuery.setOperator(filterOperator);
        Function functionCall = filterExpression.getFunctionCall();
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
          case TEXT_MATCH:
          case JSON_MATCH:
            //first operand is the always the column
            filterQuery.setColumn(ParserUtils.standardizeExpression(operands.get(0), false));
            filterQuery.setValue(ParserUtils.getFilterValues(filterKind, operands));
            break;
          case IS_NULL:
          case IS_NOT_NULL:
            //first operand is the always the column
            filterQuery.setColumn(ParserUtils.standardizeExpression(operands.get(0), false));
            break;
          default:
            throw new UnsupportedOperationException("Filter UDF not supported");
        }
        break;
      default:
        throw new IllegalArgumentException("Unrecognized filter expression type - " + filterExpressionType);
    }

    filterQuery.setNestedFilterQueryIds(childFilterIds);
    return filterQuery;
  }
}
