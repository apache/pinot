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
package org.apache.pinot.core.query.request.context;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory;


/**
 * The {@code QueryContext} class encapsulates all the query related information extracted from the wiring Object.
 * <p>The query engine should work on the QueryContext instead of the wiring Object for the following benefits:
 * <ul>
 *   <li>
 *     Execution layer can be decoupled from the wiring layer, so that changes for one layer won't affect the other
 *     layer.
 *   </li>
 *   <li>
 *     It is very hard to change wiring Object because it involves protocol change, so we should make it as generic as
 *     possible to support future features. Instead, QueryContext is extracted from the wiring Object within each
 *     Broker/Server, and changing it won't cause any protocol change, so we can upgrade it along with the new feature
 *     support in query engine as needed. Also, because of this, we don't have to make QueryContext very generic, which
 *     can help save the overhead of handling generic Objects (e.g. we can pre-compute the Predicates instead of the
 *     using the generic Expressions).
 *   </li>
 *   <li>
 *     In case we need to change the wiring Object (e.g. switch from Thrift to Protobuf), we don't need to change the
 *     whole query engine.
 *   </li>
 *   <li>
 *     We can also add some helper variables or methods in the context classes which can be shared for all segments to
 *     reduce the repetitive work for each segment.
 *   </li>
 * </ul>
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class QueryContext {
  private final String _tableName;
  private final List<ExpressionContext> _selectExpressions;
  private final Map<ExpressionContext, String> _aliasMap;
  private final FilterContext _filter;
  private final List<ExpressionContext> _groupByExpressions;
  private final FilterContext _havingFilter;
  private final List<OrderByExpressionContext> _orderByExpressions;
  private final int _limit;
  private final int _offset;
  private final Map<String, String> _queryOptions;
  private final Map<String, String> _debugOptions;

  // Keep the BrokerRequest to make incremental changes
  // TODO: Remove it once the whole query engine is using the QueryContext
  private final BrokerRequest _brokerRequest;

  // Pre-calculate the aggregation functions and columns for the query so that it can be shared among all the segments
  private AggregationFunction[] _aggregationFunctions;
  private Map<FunctionContext, Integer> _aggregationFunctionIndexMap;
  private Set<String> _columns;

  private QueryContext(String tableName, List<ExpressionContext> selectExpressions,
      Map<ExpressionContext, String> aliasMap, @Nullable FilterContext filter,
      @Nullable List<ExpressionContext> groupByExpressions, @Nullable FilterContext havingFilter,
      @Nullable List<OrderByExpressionContext> orderByExpressions, int limit, int offset,
      @Nullable Map<String, String> queryOptions, @Nullable Map<String, String> debugOptions,
      BrokerRequest brokerRequest) {
    _tableName = tableName;
    _selectExpressions = selectExpressions;
    _aliasMap = Collections.unmodifiableMap(aliasMap);
    _filter = filter;
    _groupByExpressions = groupByExpressions;
    _havingFilter = havingFilter;
    _orderByExpressions = orderByExpressions;
    _limit = limit;
    _offset = offset;
    _queryOptions = queryOptions;
    _debugOptions = debugOptions;
    _brokerRequest = brokerRequest;
  }

  /**
   * Returns the table name.
   */
  public String getTableName() {
    return _tableName;
  }

  /**
   * Returns a list of expressions in the SELECT clause.
   */
  public List<ExpressionContext> getSelectExpressions() {
    return _selectExpressions;
  }

  /**
   * Returns an unmodifiable map from the expression to its alias.
   */
  public Map<ExpressionContext, String> getAliasMap() {
    return _aliasMap;
  }

  /**
   * Returns the filter in the WHERE clause, or {@code null} if there is no WHERE clause.
   */
  @Nullable
  public FilterContext getFilter() {
    return _filter;
  }

  /**
   * Returns a list of expressions in the GROUP-BY clause, or {@code null} if there is no GROUP-BY clause.
   */
  @Nullable
  public List<ExpressionContext> getGroupByExpressions() {
    return _groupByExpressions;
  }

  /**
   * Returns the filter in the HAVING clause, or {@code null} if there is no HAVING clause.
   */
  @Nullable
  public FilterContext getHavingFilter() {
    return _havingFilter;
  }

  /**
   * Returns a list of order-by expressions in the ORDER-BY clause, or {@code null} if there is no ORDER-BY clause.
   */
  @Nullable
  public List<OrderByExpressionContext> getOrderByExpressions() {
    return _orderByExpressions;
  }

  /**
   * Returns the limit of the query.
   */
  public int getLimit() {
    return _limit;
  }

  /**
   * Returns the offset of the query.
   */
  public int getOffset() {
    return _offset;
  }

  /**
   * Returns the query options of the query, or {@code null} if not exist.
   */
  @Nullable
  public Map<String, String> getQueryOptions() {
    return _queryOptions;
  }

  /**
   * Returns the debug options of the query, or {@code null} if not exist.
   */
  @Nullable
  public Map<String, String> getDebugOptions() {
    return _debugOptions;
  }

  /**
   * Returns the BrokerRequest where the QueryContext is extracted from.
   */
  public BrokerRequest getBrokerRequest() {
    return _brokerRequest;
  }

  /**
   * Returns the aggregation functions for the query, or {@code null} if the query does not have any aggregation.
   */
  @Nullable
  public AggregationFunction[] getAggregationFunctions() {
    return _aggregationFunctions;
  }

  /**
   * Returns a map from the AGGREGATION FunctionContext to the index of the corresponding AggregationFunction in the
   * aggregation functions array.
   */
  @Nullable
  public Map<FunctionContext, Integer> getAggregationFunctionIndexMap() {
    return _aggregationFunctionIndexMap;
  }

  /**
   * Returns the columns (IDENTIFIER expressions) in the query.
   */
  public Set<String> getColumns() {
    return _columns;
  }

  /**
   * NOTE: For debugging only.
   */
  @Override
  public String toString() {
    return "QueryContext{" + "_tableName='" + _tableName + '\'' + ", _selectExpressions=" + _selectExpressions
        + ", _aliasMap=" + _aliasMap + ", _filter=" + _filter + ", _groupByExpressions=" + _groupByExpressions
        + ", _havingFilter=" + _havingFilter + ", _orderByExpressions=" + _orderByExpressions + ", _limit=" + _limit
        + ", _offset=" + _offset + ", _queryOptions=" + _queryOptions + ", _debugOptions=" + _debugOptions
        + ", _brokerRequest=" + _brokerRequest + '}';
  }

  public static class Builder {
    private String _tableName;
    private List<ExpressionContext> _selectExpressions;
    private Map<ExpressionContext, String> _aliasMap;
    private FilterContext _filter;
    private List<ExpressionContext> _groupByExpressions;
    private FilterContext _havingFilter;
    private List<OrderByExpressionContext> _orderByExpressions;
    private int _limit;
    private int _offset;
    private Map<String, String> _queryOptions;
    private Map<String, String> _debugOptions;
    private BrokerRequest _brokerRequest;

    public Builder setTableName(String tableName) {
      _tableName = tableName;
      return this;
    }

    public Builder setSelectExpressions(List<ExpressionContext> selectExpressions) {
      _selectExpressions = selectExpressions;
      return this;
    }

    public Builder setAliasMap(Map<ExpressionContext, String> aliasMap) {
      _aliasMap = aliasMap;
      return this;
    }

    public Builder setFilter(@Nullable FilterContext filter) {
      _filter = filter;
      return this;
    }

    public Builder setGroupByExpressions(@Nullable List<ExpressionContext> groupByExpressions) {
      _groupByExpressions = groupByExpressions;
      return this;
    }

    public Builder setHavingFilter(@Nullable FilterContext havingFilter) {
      _havingFilter = havingFilter;
      return this;
    }

    public Builder setOrderByExpressions(@Nullable List<OrderByExpressionContext> orderByExpressions) {
      _orderByExpressions = orderByExpressions;
      return this;
    }

    public Builder setLimit(int limit) {
      _limit = limit;
      return this;
    }

    public Builder setOffset(int offset) {
      _offset = offset;
      return this;
    }

    public Builder setQueryOptions(@Nullable Map<String, String> queryOptions) {
      _queryOptions = queryOptions;
      return this;
    }

    public Builder setDebugOptions(@Nullable Map<String, String> debugOptions) {
      _debugOptions = debugOptions;
      return this;
    }

    public Builder setBrokerRequest(BrokerRequest brokerRequest) {
      _brokerRequest = brokerRequest;
      return this;
    }

    public QueryContext build() {
      // TODO: Add validation logic here

      QueryContext queryContext =
          new QueryContext(_tableName, _selectExpressions, _aliasMap, _filter, _groupByExpressions, _havingFilter,
              _orderByExpressions, _limit, _offset, _queryOptions, _debugOptions, _brokerRequest);

      // Pre-calculate the aggregation functions and columns for the query
      generateAggregationFunctions(queryContext);
      extractColumns(queryContext);

      return queryContext;
    }

    /**
     * Helper method to generate the aggregation functions for the query.
     */
    private void generateAggregationFunctions(QueryContext queryContext) {
      List<AggregationFunction> aggregationFunctions = new ArrayList<>();
      Map<FunctionContext, Integer> aggregationFunctionIndexMap = new HashMap<>();

      // Add aggregation functions in the SELECT clause
      // NOTE: DO NOT deduplicate the aggregation functions in the SELECT clause because that involves protocol change.
      List<FunctionContext> aggregationsInSelect = new ArrayList<>();
      for (ExpressionContext selectExpression : queryContext._selectExpressions) {
        getAggregations(selectExpression, aggregationsInSelect);
      }
      for (FunctionContext function : aggregationsInSelect) {
        int functionIndex = aggregationFunctions.size();
        aggregationFunctions.add(AggregationFunctionFactory.getAggregationFunction(function, queryContext));
        aggregationFunctionIndexMap.put(function, functionIndex);
      }

      // Add aggregation functions in the HAVING clause but not in the SELECT clause
      if (queryContext._havingFilter != null) {
        List<FunctionContext> aggregationsInHaving = new ArrayList<>();
        getAggregations(queryContext._havingFilter, aggregationsInHaving);
        for (FunctionContext function : aggregationsInHaving) {
          if (!aggregationFunctionIndexMap.containsKey(function)) {
            int functionIndex = aggregationFunctions.size();
            aggregationFunctions.add(AggregationFunctionFactory.getAggregationFunction(function, queryContext));
            aggregationFunctionIndexMap.put(function, functionIndex);
          }
        }
      }

      // Add aggregation functions in the ORDER-BY clause but not in the SELECT or HAVING clause
      if (queryContext._orderByExpressions != null) {
        List<FunctionContext> aggregationsInOrderBy = new ArrayList<>();
        for (OrderByExpressionContext orderByExpression : queryContext._orderByExpressions) {
          getAggregations(orderByExpression.getExpression(), aggregationsInOrderBy);
        }
        for (FunctionContext function : aggregationsInOrderBy) {
          if (!aggregationFunctionIndexMap.containsKey(function)) {
            int functionIndex = aggregationFunctions.size();
            aggregationFunctions.add(AggregationFunctionFactory.getAggregationFunction(function, queryContext));
            aggregationFunctionIndexMap.put(function, functionIndex);
          }
        }
      }

      if (!aggregationFunctions.isEmpty()) {
        queryContext._aggregationFunctions = aggregationFunctions.toArray(new AggregationFunction[0]);
        queryContext._aggregationFunctionIndexMap = aggregationFunctionIndexMap;
      }
    }

    /**
     * Helper method to extract AGGREGATION FunctionContexts from the given expression.
     */
    private static void getAggregations(ExpressionContext expression, List<FunctionContext> aggregations) {
      FunctionContext function = expression.getFunction();
      if (function == null) {
        return;
      }
      if (function.getType() == FunctionContext.Type.AGGREGATION) {
        // Aggregation
        aggregations.add(function);
      } else {
        // Transform
        for (ExpressionContext argument : function.getArguments()) {
          getAggregations(argument, aggregations);
        }
      }
    }

    /**
     * Helper method to extract AGGREGATION FunctionContexts from the given filter.
     */
    private static void getAggregations(FilterContext filter, List<FunctionContext> aggregations) {
      List<FilterContext> children = filter.getChildren();
      if (children != null) {
        for (FilterContext child : children) {
          getAggregations(child, aggregations);
        }
      } else {
        getAggregations(filter.getPredicate().getLhs(), aggregations);
      }
    }

    /**
     * Helper method to extract the columns (IDENTIFIER expressions) for the query.
     */
    private void extractColumns(QueryContext query) {
      Set<String> columns = new HashSet<>();

      for (ExpressionContext expression : query._selectExpressions) {
        expression.getColumns(columns);
      }
      if (query._filter != null) {
        query._filter.getColumns(columns);
      }
      if (query._groupByExpressions != null) {
        for (ExpressionContext expression : query._groupByExpressions) {
          expression.getColumns(columns);
        }
      }
      if (query._havingFilter != null) {
        query._havingFilter.getColumns(columns);
      }
      if (query._orderByExpressions != null) {
        for (OrderByExpressionContext orderByExpression : query._orderByExpressions) {
          orderByExpression.getColumns(columns);
        }
      }

      // NOTE: Also gather columns from the input expressions of the aggregation functions because for certain types of
      //       aggregation (e.g. DistinctCountThetaSketch), some input expressions are compiled while constructing the
      //       aggregation function.
      if (query._aggregationFunctions != null) {
        for (AggregationFunction aggregationFunction : query._aggregationFunctions) {
          List<ExpressionContext> inputExpressions = aggregationFunction.getInputExpressions();
          for (ExpressionContext expression : inputExpressions) {
            expression.getColumns(columns);
          }
        }
      }

      query._columns = columns;
    }
  }
}
