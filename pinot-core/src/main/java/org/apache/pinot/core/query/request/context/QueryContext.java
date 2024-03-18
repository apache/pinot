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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import org.apache.pinot.core.util.MemoizedClassAssociation;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.config.table.FieldConfig;


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
  private final QueryContext _subquery;
  private final List<ExpressionContext> _selectExpressions;
  private final boolean _distinct;
  private final List<String> _aliasList;
  private final FilterContext _filter;
  private final List<ExpressionContext> _groupByExpressions;
  private final FilterContext _havingFilter;
  private final List<OrderByExpressionContext> _orderByExpressions;
  private final int _limit;
  private final int _offset;
  private final Map<String, String> _queryOptions;
  private final Map<ExpressionContext, ExpressionContext> _expressionOverrideHints;
  private final boolean _explain;

  private final Function<Class<?>, Map<?, ?>> _sharedValues = MemoizedClassAssociation.of(ConcurrentHashMap::new);

  // Pre-calculate the aggregation functions and columns for the query so that it can be shared across all the segments
  private AggregationFunction[] _aggregationFunctions;
  private Map<FunctionContext, Integer> _aggregationFunctionIndexMap;
  private boolean _hasFilteredAggregations;
  private List<Pair<AggregationFunction, FilterContext>> _filteredAggregationFunctions;
  private Map<Pair<FunctionContext, FilterContext>, Integer> _filteredAggregationsIndexMap;
  private Set<String> _columns;

  // Other properties to be shared across all the segments
  // End time in milliseconds for the query
  private long _endTimeMs;
  // Whether to enable prefetch for the query
  private boolean _enablePrefetch;
  // Whether to skip upsert for the query
  private boolean _skipUpsert;
  // Whether to skip star-tree index for the query
  private boolean _skipStarTree;
  // Whether to skip reordering scan filters for the query
  private boolean _skipScanFilterReorder;
  // Maximum number of threads used to execute the query
  private int _maxExecutionThreads = InstancePlanMakerImplV2.DEFAULT_MAX_EXECUTION_THREADS;
  // The following properties apply to group-by queries
  // Maximum initial capacity of the group-by result holder
  private int _maxInitialResultHolderCapacity = InstancePlanMakerImplV2.DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY;
  // Limit of number of groups stored in each segment
  private int _numGroupsLimit = InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT;
  // Minimum number of groups to keep per segment when trimming groups for SQL GROUP BY
  private int _minSegmentGroupTrimSize = InstancePlanMakerImplV2.DEFAULT_MIN_SEGMENT_GROUP_TRIM_SIZE;
  // Minimum number of groups to keep across segments when trimming groups for SQL GROUP BY
  private int _minServerGroupTrimSize = InstancePlanMakerImplV2.DEFAULT_MIN_SERVER_GROUP_TRIM_SIZE;
  // Trim threshold to use for server combine for SQL GROUP BY
  private int _groupTrimThreshold = InstancePlanMakerImplV2.DEFAULT_GROUPBY_TRIM_THRESHOLD;
  // Whether null handling is enabled
  private boolean _nullHandlingEnabled;
  // Whether server returns the final result
  private boolean _serverReturnFinalResult;
  // Collection of index types to skip per column
  private Map<String, Set<FieldConfig.IndexType>> _skipIndexes;

  private QueryContext(@Nullable String tableName, @Nullable QueryContext subquery,
      List<ExpressionContext> selectExpressions, boolean distinct, List<String> aliasList,
      @Nullable FilterContext filter, @Nullable List<ExpressionContext> groupByExpressions,
      @Nullable FilterContext havingFilter, @Nullable List<OrderByExpressionContext> orderByExpressions, int limit,
      int offset, Map<String, String> queryOptions,
      @Nullable Map<ExpressionContext, ExpressionContext> expressionOverrideHints, boolean explain) {
    _tableName = tableName;
    _subquery = subquery;
    _selectExpressions = selectExpressions;
    _distinct = distinct;
    _aliasList = Collections.unmodifiableList(aliasList);
    _filter = filter;
    _groupByExpressions = groupByExpressions;
    _havingFilter = havingFilter;
    _orderByExpressions = orderByExpressions;
    _limit = limit;
    _offset = offset;
    _queryOptions = queryOptions;
    _expressionOverrideHints = expressionOverrideHints;
    _explain = explain;
  }

  /**
   * Returns the table name.
   * NOTE: on the broker side, table name might be {@code null} when subquery is available.
   */
  public String getTableName() {
    return _tableName;
  }

  /**
   * Returns the subquery.
   */
  @Nullable
  public QueryContext getSubquery() {
    return _subquery;
  }

  /**
   * Returns a list of expressions in the SELECT clause.
   */
  public List<ExpressionContext> getSelectExpressions() {
    return _selectExpressions;
  }

  /**
   * Returns whether the query is a DISTINCT query.
   */
  public boolean isDistinct() {
    return _distinct;
  }

  /**
   * Returns an unmodifiable list from the expression to its alias.
   */
  public List<String> getAliasList() {
    return _aliasList;
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
   * Returns the query options of the query.
   */
  public Map<String, String> getQueryOptions() {
    return _queryOptions;
  }

  /**
   * Returns the expression override hints.
   */
  public Map<ExpressionContext, ExpressionContext> getExpressionOverrideHints() {
    return _expressionOverrideHints;
  }

  /**
   * Returns {@code true} if the query is an EXPLAIN query, {@code false} otherwise.
   */
  public boolean isExplain() {
    return _explain;
  }

  /**
   * Returns the aggregation functions for the query, or {@code null} if the query does not have any aggregation.
   */
  @Nullable
  public AggregationFunction[] getAggregationFunctions() {
    return _aggregationFunctions;
  }

  /**
   * Returns the filtered aggregation functions for a query, or {@code null} if the query does not have any aggregation.
   */
  @Nullable
  public List<Pair<AggregationFunction, FilterContext>> getFilteredAggregationFunctions() {
    return _filteredAggregationFunctions;
  }

  /**
   * Returns the filtered aggregation expressions for the query.
   */
  public boolean hasFilteredAggregations() {
    return _hasFilteredAggregations;
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
   * Returns a map from the filtered aggregation (pair of AGGREGATION FunctionContext and FILTER FilterContext) to the
   * index of corresponding AggregationFunction in the aggregation functions array.
   */
  @Nullable
  public Map<Pair<FunctionContext, FilterContext>, Integer> getFilteredAggregationsIndexMap() {
    return _filteredAggregationsIndexMap;
  }

  /**
   * Returns the columns (IDENTIFIER expressions) in the query.
   */
  public Set<String> getColumns() {
    return _columns;
  }

  public long getEndTimeMs() {
    return _endTimeMs;
  }

  public void setEndTimeMs(long endTimeMs) {
    _endTimeMs = endTimeMs;
  }

  public boolean isEnablePrefetch() {
    return _enablePrefetch;
  }

  public void setEnablePrefetch(boolean enablePrefetch) {
    _enablePrefetch = enablePrefetch;
  }

  public boolean isSkipUpsert() {
    return _skipUpsert;
  }

  public void setSkipUpsert(boolean skipUpsert) {
    _skipUpsert = skipUpsert;
  }

  public boolean isSkipStarTree() {
    return _skipStarTree;
  }

  public void setSkipStarTree(boolean skipStarTree) {
    _skipStarTree = skipStarTree;
  }

  public boolean isSkipScanFilterReorder() {
    return _skipScanFilterReorder;
  }

  public void setSkipScanFilterReorder(boolean skipScanFilterReorder) {
    _skipScanFilterReorder = skipScanFilterReorder;
  }

  public int getMaxExecutionThreads() {
    return _maxExecutionThreads;
  }

  public void setMaxExecutionThreads(int maxExecutionThreads) {
    _maxExecutionThreads = maxExecutionThreads;
  }

  public int getMaxInitialResultHolderCapacity() {
    return _maxInitialResultHolderCapacity;
  }

  public void setMaxInitialResultHolderCapacity(int maxInitialResultHolderCapacity) {
    _maxInitialResultHolderCapacity = maxInitialResultHolderCapacity;
  }

  public int getNumGroupsLimit() {
    return _numGroupsLimit;
  }

  public void setNumGroupsLimit(int numGroupsLimit) {
    _numGroupsLimit = numGroupsLimit;
  }

  public int getMinSegmentGroupTrimSize() {
    return _minSegmentGroupTrimSize;
  }

  public void setMinSegmentGroupTrimSize(int minSegmentGroupTrimSize) {
    _minSegmentGroupTrimSize = minSegmentGroupTrimSize;
  }

  public int getMinServerGroupTrimSize() {
    return _minServerGroupTrimSize;
  }

  public void setMinServerGroupTrimSize(int minServerGroupTrimSize) {
    _minServerGroupTrimSize = minServerGroupTrimSize;
  }

  public int getGroupTrimThreshold() {
    return _groupTrimThreshold;
  }

  public void setGroupTrimThreshold(int groupTrimThreshold) {
    _groupTrimThreshold = groupTrimThreshold;
  }

  public boolean isNullHandlingEnabled() {
    return _nullHandlingEnabled;
  }

  public void setNullHandlingEnabled(boolean nullHandlingEnabled) {
    _nullHandlingEnabled = nullHandlingEnabled;
  }

  public boolean isServerReturnFinalResult() {
    return _serverReturnFinalResult;
  }

  public void setServerReturnFinalResult(boolean serverReturnFinalResult) {
    _serverReturnFinalResult = serverReturnFinalResult;
  }

  /**
   * Gets or computes a value of type {@code V} associated with a key of type {@code K} so that it can be shared
   * within the scope of a query.
   * @param type the type of the value produced, guarantees type pollution is impossible.
   * @param key the key used to determine if the value has already been computed.
   * @param mapper A function to apply the first time a key is encountered to construct the value.
   * @param <K> the key type
   * @param <V> the value type
   * @return the shared value
   */
  public <K, V> V getOrComputeSharedValue(Class<V> type, K key, Function<K, V> mapper) {
    return ((ConcurrentHashMap<K, V>) _sharedValues.apply(type)).computeIfAbsent(key, mapper);
  }

  /**
   * NOTE: For debugging only.
   */
  @Override
  public String toString() {
    return "QueryContext{" + "_tableName='" + _tableName + '\'' + ", _subquery=" + _subquery + ", _selectExpressions="
        + _selectExpressions + ", _distinct=" + _distinct + ", _aliasList=" + _aliasList + ", _filter=" + _filter
        + ", _groupByExpressions=" + _groupByExpressions + ", _havingFilter=" + _havingFilter + ", _orderByExpressions="
        + _orderByExpressions + ", _limit=" + _limit + ", _offset=" + _offset + ", _queryOptions=" + _queryOptions
        + ", _expressionOverrideHints=" + _expressionOverrideHints + ", _explain=" + _explain + '}';
  }

  public void setSkipIndexes(Map<String, Set<FieldConfig.IndexType>> skipIndexes) {
    _skipIndexes = skipIndexes;
  }

  public boolean isIndexUseAllowed(String columnName, FieldConfig.IndexType indexType) {
    if (_skipIndexes == null) {
      return true;
    }
    return !_skipIndexes.getOrDefault(columnName, Collections.EMPTY_SET).contains(indexType);
  }

  public boolean isIndexUseAllowed(DataSource dataSource, FieldConfig.IndexType indexType) {
    return isIndexUseAllowed(dataSource.getColumnName(), indexType);
  }

  public static class Builder {
    private String _tableName;
    private QueryContext _subquery;
    private List<ExpressionContext> _selectExpressions;
    private boolean _distinct;
    private List<String> _aliasList;
    private FilterContext _filter;
    private List<ExpressionContext> _groupByExpressions;
    private FilterContext _havingFilter;
    private List<OrderByExpressionContext> _orderByExpressions;
    private int _limit;
    private int _offset;
    private Map<String, String> _queryOptions;
    private Map<String, String> _debugOptions;
    private Map<ExpressionContext, ExpressionContext> _expressionOverrideHints;
    private boolean _explain;

    public Builder setTableName(String tableName) {
      _tableName = tableName;
      return this;
    }

    public Builder setSubquery(QueryContext subquery) {
      _subquery = subquery;
      return this;
    }

    public Builder setSelectExpressions(List<ExpressionContext> selectExpressions) {
      _selectExpressions = selectExpressions;
      return this;
    }

    public Builder setDistinct(boolean distinct) {
      _distinct = distinct;
      return this;
    }

    public Builder setAliasList(List<String> aliasList) {
      _aliasList = aliasList;
      return this;
    }

    public Builder setFilter(FilterContext filter) {
      _filter = filter;
      return this;
    }

    public Builder setGroupByExpressions(List<ExpressionContext> groupByExpressions) {
      _groupByExpressions = groupByExpressions;
      return this;
    }

    public Builder setHavingFilter(FilterContext havingFilter) {
      _havingFilter = havingFilter;
      return this;
    }

    public Builder setOrderByExpressions(List<OrderByExpressionContext> orderByExpressions) {
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

    public Builder setQueryOptions(Map<String, String> queryOptions) {
      _queryOptions = queryOptions;
      return this;
    }

    public Builder setExpressionOverrideHints(Map<ExpressionContext, ExpressionContext> expressionOverrideHints) {
      _expressionOverrideHints = expressionOverrideHints;
      return this;
    }

    public Builder setExplain(boolean explain) {
      _explain = explain;
      return this;
    }

    public QueryContext build() {
      // TODO: Add validation logic here

      if (_queryOptions == null) {
        _queryOptions = Collections.emptyMap();
      }
      QueryContext queryContext =
          new QueryContext(_tableName, _subquery, _selectExpressions, _distinct, _aliasList, _filter,
              _groupByExpressions, _havingFilter, _orderByExpressions, _limit, _offset, _queryOptions,
              _expressionOverrideHints, _explain);
      queryContext.setNullHandlingEnabled(QueryOptionsUtils.isNullHandlingEnabled(_queryOptions));
      queryContext.setServerReturnFinalResult(QueryOptionsUtils.isServerReturnFinalResult(_queryOptions));

      // Pre-calculate the aggregation functions and columns for the query
      generateAggregationFunctions(queryContext);
      extractColumns(queryContext);

      return queryContext;
    }

    /**
     * Helper method to generate the aggregation functions for the query.
     */
    private void generateAggregationFunctions(QueryContext queryContext) {
      List<Pair<AggregationFunction, FilterContext>> filteredAggregationFunctions = new ArrayList<>();
      Map<Pair<FunctionContext, FilterContext>, Integer> filteredAggregationsIndexMap = new HashMap<>();

      // Add aggregation functions in the SELECT clause
      // NOTE: DO NOT deduplicate the aggregation functions in the SELECT clause because that involves protocol change.
      List<Pair<FunctionContext, FilterContext>> filteredAggregations = new ArrayList<>();
      for (ExpressionContext selectExpression : queryContext._selectExpressions) {
        getAggregations(selectExpression, filteredAggregations);
      }
      for (Pair<FunctionContext, FilterContext> pair : filteredAggregations) {
        FunctionContext aggregation = pair.getLeft();
        FilterContext filter = pair.getRight();
        if (filter != null) {
          queryContext._hasFilteredAggregations = true;
        }
        int functionIndex = filteredAggregationFunctions.size();
        AggregationFunction aggregationFunction =
            AggregationFunctionFactory.getAggregationFunction(aggregation, queryContext._nullHandlingEnabled);
        filteredAggregationFunctions.add(Pair.of(aggregationFunction, filter));
        filteredAggregationsIndexMap.put(Pair.of(aggregation, filter), functionIndex);
      }

      // Add aggregation functions in the HAVING and ORDER-BY clause but not in the SELECT clause
      filteredAggregations.clear();
      if (queryContext._havingFilter != null) {
        getAggregations(queryContext._havingFilter, filteredAggregations);
      }
      if (queryContext._orderByExpressions != null) {
        for (OrderByExpressionContext orderByExpression : queryContext._orderByExpressions) {
          getAggregations(orderByExpression.getExpression(), filteredAggregations);
        }
      }
      for (Pair<FunctionContext, FilterContext> pair : filteredAggregations) {
        if (!filteredAggregationsIndexMap.containsKey(pair)) {
          FunctionContext aggregation = pair.getLeft();
          FilterContext filter = pair.getRight();
          int functionIndex = filteredAggregationFunctions.size();
          AggregationFunction aggregationFunction =
              AggregationFunctionFactory.getAggregationFunction(aggregation, queryContext._nullHandlingEnabled);
          filteredAggregationFunctions.add(Pair.of(aggregationFunction, filter));
          filteredAggregationsIndexMap.put(Pair.of(aggregation, filter), functionIndex);
        }
      }

      if (!filteredAggregationFunctions.isEmpty()) {
        int numAggregations = filteredAggregationFunctions.size();
        AggregationFunction[] aggregationFunctions = new AggregationFunction[numAggregations];
        for (int i = 0; i < numAggregations; i++) {
          aggregationFunctions[i] = filteredAggregationFunctions.get(i).getLeft();
        }
        Map<FunctionContext, Integer> aggregationFunctionIndexMap = new HashMap<>();
        for (Map.Entry<Pair<FunctionContext, FilterContext>, Integer> entry : filteredAggregationsIndexMap.entrySet()) {
          aggregationFunctionIndexMap.put(entry.getKey().getLeft(), entry.getValue());
        }
        queryContext._aggregationFunctions = aggregationFunctions;
        queryContext._aggregationFunctionIndexMap = aggregationFunctionIndexMap;
        queryContext._filteredAggregationFunctions = filteredAggregationFunctions;
        queryContext._filteredAggregationsIndexMap = filteredAggregationsIndexMap;
      }
    }

    /**
     * Helper method to extract AGGREGATION FunctionContexts and FILTER FilterContexts from the given expression.
     */
    private static void getAggregations(ExpressionContext expression,
        List<Pair<FunctionContext, FilterContext>> filteredAggregations) {
      FunctionContext function = expression.getFunction();
      if (function == null) {
        return;
      }
      if (function.getType() == FunctionContext.Type.AGGREGATION) {
        // Aggregation
        filteredAggregations.add(Pair.of(function, null));
      } else {
        List<ExpressionContext> arguments = function.getArguments();
        if (function.getFunctionName().equalsIgnoreCase("filter")) {
          // Filtered aggregation
          Preconditions.checkState(arguments.size() == 2, "FILTER must contain 2 arguments");
          FunctionContext aggregation = arguments.get(0).getFunction();
          Preconditions.checkState(aggregation != null && aggregation.getType() == FunctionContext.Type.AGGREGATION,
              "First argument of FILTER must be an aggregation function");
          ExpressionContext filterExpression = arguments.get(1);
          FilterContext filter = RequestContextUtils.getFilter(filterExpression);
          filteredAggregations.add(Pair.of(aggregation, filter));
        } else {
          // Transform
          for (ExpressionContext argument : arguments) {
            getAggregations(argument, filteredAggregations);
          }
        }
      }
    }

    /**
     * Helper method to extract AGGREGATION FunctionContexts and FILTER FilterContexts from the given filter.
     */
    private static void getAggregations(FilterContext filter,
        List<Pair<FunctionContext, FilterContext>> filteredAggregations) {
      List<FilterContext> children = filter.getChildren();
      if (children != null) {
        for (FilterContext child : children) {
          getAggregations(child, filteredAggregations);
        }
      } else {
        getAggregations(filter.getPredicate().getLhs(), filteredAggregations);
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
