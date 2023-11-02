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
package org.apache.pinot.core.operator.query;

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.TableResizer;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.DefaultGroupByExecutor;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.spi.trace.Tracing;


/**
 * The <code>FilteredGroupByOperator</code> class provides the operator for group-by query on a single segment when
 * there are 1 or more filter expressions on aggregations.
 */
@SuppressWarnings("rawtypes")
public class FilteredGroupByOperator extends BaseOperator<GroupByResultsBlock> {
  private static final String EXPLAIN_NAME = "GROUP_BY_FILTERED";

  private final QueryContext _queryContext;
  private final AggregationFunction[] _aggregationFunctions;
  private final ExpressionContext[] _groupByExpressions;
  private final List<Pair<AggregationFunction[], BaseProjectOperator<?>>> _projectOperators;
  private final long _numTotalDocs;
  private final DataSchema _dataSchema;

  private long _numDocsScanned;
  private long _numEntriesScannedInFilter;
  private long _numEntriesScannedPostFilter;

  public FilteredGroupByOperator(QueryContext queryContext,
      List<Pair<AggregationFunction[], BaseProjectOperator<?>>> projectOperators, long numTotalDocs) {
    assert queryContext.getAggregationFunctions() != null && queryContext.getFilteredAggregationFunctions() != null
        && queryContext.getGroupByExpressions() != null;
    _queryContext = queryContext;
    _aggregationFunctions = queryContext.getAggregationFunctions();
    _groupByExpressions = queryContext.getGroupByExpressions().toArray(new ExpressionContext[0]);
    _projectOperators = projectOperators;
    _numTotalDocs = numTotalDocs;

    // NOTE: The indexedTable expects that the data schema will have group by columns before aggregation columns
    int numGroupByExpressions = _groupByExpressions.length;
    int numAggregationFunctions = _aggregationFunctions.length;
    int numColumns = numGroupByExpressions + numAggregationFunctions;
    String[] columnNames = new String[numColumns];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[numColumns];

    // Extract column names and data types for group-by columns
    BaseProjectOperator<?> projectOperator = projectOperators.get(0).getRight();
    for (int i = 0; i < numGroupByExpressions; i++) {
      ExpressionContext groupByExpression = _groupByExpressions[i];
      columnNames[i] = groupByExpression.toString();
      columnDataTypes[i] = DataSchema.ColumnDataType.fromDataTypeSV(
          projectOperator.getResultColumnContext(groupByExpression).getDataType());
    }

    // Extract column names and data types for aggregation functions
    for (int i = 0; i < numAggregationFunctions; i++) {
      int index = numGroupByExpressions + i;
      Pair<AggregationFunction, FilterContext> pair = queryContext.getFilteredAggregationFunctions().get(i);
      AggregationFunction aggregationFunction = pair.getLeft();
      String columnName = AggregationFunctionUtils.getResultColumnName(aggregationFunction, pair.getRight());
      columnNames[index] = columnName;
      columnDataTypes[index] = aggregationFunction.getIntermediateResultColumnType();
    }

    _dataSchema = new DataSchema(columnNames, columnDataTypes);
  }

  @Override
  protected GroupByResultsBlock getNextBlock() {
    // TODO(egalpin): Support Startree query resolution when possible, even with FILTER expressions
    int numAggregations = _aggregationFunctions.length;

    GroupByResultHolder[] groupByResultHolders = new GroupByResultHolder[numAggregations];
    IdentityHashMap<AggregationFunction, Integer> resultHolderIndexMap =
        new IdentityHashMap<>(_aggregationFunctions.length);
    for (int i = 0; i < numAggregations; i++) {
      resultHolderIndexMap.put(_aggregationFunctions[i], i);
    }

    GroupKeyGenerator groupKeyGenerator = null;
    for (Pair<AggregationFunction[], BaseProjectOperator<?>> pair : _projectOperators) {
      AggregationFunction[] aggregationFunctions = pair.getLeft();
      BaseProjectOperator<?> projectOperator = pair.getRight();

      // Perform aggregation group-by on all the blocks
      DefaultGroupByExecutor groupByExecutor;
      if (groupKeyGenerator == null) {
        // The group key generator should be shared across all AggregationFunctions so that agg results can be
        // aligned. Given that filtered aggregations are stored as an iterable of iterables so that all filtered aggs
        // with the same filter can share transform blocks, rather than a singular flat iterable in the case where
        // aggs are all non-filtered, sharing a GroupKeyGenerator across all aggs cannot be accomplished by allowing
        // the GroupByExecutor to have sole ownership of the GroupKeyGenerator. Therefore, we allow constructing a
        // GroupByExecutor with a pre-existing GroupKeyGenerator so that the GroupKeyGenerator can be shared across
        // loop iterations i.e. across all aggs.
        groupByExecutor =
            new DefaultGroupByExecutor(_queryContext, aggregationFunctions, _groupByExpressions, projectOperator);
        groupKeyGenerator = groupByExecutor.getGroupKeyGenerator();
      } else {
        groupByExecutor =
            new DefaultGroupByExecutor(_queryContext, aggregationFunctions, _groupByExpressions, projectOperator,
                groupKeyGenerator);
      }

      int numDocsScanned = 0;
      ValueBlock valueBlock;
      while ((valueBlock = projectOperator.nextBlock()) != null) {
        numDocsScanned += valueBlock.getNumDocs();
        groupByExecutor.process(valueBlock);
      }

      _numDocsScanned += numDocsScanned;
      _numEntriesScannedInFilter += projectOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
      _numEntriesScannedPostFilter += (long) numDocsScanned * projectOperator.getNumColumnsProjected();
      GroupByResultHolder[] filterGroupByResults = groupByExecutor.getGroupByResultHolders();
      for (int i = 0; i < aggregationFunctions.length; i++) {
        groupByResultHolders[resultHolderIndexMap.get(aggregationFunctions[i])] = filterGroupByResults[i];
      }
    }
    assert groupKeyGenerator != null;
    for (GroupByResultHolder groupByResultHolder : groupByResultHolders) {
      groupByResultHolder.ensureCapacity(groupKeyGenerator.getNumKeys());
    }

    // Check if the groups limit is reached
    boolean numGroupsLimitReached = groupKeyGenerator.globalGroupKeyLimitReached();
    Tracing.activeRecording().setNumGroups(_queryContext.getNumGroupsLimit(), groupKeyGenerator.getNumKeys());

    // Trim the groups when iff:
    // - Query has ORDER BY clause
    // - Segment group trim is enabled
    // - There are more groups than the trim size
    // TODO: Currently the groups are not trimmed if there is no ordering specified. Consider ordering on group-by
    //       columns if no ordering is specified.
    int minGroupTrimSize = _queryContext.getMinSegmentGroupTrimSize();
    if (_queryContext.getOrderByExpressions() != null && minGroupTrimSize > 0) {
      int trimSize = GroupByUtils.getTableCapacity(_queryContext.getLimit(), minGroupTrimSize);
      if (groupKeyGenerator.getNumKeys() > trimSize) {
        TableResizer tableResizer = new TableResizer(_dataSchema, _queryContext);
        Collection<IntermediateRecord> intermediateRecords =
            tableResizer.trimInSegmentResults(groupKeyGenerator, groupByResultHolders, trimSize);
        GroupByResultsBlock resultsBlock = new GroupByResultsBlock(_dataSchema, intermediateRecords, _queryContext);
        resultsBlock.setNumGroupsLimitReached(numGroupsLimitReached);
        resultsBlock.setIsAccurateGroupBy(false);
        return resultsBlock;
      }
    }

    AggregationGroupByResult aggGroupByResult =
        new AggregationGroupByResult(groupKeyGenerator, _aggregationFunctions, groupByResultHolders);
    GroupByResultsBlock resultsBlock = new GroupByResultsBlock(_dataSchema, aggGroupByResult, _queryContext);
    resultsBlock.setNumGroupsLimitReached(numGroupsLimitReached);
    resultsBlock.setIsAccurateGroupBy(!numGroupsLimitReached);
    return resultsBlock;
  }

  @Override
  public List<Operator> getChildOperators() {
    return _projectOperators.stream().map(Pair::getRight).collect(Collectors.toList());
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return new ExecutionStatistics(_numDocsScanned, _numEntriesScannedInFilter, _numEntriesScannedPostFilter,
        _numTotalDocs);
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(groupKeys:");
    if (_groupByExpressions.length > 0) {
      stringBuilder.append(_groupByExpressions[0].toString());
      for (int i = 1; i < _groupByExpressions.length; i++) {
        stringBuilder.append(", ").append(_groupByExpressions[i].toString());
      }
    }

    stringBuilder.append(", aggregations:");
    if (_aggregationFunctions.length > 0) {
      stringBuilder.append(_aggregationFunctions[0].toExplainString());
      for (int i = 1; i < _aggregationFunctions.length; i++) {
        stringBuilder.append(", ").append(_aggregationFunctions[i].toExplainString());
      }
    }

    return stringBuilder.append(')').toString();
  }
}
