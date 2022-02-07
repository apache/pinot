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
package org.apache.pinot.core.query.reduce;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.GapfillUtils;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;

/**
 * Helper class to reduce and set Aggregation results into the BrokerResponseNative
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class PreAggregationGapFillDataTableReducer implements DataTableReducer {
  private final QueryContext _queryContext;

  private final int _limitForAggregatedResult;
  private final int _limitForGapfilledResult;

  private final DateTimeGranularitySpec _dateTimeGranularity;
  private final DateTimeFormatSpec _dateTimeFormatter;
  private final long _startMs;
  private final long _endMs;
  private final long _timeBucketSize;

  private final List<Integer> _groupByKeyIndexes;
  private boolean [] _isGroupBySelections;
  private final Set<Key> _groupByKeys;
  private final Map<Key, Object[]> _previousByGroupKey;
  private final Map<String, ExpressionContext> _fillExpressions;
  private final List<ExpressionContext> _timeSeries;

  PreAggregationGapFillDataTableReducer(QueryContext queryContext) {
    _queryContext = queryContext;
    _limitForAggregatedResult = queryContext.getLimit();
    _limitForGapfilledResult = queryContext.getSubQueryContext().getLimit();

    ExpressionContext gapFillSelection =
        GapfillUtils.getPreAggregateGapfillExpressionContext(queryContext.getSubQueryContext());

    Preconditions.checkArgument(
        gapFillSelection != null && gapFillSelection.getFunction() != null, "Gapfill Expression should be function.");
    List<ExpressionContext> args = gapFillSelection.getFunction().getArguments();
    Preconditions.checkArgument(
        args.size() > 5, "PreAggregateGapFill does not have correct number of arguments.");
    Preconditions.checkArgument(
        args.get(1).getLiteral() != null, "The second argument of PostAggregateGapFill should be TimeFormatter.");
    Preconditions.checkArgument(
        args.get(2).getLiteral() != null, "The third argument of PostAggregateGapFill should be start time.");
    Preconditions.checkArgument(
        args.get(3).getLiteral() != null, "The fourth argument of PostAggregateGapFill should be end time.");
    Preconditions.checkArgument(
        args.get(4).getLiteral() != null, "The fifth argument of PostAggregateGapFill should be time bucket size.");

    _dateTimeFormatter = new DateTimeFormatSpec(args.get(1).getLiteral());
    _dateTimeGranularity = new DateTimeGranularitySpec(args.get(4).getLiteral());
    String start = args.get(2).getLiteral();
    _startMs = truncate(_dateTimeFormatter.fromFormatToMillis(start));
    String end = args.get(3).getLiteral();
    _endMs = truncate(_dateTimeFormatter.fromFormatToMillis(end));
    _timeBucketSize = _dateTimeGranularity.granularityToMillis();

    _fillExpressions = GapfillUtils.getFillExpressions(gapFillSelection);

    _previousByGroupKey = new HashMap<>();
    _groupByKeyIndexes = new ArrayList<>();
    _groupByKeys = new HashSet<>();

    ExpressionContext timeseriesOn = GapfillUtils.getTimeSeriesOnExpressionContext(gapFillSelection);
    Preconditions.checkArgument(timeseriesOn != null, "The TimeSeriesOn expressions should be specified.");
    _timeSeries = timeseriesOn.getFunction().getArguments();
  }

  private void replaceColumnNameWithAlias(DataSchema dataSchema) {
    List<String> aliasList = _queryContext.getSubQueryContext().getAliasList();
    Map<String, String> columnNameToAliasMap = new HashMap<>();
    for (int i = 0; i < aliasList.size(); i++) {
      if (aliasList.get(i) != null) {
        ExpressionContext selection = _queryContext.getSubQueryContext().getSelectExpressions().get(i);
        if (GapfillUtils.isGapfill(selection)) {
          selection = selection.getFunction().getArguments().get(0);
        }
        columnNameToAliasMap.put(selection.toString(), aliasList.get(i));
      }
    }
    for (int i = 0; i < dataSchema.getColumnNames().length; i++) {
      if (columnNameToAliasMap.containsKey(dataSchema.getColumnNames()[i])) {
        dataSchema.getColumnNames()[i] = columnNameToAliasMap.get(dataSchema.getColumnNames()[i]);
      }
    }
  }

  /**
   * Here are three things that happen
   * 1. Sort the result sets from all pinot servers based on timestamp
   * 2. Gapfill the data for missing entities per time bucket
   * 3. Aggregate the dataset per time bucket.
   */
  @Override
  public void reduceAndSetResults(String tableName, DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap, BrokerResponseNative brokerResponseNative,
      DataTableReducerContext reducerContext, BrokerMetrics brokerMetrics) {
    DataSchema resultTableSchema = getResultTableDataSchema();
    if (dataTableMap.isEmpty()) {
      brokerResponseNative.setResultTable(new ResultTable(resultTableSchema, Collections.emptyList()));
      return;
    }

    String[] columns = dataSchema.getColumnNames();

    Map<String, Integer> indexes = new HashMap<>();
    for (int i = 0; i < columns.length; i++) {
      indexes.put(columns[i], i);
    }

    _isGroupBySelections = new boolean[dataSchema.getColumnDataTypes().length];

    // The first one argument of timeSeries is time column. The left ones are defining entity.
    for (ExpressionContext entityColum : _timeSeries) {
      int index = indexes.get(entityColum.getIdentifier());
      _isGroupBySelections[index] = true;
      _groupByKeyIndexes.add(index);
    }


    List<Object[]> sortedRawRows = mergeAndSort(dataTableMap.values(), dataSchema);
    List<Object[]> resultRows;
    replaceColumnNameWithAlias(dataSchema);
    if (_queryContext.getAggregationFunctions() != null) {
      validateGroupByForOuterQuery();
    }
    if (_queryContext.getSubQueryContext().getAggregationFunctions() == null) {
      List<Object[]> gapfilledRows = gapFillAndAggregate(sortedRawRows, resultTableSchema, dataSchema);
      if (_queryContext.getAggregationFunctions() == null) {
        List<String> selectionColumns = SelectionOperatorUtils.getSelectionColumns(_queryContext, dataSchema);
        resultRows = new ArrayList<>(gapfilledRows.size());

        Map<String, Integer> columnNameToIndexMap = new HashMap<>(dataSchema.getColumnNames().length);
        String[] columnNames = dataSchema.getColumnNames();
        for (int i = 0; i < columnNames.length; i++) {
          columnNameToIndexMap.put(columnNames[i], i);
        }

        ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
        ColumnDataType[] resultColumnDataTypes = new ColumnDataType[selectionColumns.size()];
        for (int i = 0; i < resultColumnDataTypes.length; i++) {
          String name = selectionColumns.get(i);
          int index = columnNameToIndexMap.get(name);
          resultColumnDataTypes[i] = columnDataTypes[index];
        }

        for (Object[] row : gapfilledRows) {
          Object[] resultRow = new Object[selectionColumns.size()];
          for (int i = 0; i < selectionColumns.size(); i++) {
            int index = columnNameToIndexMap.get(selectionColumns.get(i));
            resultRow[i] = resultColumnDataTypes[i].convertAndFormat(row[index]);
          }
          resultRows.add(resultRow);
        }
      } else {
        resultRows = gapfilledRows;
      }
    } else {
      this.setupColumnTypeForAggregatedColum(dataSchema.getColumnDataTypes());
      ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
      for (Object[] row : sortedRawRows) {
        extractFinalAggregationResults(row);
        for (int i = 0; i < columnDataTypes.length; i++) {
          row[i] = columnDataTypes[i].convert(row[i]);
        }
      }
      resultRows = gapFillAndAggregate(sortedRawRows, resultTableSchema, dataSchema);
    }
    brokerResponseNative.setResultTable(new ResultTable(resultTableSchema, resultRows));
  }

  private void extractFinalAggregationResults(Object[] row) {
    AggregationFunction[] aggregationFunctions = _queryContext.getSubQueryContext().getAggregationFunctions();
    int numAggregationFunctionsForInnerQuery = aggregationFunctions == null ? 0 : aggregationFunctions.length;
    for (int i = 0; i < numAggregationFunctionsForInnerQuery; i++) {
      int valueIndex = _timeSeries.size() + 1 + i;
      row[valueIndex] = aggregationFunctions[i].extractFinalResult(row[valueIndex]);
    }
  }

  private void setupColumnTypeForAggregatedColum(ColumnDataType[] columnDataTypes) {
    AggregationFunction[] aggregationFunctions = _queryContext.getSubQueryContext().getAggregationFunctions();
    int numAggregationFunctionsForInnerQuery = aggregationFunctions == null ? 0 : aggregationFunctions.length;
    for (int i = 0; i < numAggregationFunctionsForInnerQuery; i++) {
      columnDataTypes[_timeSeries.size() + 1 + i] = aggregationFunctions[i].getFinalResultColumnType();
    }
  }

  /**
   * Constructs the DataSchema for the ResultTable.
   */
  private DataSchema getResultTableDataSchema() {
    String [] columnNames = new String[_queryContext.getSelectExpressions().size()];
    ColumnDataType [] columnDataTypes = new ColumnDataType[_queryContext.getSelectExpressions().size()];
    for (int i = 0; i < _queryContext.getSelectExpressions().size(); i++) {
      ExpressionContext expressionContext = _queryContext.getSelectExpressions().get(i);
      if (expressionContext.getType() != ExpressionContext.Type.FUNCTION) {
        columnNames[i] = expressionContext.getIdentifier();
        columnDataTypes[i] = ColumnDataType.STRING;
      } else {
        FunctionContext functionContext = expressionContext.getFunction();
        AggregationFunction aggregationFunction =
            AggregationFunctionFactory.getAggregationFunction(functionContext, _queryContext);
        columnDataTypes[i] = aggregationFunction.getFinalResultColumnType();
        columnNames[i] = functionContext.toString();
      }
    }
    return new DataSchema(columnNames, columnDataTypes);
  }

  private Key constructGroupKeys(Object[] row) {
    Object [] groupKeys = new Object[_groupByKeyIndexes.size()];
    for (int i = 0; i < _groupByKeyIndexes.size(); i++) {
      groupKeys[i] = row[_groupByKeyIndexes.get(i)];
    }
    return new Key(groupKeys);
  }

  private long truncate(long epoch) {
    int sz = _dateTimeGranularity.getSize();
    return epoch / sz * sz;
  }

  private List<Object[]> gapFillAndAggregate(List<Object[]> sortedRows,
      DataSchema dataSchemaForAggregatedResult,
      DataSchema dataSchema) {
    List<Object[]> result = new ArrayList<>();

    PreAggregateGapfillFilterHandler postGapfillFilterHandler = null;
    if (_queryContext.getFilter() != null) {
      postGapfillFilterHandler = new PreAggregateGapfillFilterHandler(_queryContext.getFilter(), dataSchema);
    }
    PreAggregateGapfillFilterHandler postAggregateHavingFilterHandler = null;
    if (_queryContext.getHavingFilter() != null) {
      postAggregateHavingFilterHandler = new PreAggregateGapfillFilterHandler(
          _queryContext.getHavingFilter(), dataSchemaForAggregatedResult);
    }
    Object[] previous = null;
    Iterator<Object[]> sortedIterator = sortedRows.iterator();
    for (long time = _startMs; time < _endMs; time += _timeBucketSize) {
      List<Object[]> bucketedResult = new ArrayList<>();
      previous = gapfill(time, bucketedResult, sortedIterator, previous, dataSchema, postGapfillFilterHandler);
      if (_queryContext.getAggregationFunctions() == null) {
        result.addAll(bucketedResult);
      } else if (bucketedResult.size() > 0) {
        List<Object[]> aggregatedRows = aggregateGapfilledData(bucketedResult, dataSchema);
        for (Object[] aggregatedRow : aggregatedRows) {
          if (postAggregateHavingFilterHandler == null || postAggregateHavingFilterHandler.isMatch(aggregatedRow)) {
            result.add(aggregatedRow);
          }
          if (result.size() >= _limitForAggregatedResult) {
            return result;
          }
        }
      }
    }
    return result;
  }

  private Object[] gapfill(long bucketTime,
      List<Object[]> bucketedResult,
      Iterator<Object[]> sortedIterator,
      Object[] previous,
      DataSchema dataSchema,
      PreAggregateGapfillFilterHandler postGapfillFilterHandler) {
    ColumnDataType[] resultColumnDataTypes = dataSchema.getColumnDataTypes();
    int numResultColumns = resultColumnDataTypes.length;
    Set<Key> keys = new HashSet<>(_groupByKeys);
    if (previous == null && sortedIterator.hasNext()) {
      previous = sortedIterator.next();
    }

    while (previous != null) {
      Object[] resultRow = previous;
      for (int i = 0; i < resultColumnDataTypes.length; i++) {
        resultRow[i] = resultColumnDataTypes[i].format(resultRow[i]);
      }

      long timeCol = _dateTimeFormatter.fromFormatToMillis(String.valueOf(resultRow[0]));
      if (timeCol > bucketTime) {
        break;
      }
      if (timeCol == bucketTime) {
        if (postGapfillFilterHandler == null || postGapfillFilterHandler.isMatch(previous)) {
          bucketedResult.add(resultRow);
          if (bucketedResult.size() >= _limitForGapfilledResult) {
            break;
          }
        }
        Key key = constructGroupKeys(resultRow);
        keys.remove(key);
        _previousByGroupKey.put(key, resultRow);
      }
      if (sortedIterator.hasNext()) {
        previous = sortedIterator.next();
      } else {
        previous = null;
      }
    }

    for (Key key : keys) {
      Object[] gapfillRow = new Object[numResultColumns];
      int keyIndex = 0;
      if (resultColumnDataTypes[0] == ColumnDataType.LONG) {
        gapfillRow[0] = Long.valueOf(_dateTimeFormatter.fromMillisToFormat(bucketTime));
      } else {
        gapfillRow[0] = _dateTimeFormatter.fromMillisToFormat(bucketTime);
      }
      for (int i = 1; i < _isGroupBySelections.length; i++) {
        if (_isGroupBySelections[i]) {
          gapfillRow[i] = key.getValues()[keyIndex++];
        } else {
          gapfillRow[i] = getFillValue(i, dataSchema.getColumnName(i), key, resultColumnDataTypes[i]);
        }
      }

      if (postGapfillFilterHandler == null || postGapfillFilterHandler.isMatch(gapfillRow)) {
        bucketedResult.add(gapfillRow);
        if (bucketedResult.size() > _limitForGapfilledResult) {
          break;
        }
      }
    }
    return previous;
  }

  /**
   * Make sure that the outer query has the group by clause and the group by clause has the time bucket.
   */
  private void validateGroupByForOuterQuery() {
    List<ExpressionContext> groupbyExpressions = _queryContext.getGroupByExpressions();
    Preconditions.checkArgument(groupbyExpressions != null, "No GroupBy Clause.");
    List<ExpressionContext> innerSelections = _queryContext.getSubQueryContext().getSelectExpressions();
    String timeBucketCol = null;
    List<String> strAlias = _queryContext.getSubQueryContext().getAliasList();
    for (int i = 0; i < innerSelections.size(); i++) {
      ExpressionContext innerSelection = innerSelections.get(i);
      if (GapfillUtils.isGapfill(innerSelection)) {
        if (strAlias.get(i) != null) {
          timeBucketCol = strAlias.get(i);
        } else {
          timeBucketCol = innerSelection.getFunction().getArguments().get(0).toString();
        }
        break;
      }
    }

    Preconditions.checkArgument(timeBucketCol != null, "No Group By timebucket.");

    boolean findTimeBucket = false;
    for (ExpressionContext groupbyExp : groupbyExpressions) {
      if (timeBucketCol.equals(groupbyExp.toString())) {
        findTimeBucket = true;
        break;
      }
    }

    Preconditions.checkArgument(findTimeBucket, "No Group By timebucket.");
  }

  private List<Object[]> aggregateGapfilledData(List<Object[]> bucketedRows, DataSchema dataSchema) {
    List<ExpressionContext> groupbyExpressions = _queryContext.getGroupByExpressions();
    Preconditions.checkArgument(groupbyExpressions != null, "No GroupBy Clause.");
    Map<String, Integer> indexes = new HashMap<>();
    for (int i = 0; i < dataSchema.getColumnNames().length; i++) {
      indexes.put(dataSchema.getColumnName(i), i);
    }

    Map<List<Object>, Integer> groupKeyIndexes = new HashMap<>();
    int[] groupKeyArray = new int[bucketedRows.size()];
    List<Object[]> aggregatedResult = new ArrayList<>();
    for (int j = 0; j < bucketedRows.size(); j++) {
      Object [] bucketedRow = bucketedRows.get(j);
      List<Object> groupKey = new ArrayList<>(groupbyExpressions.size());
      for (ExpressionContext groupbyExpression : groupbyExpressions) {
        int columnIndex = indexes.get(groupbyExpression.toString());
        groupKey.add(bucketedRow[columnIndex]);
      }
      if (groupKeyIndexes.containsKey(groupKey)) {
        groupKeyArray[j] = groupKeyIndexes.get(groupKey);
      } else {
        // create the new groupBy Result row and fill the group by key
        groupKeyArray[j] = groupKeyIndexes.size();
        groupKeyIndexes.put(groupKey, groupKeyIndexes.size());
        Object[] row = new Object[_queryContext.getSelectExpressions().size()];
        for (int i = 0; i < _queryContext.getSelectExpressions().size(); i++) {
          ExpressionContext expressionContext = _queryContext.getSelectExpressions().get(i);
          if (expressionContext.getType() != ExpressionContext.Type.FUNCTION) {
            row[i] = bucketedRow[indexes.get(expressionContext.toString())];
          }
        }
        aggregatedResult.add(row);
      }
    }

    Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
    for (int i = 1; i < dataSchema.getColumnNames().length; i++) {
      blockValSetMap.put(ExpressionContext.forIdentifier(dataSchema.getColumnName(i)),
          new ColumnDataToBlockValSetConverter(dataSchema.getColumnDataType(i), bucketedRows, i));
    }

    for (int i = 0; i < _queryContext.getSelectExpressions().size(); i++) {
      ExpressionContext expressionContext = _queryContext.getSelectExpressions().get(i);
      if (expressionContext.getType() == ExpressionContext.Type.FUNCTION) {
        FunctionContext functionContext = expressionContext.getFunction();
        AggregationFunction aggregationFunction =
            AggregationFunctionFactory.getAggregationFunction(functionContext, _queryContext);
        GroupByResultHolder groupByResultHolder =
            aggregationFunction.createGroupByResultHolder(_groupByKeys.size(), _groupByKeys.size());
        aggregationFunction
            .aggregateGroupBySV(bucketedRows.size(), groupKeyArray, groupByResultHolder, blockValSetMap);
        for (int j = 0; j < groupKeyIndexes.size(); j++) {
          Object [] row = aggregatedResult.get(j);
          row[i] = aggregationFunction.extractGroupByResult(groupByResultHolder, j);
          row[i] = aggregationFunction.extractFinalResult(row[i]);
        }
      }
    }
    return aggregatedResult;
  }

  private Object getFillValue(int columnIndex, String columnName, Object key, ColumnDataType dataType) {
    ExpressionContext expressionContext = _fillExpressions.get(columnName);
    if (expressionContext != null
        && expressionContext.getFunction() != null
        && GapfillUtils.isFill(expressionContext)) {
      List<ExpressionContext> args = expressionContext.getFunction().getArguments();
      if (args.get(1).getLiteral() == null) {
        throw new UnsupportedOperationException("Wrong Sql.");
      }
      GapfillUtils.FillType fillType = GapfillUtils.FillType.valueOf(args.get(1).getLiteral());
      if (fillType == GapfillUtils.FillType.FILL_DEFAULT_VALUE) {
        // TODO: may fill the default value from sql in the future.
        return GapfillUtils.getDefaultValue(dataType);
      } else if (fillType == GapfillUtils.FillType.FILL_PREVIOUS_VALUE) {
        Object[] row = _previousByGroupKey.get(key);
        if (row != null) {
          return row[columnIndex];
        } else {
          return GapfillUtils.getDefaultValue(dataType);
        }
      } else {
        throw new UnsupportedOperationException("unsupported fill type.");
      }
    } else {
      return GapfillUtils.getDefaultValue(dataType);
    }
  }

  /**
   * Helper method to get the type-compatible {@link Comparator} based on the timestamp.
   *
   * @return flexible {@link Comparator} based on the timestamp.
   */
  private Comparator<Object[]> getTypeCompatibleComparator(DataSchema dataSchema) {
    ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();

    return (o1, o2) -> {
      Object v1 = o1[0];
      Object v2 = o2[0];
      int result;
      if (columnDataTypes[0].isNumber()) {
        result = Double.compare(((Number) v1).doubleValue(), ((Number) v2).doubleValue());
      } else {
        long timeCol1 = _dateTimeFormatter.fromFormatToMillis(String.valueOf(v1));
        long timeCol2 = _dateTimeFormatter.fromFormatToMillis(String.valueOf(v2));
        if (timeCol1 < timeCol2) {
          return -1;
        } else if (timeCol1 == timeCol2) {
          return 0;
        } else {
          return 1;
        }
      }
      return result;
    };
  }

  /**
   * Merge all result tables from different pinot servers and sort the rows based on timebucket.
   */
  private List<Object[]> mergeAndSort(Collection<DataTable> dataTables, DataSchema dataSchema) {
    PriorityQueue<Object[]> rows = new PriorityQueue<>(Math.min(_limitForAggregatedResult,
        SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY),
        getTypeCompatibleComparator(dataSchema));

    for (DataTable dataTable : dataTables) {
      int numRows = dataTable.getNumberOfRows();
      for (int rowId = 0; rowId < numRows; rowId++) {
        Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
        rows.add(row);
      }
    }
    LinkedList<Object[]> sortedRows = new LinkedList<>();
    while (!rows.isEmpty()) {
      Object[] row = rows.poll();
      sortedRows.add(row);
      _groupByKeys.add(constructGroupKeys(row));
    }
    return sortedRows;
  }
}
