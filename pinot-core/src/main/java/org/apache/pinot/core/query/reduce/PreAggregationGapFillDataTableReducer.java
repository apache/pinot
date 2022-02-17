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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.data.table.ConcurrentIndexedTable;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.SimpleIndexedTable;
import org.apache.pinot.core.data.table.UnboundedConcurrentIndexedTable;
import org.apache.pinot.core.operator.combine.GroupByOrderByCombineOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.GapfillUtils;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;

/**
 * Helper class to reduce and set Aggregation results into the BrokerResponseNative
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class PreAggregationGapFillDataTableReducer implements DataTableReducer {
  private static final int MIN_DATA_TABLES_FOR_CONCURRENT_REDUCE = 2; // TBD, find a better value.

  private final QueryContext _queryContext;

  private final int _limitForAggregatedResult;
  private int _limitForGapfilledResult;

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
  private final GapfillUtils.GapfillType _gapfillType;

  PreAggregationGapFillDataTableReducer(QueryContext queryContext) {
    _queryContext = queryContext;
    _gapfillType = queryContext.getGapfillType();
    _limitForAggregatedResult = queryContext.getLimit();
    if (_gapfillType == GapfillUtils.GapfillType.AGGREGATE_GAP_FILL
        || _gapfillType == GapfillUtils.GapfillType.GAP_FILL) {
      _limitForGapfilledResult = queryContext.getLimit();
    } else {
      _limitForGapfilledResult = queryContext.getSubQueryContext().getLimit();
    }

    ExpressionContext gapFillSelection = GapfillUtils.getGapfillExpressionContext(queryContext);

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
    QueryContext queryContext;
    if (_gapfillType == GapfillUtils.GapfillType.AGGREGATE_GAP_FILL_AGGREGATE) {
      queryContext = _queryContext.getSubQueryContext().getSubQueryContext();
    } else if (_gapfillType == GapfillUtils.GapfillType.GAP_FILL) {
      queryContext = _queryContext;
    } else {
      queryContext = _queryContext.getSubQueryContext();
    }
    List<String> aliasList = queryContext.getAliasList();
    Map<String, String> columnNameToAliasMap = new HashMap<>();
    for (int i = 0; i < aliasList.size(); i++) {
      if (aliasList.get(i) != null) {
        ExpressionContext selection = queryContext.getSelectExpressions().get(i);
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
   * Computes the number of reduce threads to use per query.
   * <ul>
   *   <li> Use single thread if number of data tables to reduce is less than
   *   {@value #MIN_DATA_TABLES_FOR_CONCURRENT_REDUCE}.</li>
   *   <li> Else, use min of max allowed reduce threads per query, and number of data tables.</li>
   * </ul>
   *
   * @param numDataTables Number of data tables to reduce
   * @param maxReduceThreadsPerQuery Max allowed reduce threads per query
   * @return Number of reduce threads to use for the query
   */
  private int getNumReduceThreadsToUse(int numDataTables, int maxReduceThreadsPerQuery) {
    // Use single thread if number of data tables < MIN_DATA_TABLES_FOR_CONCURRENT_REDUCE.
    if (numDataTables < MIN_DATA_TABLES_FOR_CONCURRENT_REDUCE) {
      return Math.min(1, numDataTables); // Number of data tables can be zero.
    }

    return Math.min(maxReduceThreadsPerQuery, numDataTables);
  }

  private IndexedTable getIndexedTable(DataSchema dataSchema, Collection<DataTable> dataTablesToReduce,
      DataTableReducerContext reducerContext)
      throws TimeoutException {
    long start = System.currentTimeMillis();
    int numDataTables = dataTablesToReduce.size();

    // Get the number of threads to use for reducing.
    // In case of single reduce thread, fall back to SimpleIndexedTable to avoid redundant locking/unlocking calls.
    int numReduceThreadsToUse = getNumReduceThreadsToUse(numDataTables, reducerContext.getMaxReduceThreadsPerQuery());
    int limit = _queryContext.getLimit();
    // TODO: Make minTrimSize configurable
    int trimSize = GroupByUtils.getTableCapacity(limit);
    // NOTE: For query with HAVING clause, use trimSize as resultSize to ensure the result accuracy.
    // TODO: Resolve the HAVING clause within the IndexedTable before returning the result
    int resultSize = _queryContext.getHavingFilter() != null ? trimSize : limit;
    int trimThreshold = reducerContext.getGroupByTrimThreshold();
    IndexedTable indexedTable;
    if (numReduceThreadsToUse <= 1) {
      indexedTable = new SimpleIndexedTable(dataSchema, _queryContext, resultSize, trimSize, trimThreshold);
    } else {
      if (trimThreshold >= GroupByOrderByCombineOperator.MAX_TRIM_THRESHOLD) {
        // special case of trim threshold where it is set to max value.
        // there won't be any trimming during upsert in this case.
        // thus we can avoid the overhead of read-lock and write-lock
        // in the upsert method.
        indexedTable = new UnboundedConcurrentIndexedTable(dataSchema, _queryContext, resultSize);
      } else {
        indexedTable = new ConcurrentIndexedTable(dataSchema, _queryContext, resultSize, trimSize, trimThreshold);
      }
    }

    Future[] futures = new Future[numDataTables];
    CountDownLatch countDownLatch = new CountDownLatch(numDataTables);

    // Create groups of data tables that each thread can process concurrently.
    // Given that numReduceThreads is <= numDataTables, each group will have at least one data table.
    ArrayList<DataTable> dataTables = new ArrayList<>(dataTablesToReduce);
    List<List<DataTable>> reduceGroups = new ArrayList<>(numReduceThreadsToUse);

    for (int i = 0; i < numReduceThreadsToUse; i++) {
      reduceGroups.add(new ArrayList<>());
    }
    for (int i = 0; i < numDataTables; i++) {
      reduceGroups.get(i % numReduceThreadsToUse).add(dataTables.get(i));
    }

    int cnt = 0;
    ColumnDataType[] storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();
    int numColumns = storedColumnDataTypes.length;
    for (List<DataTable> reduceGroup : reduceGroups) {
      futures[cnt++] = reducerContext.getExecutorService().submit(new TraceRunnable() {
        @Override
        public void runJob() {
          for (DataTable dataTable : reduceGroup) {
            int numRows = dataTable.getNumberOfRows();

            try {
              for (int rowId = 0; rowId < numRows; rowId++) {
                Object[] values = new Object[numColumns];
                for (int colId = 0; colId < numColumns; colId++) {
                  switch (storedColumnDataTypes[colId]) {
                    case INT:
                      values[colId] = dataTable.getInt(rowId, colId);
                      break;
                    case LONG:
                      values[colId] = dataTable.getLong(rowId, colId);
                      break;
                    case FLOAT:
                      values[colId] = dataTable.getFloat(rowId, colId);
                      break;
                    case DOUBLE:
                      values[colId] = dataTable.getDouble(rowId, colId);
                      break;
                    case STRING:
                      values[colId] = dataTable.getString(rowId, colId);
                      break;
                    case BYTES:
                      values[colId] = dataTable.getBytes(rowId, colId);
                      break;
                    case OBJECT:
                      values[colId] = dataTable.getObject(rowId, colId);
                      break;
                    // Add other aggregation intermediate result / group-by column type supports here
                    default:
                      throw new IllegalStateException();
                  }
                }
                indexedTable.upsert(new Record(values));
              }
            } finally {
              countDownLatch.countDown();
            }
          }
        }
      });
    }

    try {
      long timeOutMs = reducerContext.getReduceTimeOutMs() - (System.currentTimeMillis() - start);
      countDownLatch.await(timeOutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      for (Future future : futures) {
        if (!future.isDone()) {
          future.cancel(true);
        }
      }
      throw new TimeoutException("Timed out in broker reduce phase.");
    }

    indexedTable.finish(true);
    return indexedTable;
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
    DataSchema resultTableSchema = getResultTableDataSchema(dataSchema);
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

    List<Object[]> sortedRawRows;
    if (_gapfillType == GapfillUtils.GapfillType.GAP_FILL_AGGREGATE
        || _gapfillType == GapfillUtils.GapfillType.GAP_FILL
        || _gapfillType == GapfillUtils.GapfillType.GAP_FILL_SELECT) {
      sortedRawRows = mergeAndSort(dataTableMap.values(), dataSchema);
    } else {
      try {
        IndexedTable indexedTable = getIndexedTable(dataSchema, dataTableMap.values(), reducerContext);
        sortedRawRows = mergeAndSort(indexedTable, dataSchema);
      } catch (TimeoutException e) {
        brokerResponseNative.getProcessingExceptions()
            .add(new QueryProcessingException(QueryException.BROKER_TIMEOUT_ERROR_CODE, e.getMessage()));
        return;
      }
    }
    List<Object[]> resultRows;
    replaceColumnNameWithAlias(dataSchema);
    if (_queryContext.getAggregationFunctions() != null) {
      validateGroupByForOuterQuery();
    }

    if (_gapfillType == GapfillUtils.GapfillType.GAP_FILL_AGGREGATE
        || _gapfillType == GapfillUtils.GapfillType.GAP_FILL
        || _gapfillType == GapfillUtils.GapfillType.GAP_FILL_SELECT) {
      List<Object[]> gapfilledRows = gapFillAndAggregate(sortedRawRows, resultTableSchema, dataSchema);
      if (_gapfillType == GapfillUtils.GapfillType.GAP_FILL_SELECT) {
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
    AggregationFunction[] aggregationFunctions;
    if (_gapfillType == GapfillUtils.GapfillType.AGGREGATE_GAP_FILL) {
      aggregationFunctions = _queryContext.getSubQueryContext().getAggregationFunctions();
    } else {
      aggregationFunctions = _queryContext.getSubQueryContext().getSubQueryContext().getAggregationFunctions();
    }
    int numAggregationFunctionsForInnerQuery = aggregationFunctions == null ? 0 : aggregationFunctions.length;
    for (int i = 0; i < numAggregationFunctionsForInnerQuery; i++) {
      int valueIndex = _timeSeries.size() + 1 + i;
      row[valueIndex] = aggregationFunctions[i].extractFinalResult(row[valueIndex]);
    }
  }

  private void setupColumnTypeForAggregatedColum(ColumnDataType[] columnDataTypes) {
    AggregationFunction[] aggregationFunctions;
    if (_gapfillType == GapfillUtils.GapfillType.AGGREGATE_GAP_FILL) {
      aggregationFunctions = _queryContext.getSubQueryContext().getAggregationFunctions();
    } else {
      aggregationFunctions = _queryContext.getSubQueryContext().getSubQueryContext().getAggregationFunctions();
    }
    int numAggregationFunctionsForInnerQuery = aggregationFunctions == null ? 0 : aggregationFunctions.length;
    for (int i = 0; i < numAggregationFunctionsForInnerQuery; i++) {
      columnDataTypes[_timeSeries.size() + 1 + i] = aggregationFunctions[i].getFinalResultColumnType();
    }
  }

  /**
   * Constructs the DataSchema for the ResultTable.
   */
  private DataSchema getResultTableDataSchema(DataSchema dataSchema) {
    if (_gapfillType == GapfillUtils.GapfillType.GAP_FILL) {
      return dataSchema;
    }

    int numOfColumns = _queryContext.getSelectExpressions().size();
    String [] columnNames = new String[numOfColumns];
    ColumnDataType [] columnDataTypes = new ColumnDataType[numOfColumns];
    for (int i = 0; i < numOfColumns; i++) {
      ExpressionContext expressionContext = _queryContext.getSelectExpressions().get(i);
      if (GapfillUtils.isGapfill(expressionContext)) {
        expressionContext = expressionContext.getFunction().getArguments().get(0);
      }
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
    if (_queryContext.getSubQueryContext() != null && _queryContext.getFilter() != null) {
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
          if (bucketedResult.size() >= _limitForGapfilledResult) {
            _limitForGapfilledResult = 0;
            break;
          } else {
            bucketedResult.add(resultRow);
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
        if (bucketedResult.size() >= _limitForGapfilledResult) {
          break;
        } else {
          bucketedResult.add(gapfillRow);
        }
      }
    }
    if (_limitForGapfilledResult > _groupByKeys.size()) {
      _limitForGapfilledResult -= _groupByKeys.size();
    } else {
      _limitForGapfilledResult = 0;
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
    for (int i = 0; i < bucketedRows.size(); i++) {
      Object [] bucketedRow = bucketedRows.get(i);
      List<Object> groupKey = new ArrayList<>(groupbyExpressions.size());
      for (ExpressionContext groupbyExpression : groupbyExpressions) {
        int columnIndex = indexes.get(groupbyExpression.toString());
        groupKey.add(bucketedRow[columnIndex]);
      }
      if (groupKeyIndexes.containsKey(groupKey)) {
        groupKeyArray[i] = groupKeyIndexes.get(groupKey);
      } else {
        // create the new groupBy Result row and fill the group by key
        groupKeyArray[i] = groupKeyIndexes.size();
        groupKeyIndexes.put(groupKey, groupKeyIndexes.size());
        Object[] row = new Object[_queryContext.getSelectExpressions().size()];
        for (int j = 0; j < _queryContext.getSelectExpressions().size(); j++) {
          ExpressionContext expressionContext = _queryContext.getSelectExpressions().get(j);
          if (expressionContext.getType() != ExpressionContext.Type.FUNCTION) {
            row[j] = bucketedRow[indexes.get(expressionContext.toString())];
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
        aggregationFunction.aggregateGroupBySV(bucketedRows.size(), groupKeyArray, groupByResultHolder, blockValSetMap);
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

  private List<Object[]> mergeAndSort(IndexedTable indexedTable, DataSchema dataSchema) {
    PriorityQueue<Object[]> rows = new PriorityQueue<>(Math.min(_limitForAggregatedResult,
        SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY),
        getTypeCompatibleComparator(dataSchema));

    Iterator<Record> iterator = indexedTable.iterator();
    while (iterator.hasNext()) {
      rows.add(iterator.next().getValues());
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
