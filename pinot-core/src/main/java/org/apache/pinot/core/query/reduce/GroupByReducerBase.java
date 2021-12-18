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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.GroupByResult;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.data.table.ConcurrentIndexedTable;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.SimpleIndexedTable;
import org.apache.pinot.core.data.table.UnboundedConcurrentIndexedTable;
import org.apache.pinot.core.operator.combine.GroupByOrderByCombineOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByTrimmingService;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.core.util.QueryOptionsUtils;
import org.apache.pinot.core.util.trace.TraceRunnable;


/**
 * Helper class to reduce data tables and set group by results into the BrokerResponseNative
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class GroupByReducerBase {
  private static final int MIN_DATA_TABLES_FOR_CONCURRENT_REDUCE = 2; // TBD, find a better value.

  protected final QueryContext _queryContext;
  protected final AggregationFunction[] _aggregationFunctions;
  protected final int _numAggregationFunctions;
  protected final List<ExpressionContext> _groupByExpressions;
  protected final int _numGroupByExpressions;
  protected final int _numColumns;
  protected final boolean _preserveType;
  protected final boolean _groupByModeSql;
  protected final boolean _responseFormatSql;
  protected final boolean _sqlQuery;

  public GroupByReducerBase(QueryContext queryContext) {
    _queryContext = queryContext;
    _aggregationFunctions = queryContext.getAggregationFunctions();
    assert _aggregationFunctions != null;
    _numAggregationFunctions = _aggregationFunctions.length;
    _groupByExpressions = queryContext.getGroupByExpressions();
    assert _groupByExpressions != null;
    _numGroupByExpressions = _groupByExpressions.size();
    _numColumns = _numAggregationFunctions + _numGroupByExpressions;
    Map<String, String> queryOptions = queryContext.getQueryOptions();
    _preserveType = QueryOptionsUtils.isPreserveType(queryOptions);
    _groupByModeSql = QueryOptionsUtils.isGroupByModeSQL(queryOptions);
    _responseFormatSql = QueryOptionsUtils.isResponseFormatSQL(queryOptions);
    _sqlQuery = queryContext.getBrokerRequest().getPinotQuery() != null;
  }

  /**
   * Extract group by order by results and set into {@link ResultTable}
   * @param brokerResponseNative broker response
   * @param dataSchema data schema
   * @param dataTables Collection of data tables
   * @param reducerContext DataTableReducer context
   * @param rawTableName table name
   * @param brokerMetrics broker metrics (meters)
   * @throws TimeoutException If unable complete within timeout.
   */
  protected void setSQLGroupByInResultTable(BrokerResponseNative brokerResponseNative, DataSchema dataSchema,
      Collection<DataTable> dataTables, DataTableReducerContext reducerContext, String rawTableName,
      BrokerMetrics brokerMetrics)
      throws TimeoutException {
    IndexedTable indexedTable = getIndexedTable(dataSchema, dataTables, reducerContext);
    if (brokerMetrics != null) {
      brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.NUM_RESIZES, indexedTable.getNumResizes());
      brokerMetrics.addValueToTableGauge(rawTableName, BrokerGauge.RESIZE_TIME_MS, indexedTable.getResizeTimeMs());
    }
    Iterator<Record> sortedIterator = indexedTable.iterator();
    DataSchema prePostAggregationDataSchema = getPrePostAggregationDataSchema(dataSchema);
    ColumnDataType[] columnDataTypes = prePostAggregationDataSchema.getColumnDataTypes();
    int numColumns = columnDataTypes.length;
    int limit = _queryContext.getLimit();
    List<Object[]> rows = new ArrayList<>(limit);

    if (_sqlQuery) {
      // SQL query with SQL group-by mode and response format

      PostAggregationHandler postAggregationHandler =
          new PostAggregationHandler(_queryContext, prePostAggregationDataSchema);
      FilterContext havingFilter = _queryContext.getHavingFilter();
      if (havingFilter != null) {
        HavingFilterHandler havingFilterHandler = new HavingFilterHandler(havingFilter, postAggregationHandler);
        while (rows.size() < limit && sortedIterator.hasNext()) {
          Object[] row = sortedIterator.next().getValues();
          extractFinalAggregationResults(row);
          for (int i = 0; i < numColumns; i++) {
            row[i] = columnDataTypes[i].convert(row[i]);
          }
          if (havingFilterHandler.isMatch(row)) {
            rows.add(row);
          }
        }
      } else {
        for (int i = 0; i < limit && sortedIterator.hasNext(); i++) {
          Object[] row = sortedIterator.next().getValues();
          extractFinalAggregationResults(row);
          for (int j = 0; j < numColumns; j++) {
            row[j] = columnDataTypes[j].convert(row[j]);
          }
          rows.add(row);
        }
      }
      DataSchema resultDataSchema = postAggregationHandler.getResultDataSchema();
      ColumnDataType[] resultColumnDataTypes = resultDataSchema.getColumnDataTypes();
      int numResultColumns = resultColumnDataTypes.length;
      int numResultRows = rows.size();
      List<Object[]> resultRows = new ArrayList<>(numResultRows);
      for (Object[] row : rows) {
        Object[] resultRow = postAggregationHandler.getResult(row);
        for (int i = 0; i < numResultColumns; i++) {
          resultRow[i] = resultColumnDataTypes[i].format(resultRow[i]);
        }
        resultRows.add(resultRow);
      }
      brokerResponseNative.setResultTable(new ResultTable(resultDataSchema, resultRows));
    } else {
      // PQL query with SQL group-by mode and response format
      // NOTE: For PQL query, keep the order of columns as is (group-by expressions followed by aggregations), no need
      //       to perform post-aggregation or filtering.

      for (int i = 0; i < limit && sortedIterator.hasNext(); i++) {
        Object[] row = sortedIterator.next().getValues();
        extractFinalAggregationResults(row);
        for (int j = 0; j < numColumns; j++) {
          row[j] = columnDataTypes[j].convertAndFormat(row[j]);
        }
        rows.add(row);
      }
      brokerResponseNative.setResultTable(new ResultTable(prePostAggregationDataSchema, rows));
    }
  }

  /**
   * Helper method to extract the final aggregation results for the given row (in-place).
   */
  protected void extractFinalAggregationResults(Object[] row) {
    for (int i = 0; i < _numAggregationFunctions; i++) {
      int valueIndex = i + _numGroupByExpressions;
      row[valueIndex] = _aggregationFunctions[i].extractFinalResult(row[valueIndex]);
    }
  }

  /**
   * Constructs the DataSchema for the rows before the post-aggregation (SQL mode).
   */
  protected DataSchema getPrePostAggregationDataSchema(DataSchema dataSchema) {
    String[] columnNames = dataSchema.getColumnNames();
    ColumnDataType[] columnDataTypes = new ColumnDataType[_numColumns];
    System.arraycopy(dataSchema.getColumnDataTypes(), 0, columnDataTypes, 0, _numGroupByExpressions);
    for (int i = 0; i < _numAggregationFunctions; i++) {
      columnDataTypes[i + _numGroupByExpressions] = _aggregationFunctions[i].getFinalResultColumnType();
    }
    return new DataSchema(columnNames, columnDataTypes);
  }

  protected IndexedTable getIndexedTable(DataSchema dataSchema, Collection<DataTable> dataTablesToReduce,
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
    for (List<DataTable> reduceGroup : reduceGroups) {
      futures[cnt++] = reducerContext.getExecutorService().submit(new TraceRunnable() {
        @Override
        public void runJob() {
          for (DataTable dataTable : reduceGroup) {
            int numRows = dataTable.getNumberOfRows();

            try {
              for (int rowId = 0; rowId < numRows; rowId++) {
                Object[] values = new Object[_numColumns];
                for (int colId = 0; colId < _numColumns; colId++) {
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

  /**
   * Extract the results of group by order by into a List of {@link AggregationResult}
   * There will be 1 aggregation result per aggregation. The group by keys will be the same across all aggregations
   * @param brokerResponseNative broker response
   * @param dataSchema data schema
   * @param dataTables Collection of data tables
   * @param reducerContext DataTableReducer context
   * @throws TimeoutException If unable to complete within the timeout.
   */
  protected void setSQLGroupByInAggregationResults(BrokerResponseNative brokerResponseNative, DataSchema dataSchema,
      Collection<DataTable> dataTables, DataTableReducerContext reducerContext)
      throws TimeoutException {

    List<String> groupByColumns = new ArrayList<>(_numGroupByExpressions);
    int idx = 0;
    while (idx < _numGroupByExpressions) {
      groupByColumns.add(dataSchema.getColumnName(idx));
      idx++;
    }

    List<String> aggregationColumns = new ArrayList<>(_numAggregationFunctions);
    List<List<GroupByResult>> groupByResults = new ArrayList<>(_numAggregationFunctions);
    while (idx < _numColumns) {
      aggregationColumns.add(dataSchema.getColumnName(idx));
      groupByResults.add(new ArrayList<>());
      idx++;
    }

    if (!dataTables.isEmpty()) {
      IndexedTable indexedTable = getIndexedTable(dataSchema, dataTables, reducerContext);

      int limit = _queryContext.getLimit();
      Iterator<Record> sortedIterator = indexedTable.iterator();
      int numRows = 0;
      while (numRows < limit && sortedIterator.hasNext()) {
        Record nextRecord = sortedIterator.next();
        Object[] values = nextRecord.getValues();

        int index = 0;
        List<String> group = new ArrayList<>(_numGroupByExpressions);
        while (index < _numGroupByExpressions) {
          group.add(values[index].toString());
          index++;
        }

        int aggNum = 0;
        while (index < _numColumns) {
          Serializable serializableValue =
              getSerializableValue(_aggregationFunctions[aggNum].extractFinalResult(values[index]));
          if (!_preserveType) {
            serializableValue = AggregationFunctionUtils.formatValue(serializableValue);
          }
          GroupByResult groupByResult = new GroupByResult();
          groupByResult.setGroup(group);
          groupByResult.setValue(serializableValue);
          groupByResults.get(aggNum).add(groupByResult);
          index++;
          aggNum++;
        }
        numRows++;
      }
    }

    List<AggregationResult> aggregationResults = new ArrayList<>(_numAggregationFunctions);
    for (int i = 0; i < _numAggregationFunctions; i++) {
      AggregationResult aggregationResult =
          new AggregationResult(groupByResults.get(i), groupByColumns, aggregationColumns.get(i));
      aggregationResults.add(aggregationResult);
    }
    brokerResponseNative.setAggregationResults(aggregationResults);
  }

  protected Serializable getSerializableValue(Object value) {
    if (value instanceof Number) {
      return (Number) value;
    } else {
      return value.toString();
    }
  }

  /**
   * Constructs the final result table schema for PQL execution mode
   */
  protected DataSchema getPQLResultTableSchema(AggregationFunction aggregationFunction) {
    String[] columnNames = new String[_numColumns];
    ColumnDataType[] columnDataTypes = new ColumnDataType[_numColumns];
    for (int i = 0; i < _numGroupByExpressions; i++) {
      columnNames[i] = _groupByExpressions.get(i).toString();
      columnDataTypes[i] = ColumnDataType.STRING;
    }
    columnNames[_numGroupByExpressions] = aggregationFunction.getResultColumnName();
    columnDataTypes[_numGroupByExpressions] = aggregationFunction.getFinalResultColumnType();
    return new DataSchema(columnNames, columnDataTypes);
  }

  /**
   * Reduce group-by results from multiple servers and set them into BrokerResponseNative passed in.
   *
   * @param brokerResponseNative broker response.
   * @param dataTables Collection of data tables
   */
  protected void setGroupByResults(BrokerResponseNative brokerResponseNative, Collection<DataTable> dataTables) {

    // Merge results from all data tables.
    String[] columnNames = new String[_numAggregationFunctions];
    Map<String, Object>[] intermediateResultMaps = new Map[_numAggregationFunctions];
    for (DataTable dataTable : dataTables) {
      for (int i = 0; i < _numAggregationFunctions; i++) {
        if (columnNames[i] == null) {
          columnNames[i] = dataTable.getString(i, 0);
          intermediateResultMaps[i] = dataTable.getObject(i, 1);
        } else {
          mergeResultMap(intermediateResultMaps[i], dataTable.getObject(i, 1), _aggregationFunctions[i]);
        }
      }
    }

    // Extract final result maps from the merged intermediate result maps.
    Map<String, Comparable>[] finalResultMaps = new Map[_numAggregationFunctions];
    for (int i = 0; i < _numAggregationFunctions; i++) {
      Map<String, Object> intermediateResultMap = intermediateResultMaps[i];
      Map<String, Comparable> finalResultMap = new HashMap<>();
      for (String groupKey : intermediateResultMap.keySet()) {
        Object intermediateResult = intermediateResultMap.get(groupKey);
        finalResultMap.put(groupKey, _aggregationFunctions[i].extractFinalResult(intermediateResult));
      }
      finalResultMaps[i] = finalResultMap;
    }

    // Trim the final result maps and set them into the broker response.
    AggregationGroupByTrimmingService aggregationGroupByTrimmingService =
        new AggregationGroupByTrimmingService(_queryContext);
    List<GroupByResult>[] groupByResultLists = aggregationGroupByTrimmingService.trimFinalResults(finalResultMaps);

    if (_responseFormatSql) {
      assert _numAggregationFunctions == 1;
      List<GroupByResult> groupByResultList = groupByResultLists[0];
      List<Object[]> rows = new ArrayList<>();
      for (GroupByResult groupByResult : groupByResultList) {
        Object[] row = new Object[_numColumns];
        int i = 0;
        for (String column : groupByResult.getGroup()) {
          row[i++] = column;
        }
        row[i] = groupByResult.getValue();
        rows.add(row);
      }
      DataSchema finalDataSchema = getPQLResultTableSchema(_aggregationFunctions[0]);
      brokerResponseNative.setResultTable(new ResultTable(finalDataSchema, rows));
    } else {
      // Format the value into string if required
      if (!_preserveType) {
        for (List<GroupByResult> groupByResultList : groupByResultLists) {
          for (GroupByResult groupByResult : groupByResultList) {
            groupByResult.setValue(AggregationFunctionUtils.formatValue(groupByResult.getValue()));
          }
        }
      }
      List<String> groupByColumns = new ArrayList<>(_numGroupByExpressions);
      for (ExpressionContext groupByExpression : _groupByExpressions) {
        groupByColumns.add(groupByExpression.toString());
      }
      List<AggregationResult> aggregationResults = new ArrayList<>(_numAggregationFunctions);
      for (int i = 0; i < _numAggregationFunctions; i++) {
        aggregationResults.add(new AggregationResult(groupByResultLists[i], groupByColumns, columnNames[i]));
      }
      brokerResponseNative.setAggregationResults(aggregationResults);
    }
  }

  /**
   * Helper method to merge 2 intermediate result maps.
   */
  protected void mergeResultMap(Map<String, Object> mergedResultMap, Map<String, Object> resultMapToMerge,
      AggregationFunction aggregationFunction) {
    for (Map.Entry<String, Object> entry : resultMapToMerge.entrySet()) {
      String groupKey = entry.getKey();
      Object resultToMerge = entry.getValue();
      mergedResultMap.compute(groupKey, (k, v) -> {
        if (v == null) {
          return resultToMerge;
        } else {
          return aggregationFunction.merge(v, resultToMerge);
        }
      });
    }
  }
}
