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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
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
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.roaringbitmap.RoaringBitmap;


/**
 * Helper class to reduce data tables and set group by results into the BrokerResponseNative
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class GroupByDataTableReducer implements DataTableReducer {
  private static final int MIN_DATA_TABLES_FOR_CONCURRENT_REDUCE = 2; // TBD, find a better value.

  private final QueryContext _queryContext;
  private final AggregationFunction[] _aggregationFunctions;
  private final int _numAggregationFunctions;
  private final List<ExpressionContext> _groupByExpressions;
  private final int _numGroupByExpressions;
  private final int _numColumns;

  GroupByDataTableReducer(QueryContext queryContext) {
    _queryContext = queryContext;
    _aggregationFunctions = queryContext.getAggregationFunctions();
    assert _aggregationFunctions != null;
    _numAggregationFunctions = _aggregationFunctions.length;
    _groupByExpressions = queryContext.getGroupByExpressions();
    assert _groupByExpressions != null;
    _numGroupByExpressions = _groupByExpressions.size();
    _numColumns = _numAggregationFunctions + _numGroupByExpressions;
  }

  /**
   * Reduces and sets group by results into ResultTable.
   */
  @Override
  public void reduceAndSetResults(String tableName, DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap, BrokerResponseNative brokerResponseNative,
      DataTableReducerContext reducerContext, BrokerMetrics brokerMetrics) {
    assert dataSchema != null;
    try {
      reduceToResultTable(brokerResponseNative, dataSchema, dataTableMap.values(), reducerContext, tableName,
          brokerMetrics);
      if (brokerMetrics != null) {
        brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.GROUP_BY_SIZE,
            brokerResponseNative.getResultTable().getRows().size());
      }
    } catch (TimeoutException e) {
      brokerResponseNative.getProcessingExceptions()
          .add(new QueryProcessingException(QueryException.BROKER_TIMEOUT_ERROR_CODE, e.getMessage()));
    }
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
  private void reduceToResultTable(BrokerResponseNative brokerResponseNative, DataSchema dataSchema,
      Collection<DataTable> dataTables, DataTableReducerContext reducerContext, String rawTableName,
      BrokerMetrics brokerMetrics)
      throws TimeoutException {
    int numRecords;
    Iterator<Record> sortedIterator;
    if (!dataTables.isEmpty()) {
      IndexedTable indexedTable = getIndexedTable(dataSchema, dataTables, reducerContext);
      if (brokerMetrics != null) {
        brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.NUM_RESIZES, indexedTable.getNumResizes());
        brokerMetrics.addValueToTableGauge(rawTableName, BrokerGauge.RESIZE_TIME_MS, indexedTable.getResizeTimeMs());
      }
      numRecords = indexedTable.size();
      sortedIterator = indexedTable.iterator();
    } else {
      numRecords = 0;
      sortedIterator = Collections.emptyIterator();
    }

    DataSchema prePostAggregationDataSchema = getPrePostAggregationDataSchema(dataSchema);
    PostAggregationHandler postAggregationHandler =
        new PostAggregationHandler(_queryContext, prePostAggregationDataSchema);
    DataSchema resultDataSchema = postAggregationHandler.getResultDataSchema();

    // Directly return when there is no record returned, or limit is 0
    int limit = _queryContext.getLimit();
    if (numRecords == 0 || limit == 0) {
      brokerResponseNative.setResultTable(new ResultTable(resultDataSchema, Collections.emptyList()));
      return;
    }

    // Calculate rows before post-aggregation
    List<Object[]> rows;
    ColumnDataType[] columnDataTypes = prePostAggregationDataSchema.getColumnDataTypes();
    int numColumns = columnDataTypes.length;
    FilterContext havingFilter = _queryContext.getHavingFilter();
    if (havingFilter != null) {
      rows = new ArrayList<>();
      HavingFilterHandler havingFilterHandler = new HavingFilterHandler(havingFilter, postAggregationHandler,
          _queryContext.isNullHandlingEnabled());
      while (rows.size() < limit && sortedIterator.hasNext()) {
        Object[] row = sortedIterator.next().getValues();
        extractFinalAggregationResults(row);
        for (int i = 0; i < numColumns; i++) {
          Object value = row[i];
          if (value != null) {
            row[i] = columnDataTypes[i].convert(row[i]);
          }
        }
        if (havingFilterHandler.isMatch(row)) {
          rows.add(row);
        }
      }
    } else {
      int numRows = Math.min(numRecords, limit);
      rows = new ArrayList<>(numRows);
      for (int i = 0; i < numRows; i++) {
        Object[] row = sortedIterator.next().getValues();
        extractFinalAggregationResults(row);
        for (int j = 0; j < numColumns; j++) {
          Object value = row[j];
          if (value != null) {
            row[j] = columnDataTypes[j].convert(row[j]);
          }
        }
        rows.add(row);
      }
    }

    // Calculate final result rows after post aggregation
    List<Object[]> resultRows = new ArrayList<>(rows.size());
    ColumnDataType[] resultColumnDataTypes = resultDataSchema.getColumnDataTypes();
    int numResultColumns = resultColumnDataTypes.length;
    for (Object[] row : rows) {
      Object[] resultRow = postAggregationHandler.getResult(row);
      for (int i = 0; i < numResultColumns; i++) {
        Object value = resultRow[i];
        if (value != null) {
          resultRow[i] = resultColumnDataTypes[i].format(value);
        }
      }
      resultRows.add(resultRow);
    }

    brokerResponseNative.setResultTable(new ResultTable(resultDataSchema, resultRows));
  }

  /**
   * Helper method to extract the final aggregation results for the given row (in-place).
   */
  private void extractFinalAggregationResults(Object[] row) {
    for (int i = 0; i < _numAggregationFunctions; i++) {
      int valueIndex = i + _numGroupByExpressions;
      row[valueIndex] = _aggregationFunctions[i].extractFinalResult(row[valueIndex]);
    }
  }

  /**
   * Constructs the DataSchema for the rows before the post-aggregation (SQL mode).
   */
  private DataSchema getPrePostAggregationDataSchema(DataSchema dataSchema) {
    String[] columnNames = dataSchema.getColumnNames();
    ColumnDataType[] columnDataTypes = new ColumnDataType[_numColumns];
    System.arraycopy(dataSchema.getColumnDataTypes(), 0, columnDataTypes, 0, _numGroupByExpressions);
    for (int i = 0; i < _numAggregationFunctions; i++) {
      columnDataTypes[i + _numGroupByExpressions] = _aggregationFunctions[i].getFinalResultColumnType();
    }
    return new DataSchema(columnNames, columnDataTypes);
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
    if (numReduceThreadsToUse == 1) {
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

    Future[] futures = new Future[numReduceThreadsToUse];
    CountDownLatch countDownLatch = new CountDownLatch(numDataTables);
    ColumnDataType[] storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();
    for (int i = 0; i < numReduceThreadsToUse; i++) {
      List<DataTable> reduceGroup = reduceGroups.get(i);
      futures[i] = reducerContext.getExecutorService().submit(new TraceRunnable() {
        @Override
        public void runJob() {
          for (DataTable dataTable : reduceGroup) {
            // Terminate when thread is interrupted. This is expected when the query already fails in the main thread.
            if (Thread.interrupted()) {
              return;
            }
            try {
              boolean nullHandlingEnabled = _queryContext.isNullHandlingEnabled();
              RoaringBitmap[] nullBitmaps = null;
              if (nullHandlingEnabled) {
                nullBitmaps = new RoaringBitmap[_numColumns];
                for (int i = 0; i < _numColumns; i++) {
                  nullBitmaps[i] = dataTable.getNullRowIds(i);
                }
              }

              int numRows = dataTable.getNumberOfRows();
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
                    case BIG_DECIMAL:
                      values[colId] = dataTable.getBigDecimal(rowId, colId);
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
                if (nullHandlingEnabled) {
                  for (int colId = 0; colId < _numColumns; colId++) {
                    if (nullBitmaps[colId] != null && nullBitmaps[colId].contains(rowId)) {
                      values[colId] = null;
                    }
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
      if (!countDownLatch.await(timeOutMs, TimeUnit.MILLISECONDS)) {
        throw new TimeoutException("Timed out in broker reduce phase");
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted in broker reduce phase", e);
    } finally {
      for (Future future : futures) {
        if (!future.isDone()) {
          future.cancel(true);
        }
      }
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
      return 1;
    } else {
      return Math.min(numDataTables, maxReduceThreadsPerQuery);
    }
  }
}
