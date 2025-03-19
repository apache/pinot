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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.sql.Timestamp;
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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.datatable.DataTable;
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
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.utils.rewriter.ResultRewriteUtils;
import org.apache.pinot.core.query.utils.rewriter.RewriterResult;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.RoaringBitmap;


/**
 * Helper class to reduce data tables and set group by results into the BrokerResponseNative
 * Used for key-less aggregations, e.g. select max(id), sum(quantity) from orders .
 */
@SuppressWarnings("rawtypes")
public class GroupByDataTableReducer implements DataTableReducer {
  private static final int MIN_DATA_TABLES_FOR_CONCURRENT_REDUCE = 2; // TBD, find a better value.

  private final QueryContext _queryContext;
  private final AggregationFunction[] _aggregationFunctions;
  private final int _numAggregationFunctions;
  private final int _numGroupByExpressions;
  private final int _numColumns;

  public GroupByDataTableReducer(QueryContext queryContext) {
    _queryContext = queryContext;
    _aggregationFunctions = queryContext.getAggregationFunctions();
    assert _aggregationFunctions != null;
    _numAggregationFunctions = _aggregationFunctions.length;
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    assert groupByExpressions != null;
    _numGroupByExpressions = groupByExpressions.size();
    _numColumns = _numAggregationFunctions + _numGroupByExpressions;
  }

  /**
   * Reduces and sets group by results into ResultTable.
   */
  @Override
  public void reduceAndSetResults(String tableName, DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap, BrokerResponseNative brokerResponse,
      DataTableReducerContext reducerContext, BrokerMetrics brokerMetrics) {
    dataSchema = ReducerDataSchemaUtils.canonicalizeDataSchemaForGroupBy(_queryContext, dataSchema);

    if (dataTableMap.isEmpty()) {
      PostAggregationHandler postAggregationHandler =
          new PostAggregationHandler(_queryContext, getPrePostAggregationDataSchema(dataSchema));
      DataSchema resultDataSchema = postAggregationHandler.getResultDataSchema();
      RewriterResult rewriterResult = ResultRewriteUtils.rewriteResult(resultDataSchema, Collections.emptyList());
      brokerResponse.setResultTable(new ResultTable(rewriterResult.getDataSchema(), rewriterResult.getRows()));
      return;
    }

    Collection<DataTable> dataTables = dataTableMap.values();
    // NOTE: Use regular reduce when group keys are not partitioned even if there are only one data table because the
    //       records are not sorted yet.
    if (_queryContext.isServerReturnFinalResult() && dataTables.size() == 1) {
      processSingleFinalResult(dataSchema, dataTables.iterator().next(), brokerResponse);
    } else {
      try {
        reduceResult(brokerResponse, dataSchema, dataTables, reducerContext, tableName, brokerMetrics);
      } catch (TimeoutException e) {
        brokerResponse.getExceptions()
            .add(new QueryProcessingException(QueryErrorCode.BROKER_TIMEOUT, e.getMessage()));
      }
    }

    if (brokerMetrics != null && brokerResponse.getResultTable() != null) {
      brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.GROUP_BY_SIZE,
          brokerResponse.getResultTable().getRows().size());
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
  private void reduceResult(BrokerResponseNative brokerResponseNative, DataSchema dataSchema,
      Collection<DataTable> dataTables, DataTableReducerContext reducerContext, String rawTableName,
      BrokerMetrics brokerMetrics)
      throws TimeoutException {
    // NOTE: This step will modify the data schema and also return final aggregate results.
    IndexedTable indexedTable = getIndexedTable(dataSchema, dataTables, reducerContext);
    if (brokerMetrics != null) {
      brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.NUM_RESIZES, indexedTable.getNumResizes());
      brokerMetrics.addValueToTableGauge(rawTableName, BrokerGauge.RESIZE_TIME_MS, indexedTable.getResizeTimeMs());
    }
    int numRecords = indexedTable.size();
    Iterator<Record> sortedIterator = indexedTable.iterator();

    PostAggregationHandler postAggregationHandler = new PostAggregationHandler(_queryContext, dataSchema);
    DataSchema resultDataSchema = postAggregationHandler.getResultDataSchema();

    // Directly return when there is no record returned, or limit is 0
    int limit = _queryContext.getLimit();
    if (numRecords == 0 || limit == 0) {
      brokerResponseNative.setResultTable(new ResultTable(resultDataSchema, Collections.emptyList()));
      return;
    }

    // Calculate rows before post-aggregation
    List<Object[]> rows;
    ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
    int numColumns = columnDataTypes.length;
    FilterContext havingFilter = _queryContext.getHavingFilter();
    if (havingFilter != null) {
      rows = new ArrayList<>();
      HavingFilterHandler havingFilterHandler =
          new HavingFilterHandler(havingFilter, postAggregationHandler, _queryContext.isNullHandlingEnabled());
      int processedRows = 0;
      while (rows.size() < limit && sortedIterator.hasNext()) {
        Object[] row = sortedIterator.next().getValues();
        for (int i = 0; i < numColumns; i++) {
          Object value = row[i];
          if (value != null) {
            row[i] = columnDataTypes[i].convert(row[i]);
          }
        }
        if (havingFilterHandler.isMatch(row)) {
          rows.add(row);
        }
        Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(processedRows);
        processedRows++;
      }
    } else {
      int numRows = Math.min(numRecords, limit);
      rows = new ArrayList<>(numRows);
      for (int i = 0; i < numRows; i++) {
        Object[] row = sortedIterator.next().getValues();
        for (int j = 0; j < numColumns; j++) {
          Object value = row[j];
          if (value != null) {
            row[j] = columnDataTypes[j].convert(row[j]);
          }
        }
        rows.add(row);
        Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(i);
      }
    }

    // Calculate final result rows after post aggregation
    List<Object[]> resultRows = calculateFinalResultRows(postAggregationHandler, rows);

    // Rewrite and set result table
    RewriterResult rewriterResult = ResultRewriteUtils.rewriteResult(resultDataSchema, resultRows);
    brokerResponseNative.setResultTable(new ResultTable(rewriterResult.getDataSchema(), rewriterResult.getRows()));
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

    assert !dataTablesToReduce.isEmpty();
    ArrayList<DataTable> dataTables = new ArrayList<>(dataTablesToReduce);
    int numDataTables = dataTables.size();

    // Get the number of threads to use for reducing.
    int numReduceThreadsToUse = getNumReduceThreadsToUse(numDataTables, reducerContext.getMaxReduceThreadsPerQuery());

    // Create an indexed table to perform the reduce.
    IndexedTable indexedTable =
        GroupByUtils.createIndexedTableForDataTableReducer(dataTables.get(0), _queryContext, reducerContext,
            numReduceThreadsToUse, reducerContext.getExecutorService());

    // Create groups of data tables that each thread can process concurrently.
    // Given that numReduceThreads is <= numDataTables, each group will have at least one data table.
    List<List<DataTable>> reduceGroups = new ArrayList<>(numReduceThreadsToUse);
    for (int i = 0; i < numReduceThreadsToUse; i++) {
      reduceGroups.add(new ArrayList<>());
    }
    for (int i = 0; i < numDataTables; i++) {
      reduceGroups.get(i % numReduceThreadsToUse).add(dataTables.get(i));
    }

    Future[] futures = new Future[numReduceThreadsToUse];
    CountDownLatch countDownLatch = new CountDownLatch(numReduceThreadsToUse);
    AtomicReference<Throwable> exception = new AtomicReference<>();
    ColumnDataType[] storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();
    for (int i = 0; i < numReduceThreadsToUse; i++) {
      List<DataTable> reduceGroup = reduceGroups.get(i);
      int taskId = i;
      ThreadExecutionContext parentContext = Tracing.getThreadAccountant().getThreadExecutionContext();
      futures[i] = reducerContext.getExecutorService().submit(new TraceRunnable() {
        @Override
        public void runJob() {
          Tracing.ThreadAccountantOps.setupWorker(taskId, parentContext);
          try {
            for (DataTable dataTable : reduceGroup) {
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
                // Terminate when thread is interrupted.
                // This is expected when the query already fails in the main thread.
                // The first check will always be performed when rowId = 0
                Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rowId);
                Object[] values = new Object[_numColumns];
                for (int colId = 0; colId < _numColumns; colId++) {
                  // NOTE: We need to handle data types for group key, intermediate and final aggregate result.
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
                    case INT_ARRAY:
                      values[colId] = IntArrayList.wrap(dataTable.getIntArray(rowId, colId));
                      break;
                    case LONG_ARRAY:
                      values[colId] = LongArrayList.wrap(dataTable.getLongArray(rowId, colId));
                      break;
                    case FLOAT_ARRAY:
                      values[colId] = FloatArrayList.wrap(dataTable.getFloatArray(rowId, colId));
                      break;
                    case DOUBLE_ARRAY:
                      values[colId] = DoubleArrayList.wrap(dataTable.getDoubleArray(rowId, colId));
                      break;
                    case STRING_ARRAY:
                      values[colId] = ObjectArrayList.wrap(dataTable.getStringArray(rowId, colId));
                      break;
                    case OBJECT:
                      CustomObject customObject = dataTable.getCustomObject(rowId, colId);
                      if (customObject != null) {
                        assert _aggregationFunctions != null;
                        values[colId] =
                            _aggregationFunctions[colId - _numGroupByExpressions].deserializeIntermediateResult(
                                customObject);
                      }
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
            }
          } catch (Throwable t) {
            exception.compareAndSet(null, t);
          } finally {
            countDownLatch.countDown();
            Tracing.ThreadAccountantOps.clear();
          }
        }
      });
    }

    try {
      long timeOutMs = reducerContext.getReduceTimeOutMs() - (System.currentTimeMillis() - start);
      if (!countDownLatch.await(timeOutMs, TimeUnit.MILLISECONDS)) {
        throw new TimeoutException("Timed out in broker reduce phase");
      }
      Throwable t = exception.get();
      if (t != null) {
        Utils.rethrowException(t);
      }
    } catch (InterruptedException e) {
      Exception killedErrorMsg = Tracing.getThreadAccountant().getErrorStatus();
      throw new EarlyTerminationException(
          "Interrupted in broker reduce phase" + (killedErrorMsg == null ? StringUtils.EMPTY : " " + killedErrorMsg),
          e);
    } finally {
      for (Future future : futures) {
        if (!future.isDone()) {
          future.cancel(true);
        }
      }
    }

    indexedTable.finish(true, true);
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

  private void processSingleFinalResult(DataSchema dataSchema, DataTable dataTable,
      BrokerResponseNative brokerResponseNative) {
    PostAggregationHandler postAggregationHandler = new PostAggregationHandler(_queryContext, dataSchema);
    DataSchema resultDataSchema = postAggregationHandler.getResultDataSchema();

    // Directly return when there is no record returned, or limit is 0
    int numRows = dataTable.getNumberOfRows();
    int limit = _queryContext.getLimit();
    if (numRows == 0 || limit == 0) {
      brokerResponseNative.setResultTable(new ResultTable(resultDataSchema, Collections.emptyList()));
      return;
    }

    // Calculate rows before post-aggregation
    List<Object[]> rows;
    FilterContext havingFilter = _queryContext.getHavingFilter();
    if (havingFilter != null) {
      rows = new ArrayList<>();
      HavingFilterHandler havingFilterHandler =
          new HavingFilterHandler(havingFilter, postAggregationHandler, _queryContext.isNullHandlingEnabled());
      for (int i = 0; i < numRows; i++) {
        Object[] row = getConvertedRowWithFinalResult(dataTable, i);
        if (havingFilterHandler.isMatch(row)) {
          rows.add(row);
          if (rows.size() == limit) {
            break;
          }
        }
      }
    } else {
      numRows = Math.min(numRows, limit);
      rows = new ArrayList<>(numRows);
      for (int i = 0; i < numRows; i++) {
        rows.add(getConvertedRowWithFinalResult(dataTable, i));
      }
    }

    // Calculate final result rows after post aggregation
    List<Object[]> resultRows = calculateFinalResultRows(postAggregationHandler, rows);

    // Rewrite and set result table
    RewriterResult rewriterResult = ResultRewriteUtils.rewriteResult(resultDataSchema, resultRows);
    brokerResponseNative.setResultTable(new ResultTable(rewriterResult.getDataSchema(), rewriterResult.getRows()));
  }

  private List<Object[]> calculateFinalResultRows(PostAggregationHandler postAggregationHandler, List<Object[]> rows) {
    List<Object[]> resultRows = new ArrayList<>(rows.size());
    ColumnDataType[] resultColumnDataTypes = postAggregationHandler.getResultDataSchema().getColumnDataTypes();
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
    return resultRows;
  }

  private Object[] getConvertedRowWithFinalResult(DataTable dataTable, int rowId) {
    Object[] row = new Object[_numColumns];
    ColumnDataType[] columnDataTypes = dataTable.getDataSchema().getColumnDataTypes();
    for (int i = 0; i < _numColumns; i++) {
      if (i < _numGroupByExpressions) {
        row[i] = getConvertedKey(dataTable, columnDataTypes[i], rowId, i);
      } else {
        row[i] = AggregationFunctionUtils.getConvertedFinalResult(dataTable, columnDataTypes[i], rowId, i);
      }
    }
    return row;
  }

  private Object getConvertedKey(DataTable dataTable, ColumnDataType columnDataType, int rowId, int colId) {
    switch (columnDataType) {
      case INT:
        return dataTable.getInt(rowId, colId);
      case LONG:
        return dataTable.getLong(rowId, colId);
      case FLOAT:
        return dataTable.getFloat(rowId, colId);
      case DOUBLE:
        return dataTable.getDouble(rowId, colId);
      case BIG_DECIMAL:
        return dataTable.getBigDecimal(rowId, colId);
      case BOOLEAN:
        return dataTable.getInt(rowId, colId) == 1;
      case TIMESTAMP:
        return new Timestamp(dataTable.getLong(rowId, colId));
      case STRING:
      case JSON:
        return dataTable.getString(rowId, colId);
      case BYTES:
        return dataTable.getBytes(rowId, colId).getBytes();
      default:
        throw new IllegalStateException("Illegal column data type in group key: " + columnDataType);
    }
  }
}
