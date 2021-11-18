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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
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
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.SimpleIndexedTable;
import org.apache.pinot.core.data.table.UnboundedConcurrentIndexedTable;
import org.apache.pinot.core.operator.combine.GroupByOrderByCombineOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.core.util.trace.TraceCallable;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;


/**
 * Helper class to reduce data tables and set group by results into the BrokerResponseNative
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class GapFillGroupByDataTableReducer implements DataTableReducer {
  private static final int MIN_DATA_TABLES_FOR_CONCURRENT_REDUCE = 2; // TBD, find a better value.

  private final QueryContext _queryContext;
  private final AggregationFunction[] _aggregationFunctions;
  private final int _numAggregationFunctions;
  private final List<ExpressionContext> _groupByExpressions;
  private final int _numGroupByExpressions;
  private final int _numColumns;
  private final boolean _sqlQuery;
  private final DateTimeGranularitySpec _dateTimeGranularity;
  private final DateTimeFormatSpec _dateTimeFormatter;
  private final long _startMs;
  private final long _endMs;
  private final Set<Key> _primaryKeys;
  private final Map<Key, Object[]> _previous;
  private final int _numOfKeyColumns;

  GapFillGroupByDataTableReducer(QueryContext queryContext) {
    _queryContext = queryContext;
    _aggregationFunctions = queryContext.getAggregationFunctions();
    assert _aggregationFunctions != null;
    _numAggregationFunctions = _aggregationFunctions.length;
    _groupByExpressions = queryContext.getGroupByExpressions();
    assert _groupByExpressions != null;
    _numGroupByExpressions = _groupByExpressions.size();
    _numColumns = _numAggregationFunctions + _numGroupByExpressions;
    _sqlQuery = queryContext.getBrokerRequest().getPinotQuery() != null;

    ExpressionContext firstExpressionContext = _queryContext.getSelectExpressions().get(0);
    List<ExpressionContext> args = firstExpressionContext.getFunction().getArguments();
    _dateTimeFormatter = new DateTimeFormatSpec(args.get(1).getLiteral());
    _dateTimeGranularity = new DateTimeGranularitySpec(args.get(4).getLiteral());
    String start = args.get(2).getLiteral();
    String end = args.get(3).getLiteral();
    _startMs = truncate(_dateTimeFormatter.fromFormatToMillis(start));
    _endMs = truncate(_dateTimeFormatter.fromFormatToMillis(end));
    _primaryKeys = new HashSet<>();
    _previous = new HashMap<>();
    _numOfKeyColumns = _queryContext.getGroupByExpressions().size() - 1;
  }

  private long truncate(long epoch) {
    int sz = _dateTimeGranularity.getSize();
    return epoch / sz * sz;
  }

  /**
   * Reduces and sets group by results into ResultTable, if responseFormat = sql
   * By default, sets group by results into GroupByResults
   */
  @Override
  public void reduceAndSetResults(String tableName, DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap, BrokerResponseNative brokerResponseNative,
      DataTableReducerContext reducerContext, BrokerMetrics brokerMetrics) {
    assert dataSchema != null;
    Collection<DataTable> dataTables = dataTableMap.values();

    // 1. groupByMode = sql, responseFormat = sql
    // This is the primary SQL compliant group by

    try {
      setSQLGroupByInResultTable(brokerResponseNative, dataSchema, dataTables, reducerContext, tableName,
          brokerMetrics);
    } catch (TimeoutException e) {
      brokerResponseNative.getProcessingExceptions()
          .add(new QueryProcessingException(QueryException.BROKER_TIMEOUT_ERROR_CODE, e.getMessage()));
    }
    int resultSize = brokerResponseNative.getResultTable().getRows().size();

    if (brokerMetrics != null && resultSize > 0) {
      brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.GROUP_BY_SIZE, resultSize);
    }
  }

  private Key constructKey(Object[] row) {
    return new Key(Arrays.copyOfRange(row, 1, _numOfKeyColumns + 1));
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
  private void setSQLGroupByInResultTable(BrokerResponseNative brokerResponseNative, DataSchema dataSchema,
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
      List<Object[]> resultRows = new ArrayList<>(rows.size());
      for (Object[] row : rows) {
        Object[] resultRow = postAggregationHandler.getResult(row);
        for (int i = 0; i < resultColumnDataTypes.length; i++) {
          resultRow[i] = resultColumnDataTypes[i].format(resultRow[i]);
        }
        resultRows.add(resultRow);
        _primaryKeys.add(constructKey(resultRow));
      }
      List<Object[]> gapfillResultRows = gapFill(resultRows, resultColumnDataTypes);
      brokerResponseNative.setResultTable(new ResultTable(resultDataSchema, gapfillResultRows));
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

  List<Object[]> gapFill(List<Object[]> resultRows, ColumnDataType[] resultColumnDataTypes) {
    int limit = _queryContext.getLimit();
    int numResultColumns = resultColumnDataTypes.length;
    List<Object[]> gapfillResultRows = new ArrayList<>(limit);
    long step = _dateTimeGranularity.granularityToMillis();
    int index = 0;
    for (long time = _startMs; time + 2 * step <= _endMs; time += step) {
      Set<Key> keys = new HashSet<>(_primaryKeys);
      while (index < resultRows.size()) {
        long timeCol = _dateTimeFormatter.fromFormatToMillis(String.valueOf(resultRows.get(index)[0]));
        if (timeCol < time) {
          index++;
        } else if (timeCol == time) {
          gapfillResultRows.add(resultRows.get(index));
          if (gapfillResultRows.size() == limit) {
            return gapfillResultRows;
          }
          Key key = constructKey(resultRows.get(index));
          keys.remove(key);
          _previous.put(key, resultRows.get(index));
          index++;
        } else {
          break;
        }
      }
      for (Key key : keys) {
        Object[] gapfillRow = new Object[numResultColumns];
        if (resultColumnDataTypes[0] == ColumnDataType.LONG) {
          gapfillRow[0] = Long.valueOf(_dateTimeFormatter.fromMillisToFormat(time));
        } else {
          gapfillRow[0] = _dateTimeFormatter.fromMillisToFormat(time);
        }
        System.arraycopy(key.getValues(), 0, gapfillRow, 1, _numOfKeyColumns);

        for (int i = _numOfKeyColumns + 1; i < numResultColumns; i++) {
          gapfillRow[i] = getFillValue(i, key, resultColumnDataTypes[i]);
        }
        gapfillResultRows.add(gapfillRow);
        if (gapfillResultRows.size() == limit) {
          return gapfillResultRows;
        }
      }
    }
    return gapfillResultRows;
  }

  Object getFillValue(int columIndex, Object key, ColumnDataType dataType) {
    ExpressionContext expressionContext = _queryContext.getSelectExpressions().get(columIndex);
    if (expressionContext.getFunction() != null
        && expressionContext.getFunction().getFunctionName().equalsIgnoreCase("fill")) {
      List<ExpressionContext> args = expressionContext.getFunction().getArguments();
      if (args.get(1).getLiteral() == null) {
        throw new UnsupportedOperationException("Wrong Sql.");
      }
      FillType fillType = FillType.valueOf(args.get(1).getLiteral());
      if (fillType == FillType.FILL_DEFAULT_VALUE) {
        // TODO: may fill the default value from sql in the future.
        return getDefaultValue(dataType);
      } else if (fillType == FillType.FILL_PREVIOUS_VALUE) {
        Object[] row = _previous.get(key);
        if (row != null) {
          return row[columIndex];
        } else {
          return getDefaultValue(dataType);
        }
      } else {
        throw new UnsupportedOperationException("unsupported fill type.");
      }
    } else {
      return getDefaultValue(dataType);
    }
  }

  enum FillType {
    FILL_DEFAULT_VALUE,
    FILL_PREVIOUS_VALUE,
  }

  /**
   * The default value for each column type.
   */
  private Serializable getDefaultValue(ColumnDataType dataType) {
    switch (dataType) {
      // Single-value column
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case TIMESTAMP:
        return dataType.convertAndFormat(0);
      case STRING:
      case JSON:
      case BYTES:
        return "";
      case INT_ARRAY:
        return new int[0];
      case LONG_ARRAY:
        return new long[0];
      case FLOAT_ARRAY:
        return new float[0];
      case DOUBLE_ARRAY:
        return new double[0];
      case STRING_ARRAY:
      case TIMESTAMP_ARRAY:
        return new String[0];
      case BOOLEAN_ARRAY:
        return new boolean[0];
      case BYTES_ARRAY:
        return new byte[0][0];
      default:
        throw new IllegalStateException(String.format("Cannot provide the default value for the type: %s", dataType));
    }
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

    ColumnDataType[] storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();
    long timeOutMs = reducerContext.getReduceTimeOutMs() - (System.currentTimeMillis() - start);
    try {
      reducerContext.getExecutorService().invokeAll(reduceGroups.stream()
          .map(reduceGroup -> new TraceCallable<Void>() {
            @Override
            public Void callJob() throws Exception {
              for (DataTable dataTable : reduceGroup) {
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
            }
            return null;
          }
          }).collect(Collectors.toList()), timeOutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
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
}
