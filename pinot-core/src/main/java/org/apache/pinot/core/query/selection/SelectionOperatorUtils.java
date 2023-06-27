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
package org.apache.pinot.core.query.selection;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


/**
 * The <code>SelectionOperatorUtils</code> class provides the utility methods for selection queries without
 * <code>ORDER BY</code> and {@link SelectionOperatorService}.
 * <p>Expected behavior:
 * <ul>
 *   <li>
 *     Return selection results with the same order of columns as user passed in.
 *     <ul>
 *       <li>Eg. SELECT colB, colA, colC FROM table -> [valB, valA, valC]</li>
 *     </ul>
 *   </li>
 *   <li>
 *     For 'SELECT *', return columns with alphabetically order.
 *     <ul>
 *       <li>Eg. SELECT * FROM table -> [valA, valB, valC]</li>
 *     </ul>
 *   </li>
 * </ul>
 */
public class SelectionOperatorUtils {
  private SelectionOperatorUtils() {
  }

  public static final ExpressionContext IDENTIFIER_STAR = ExpressionContext.forIdentifier("*");
  public static final int MAX_ROW_HOLDER_INITIAL_CAPACITY = 10_000;

  /**
   * Extracts the expressions from a selection query, expands {@code 'SELECT *'} to all physical columns if applies.
   * <p>Order-by expressions will be put at the front if exist. The expressions returned are deduplicated.
   * <p>NOTE: DO NOT change the order of the expressions returned because broker relies on that to process the query.
   */
  public static List<ExpressionContext> extractExpressions(QueryContext queryContext, IndexSegment indexSegment) {
    Set<ExpressionContext> expressionSet = new HashSet<>();
    List<ExpressionContext> expressions = new ArrayList<>();

    List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
    if (orderByExpressions != null && queryContext.getLimit() > 0) {
      // NOTE:
      //   1. Order-by expressions are ignored for queries with LIMIT 0.
      //   2. Order-by expressions are already deduped in QueryContext.
      for (OrderByExpressionContext orderByExpression : orderByExpressions) {
        ExpressionContext expression = orderByExpression.getExpression();
        expressionSet.add(expression);
        expressions.add(expression);
      }
    }

    List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
    if (selectExpressions.size() == 1 && selectExpressions.get(0).equals(IDENTIFIER_STAR)) {
      // For 'SELECT *', sort all columns (ignore columns that start with '$') so that the order is deterministic
      Set<String> allColumns = indexSegment.getColumnNames();
      List<String> selectColumns = new ArrayList<>(allColumns.size());
      for (String column : allColumns) {
        if (column.charAt(0) != '$') {
          selectColumns.add(column);
        }
      }
      selectColumns.sort(null);
      for (String column : selectColumns) {
        ExpressionContext expression = ExpressionContext.forIdentifier(column);
        if (!expressionSet.contains(expression)) {
          expressions.add(expression);
        }
      }
    } else {
      for (ExpressionContext selectExpression : selectExpressions) {
        if (expressionSet.add(selectExpression)) {
          expressions.add(selectExpression);
        }
      }
    }

    return expressions;
  }

  /**
   * Expands {@code 'SELECT *'} to all columns (excluding transform functions) within {@link DataSchema} with
   * alphabetical order if applies.
   */
  public static List<String> getSelectionColumns(QueryContext queryContext, DataSchema dataSchema) {
    List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
    int numSelectExpressions = selectExpressions.size();
    if (numSelectExpressions == 1 && selectExpressions.get(0).equals(IDENTIFIER_STAR)) {
      String[] columnNames = dataSchema.getColumnNames();
      int numColumns = columnNames.length;

      // NOTE: The data schema might be generated from DataTableBuilder.buildEmptyDataTable(), where for 'SELECT *' it
      //       contains a single column "*". In such case, return as is to build the empty selection result.
      if (numColumns == 1 && columnNames[0].equals("*")) {
        return new ArrayList<>(Collections.singletonList("*"));
      }

      // Directly return all columns for selection-only queries
      // NOTE: Order-by expressions are ignored for queries with LIMIT 0
      List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
      if (orderByExpressions == null || queryContext.getLimit() == 0) {
        return Arrays.asList(columnNames);
      }

      // Exclude transform functions from the returned columns and sort
      // NOTE: Do not parse the column because it might contain SQL reserved words
      List<String> allColumns = new ArrayList<>(numColumns);
      for (String column : columnNames) {
        if (column.indexOf('(') == -1) {
          allColumns.add(column);
        }
      }
      allColumns.sort(null);
      return allColumns;
    } else {
      List<String> columns = new ArrayList<>(numSelectExpressions);
      for (ExpressionContext selectExpression : selectExpressions) {
        columns.add(selectExpression.toString());
      }
      return columns;
    }
  }

  /**
   * Constructs the final selection DataSchema based on the order of selection columns (data schema can have a
   * different order, depending on order by clause)
   * @param dataSchema data schema used for execution and ordering
   * @param selectionColumns the selection order
   * @return data schema for final results
   */
  public static DataSchema getResultTableDataSchema(DataSchema dataSchema, List<String> selectionColumns) {
    Map<String, ColumnDataType> columnNameToDataType = new HashMap<>();
    String[] columnNames = dataSchema.getColumnNames();
    ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
    int numColumns = columnNames.length;
    for (int i = 0; i < numColumns; i++) {
      columnNameToDataType.put(columnNames[i], columnDataTypes[i]);
    }
    int numResultColumns = selectionColumns.size();
    ColumnDataType[] finalColumnDataTypes = new ColumnDataType[numResultColumns];
    for (int i = 0; i < numResultColumns; i++) {
      finalColumnDataTypes[i] = columnNameToDataType.get(selectionColumns.get(i));
    }
    return new DataSchema(selectionColumns.toArray(new String[0]), finalColumnDataTypes);
  }

  /**
   * Merge two partial results for selection queries without <code>ORDER BY</code>. (Server side)
   *
   * @param mergedBlock partial results 1.
   * @param blockToMerge partial results 2.
   * @param selectionSize size of the selection.
   */
  public static void mergeWithoutOrdering(SelectionResultsBlock mergedBlock, SelectionResultsBlock blockToMerge,
      int selectionSize) {
    List<Object[]> mergedRows = mergedBlock.getRows();
    List<Object[]> rowsToMerge = blockToMerge.getRows();
    int numRowsToMerge = Math.min(selectionSize - mergedRows.size(), rowsToMerge.size());
    if (numRowsToMerge > 0) {
      mergedRows.addAll(rowsToMerge.subList(0, numRowsToMerge));
    }
  }

  /**
   * Merge two partial results for selection queries with <code>ORDER BY</code>. (Server side)
   *
   * @param mergedBlock partial results 1 (sorted).
   * @param blockToMerge partial results 2 (sorted).
   * @param maxNumRows maximum number of rows need to be stored.
   */
  public static void mergeWithOrdering(SelectionResultsBlock mergedBlock, SelectionResultsBlock blockToMerge,
      int maxNumRows) {
    List<Object[]> sortedRows1 = mergedBlock.getRows();
    List<Object[]> sortedRows2 = blockToMerge.getRows();
    Comparator<? super Object[]> comparator = mergedBlock.getComparator();
    assert comparator != null;
    int numSortedRows1 = sortedRows1.size();
    int numSortedRows2 = sortedRows2.size();
    if (numSortedRows1 == 0) {
      mergedBlock.setRows(sortedRows2);
      return;
    }
    if (numSortedRows2 == 0 || (numSortedRows1 == maxNumRows
        && comparator.compare(sortedRows1.get(numSortedRows1 - 1), sortedRows2.get(0)) <= 0)) {
      return;
    }
    int numRowsToMerge = Math.min(numSortedRows1 + numSortedRows2, maxNumRows);
    List<Object[]> mergedRows = new ArrayList<>(numRowsToMerge);
    int i1 = 0;
    int i2 = 0;
    int numMergedRows = 0;
    while (i1 < numSortedRows1 && i2 < numSortedRows2 && numMergedRows < numRowsToMerge) {
      Object[] row1 = sortedRows1.get(i1);
      Object[] row2 = sortedRows2.get(i2);
      if (comparator.compare(row1, row2) <= 0) {
        mergedRows.add(row1);
        i1++;
      } else {
        mergedRows.add(row2);
        i2++;
      }
      Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(numMergedRows++);
    }
    if (numMergedRows < numRowsToMerge) {
      if (i1 < numSortedRows1) {
        assert i2 == numSortedRows2;
        mergedRows.addAll(sortedRows1.subList(i1, i1 + numRowsToMerge - numMergedRows));
      } else {
        assert i1 == numSortedRows1;
        mergedRows.addAll(sortedRows2.subList(i2, i2 + numRowsToMerge - numMergedRows));
      }
    }
    mergedBlock.setRows(mergedRows);
  }

  /**
   * Build a {@link DataTable} from a {@link Collection} of selection rows with {@link DataSchema}. (Server side)
   */
  public static DataTable getDataTableFromRows(Collection<Object[]> rows, DataSchema dataSchema,
      boolean nullHandlingEnabled)
      throws IOException {
    ColumnDataType[] storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();
    int numColumns = storedColumnDataTypes.length;

    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    RoaringBitmap[] nullBitmaps = null;
    if (nullHandlingEnabled) {
      nullBitmaps = new RoaringBitmap[numColumns];
      Object[] nullPlaceholders = new Object[numColumns];
      for (int colId = 0; colId < numColumns; colId++) {
        nullBitmaps[colId] = new RoaringBitmap();
        nullPlaceholders[colId] = storedColumnDataTypes[colId].getNullPlaceholder();
      }
      int rowId = 0;
      for (Object[] row : rows) {
        for (int i = 0; i < numColumns; i++) {
          Object columnValue = row[i];
          if (columnValue == null) {
            row[i] = nullPlaceholders[i];
            nullBitmaps[i].add(rowId);
          }
        }
        rowId++;
      }
    }

    for (Object[] row : rows) {
      dataTableBuilder.startRow();
      for (int i = 0; i < numColumns; i++) {
        Object columnValue = row[i];
        switch (storedColumnDataTypes[i]) {
          // Single-value column
          case INT:
            dataTableBuilder.setColumn(i, (int) columnValue);
            break;
          case LONG:
            dataTableBuilder.setColumn(i, (long) columnValue);
            break;
          case FLOAT:
            dataTableBuilder.setColumn(i, (float) columnValue);
            break;
          case DOUBLE:
            dataTableBuilder.setColumn(i, (double) columnValue);
            break;
          case BIG_DECIMAL:
            dataTableBuilder.setColumn(i, (BigDecimal) columnValue);
            break;
          case STRING:
            dataTableBuilder.setColumn(i, (String) columnValue);
            break;
          case BYTES:
            dataTableBuilder.setColumn(i, (ByteArray) columnValue);
            break;
          case UNKNOWN:
            dataTableBuilder.setColumn(i, (Object) null);
            break;

          // Multi-value column
          case INT_ARRAY:
            dataTableBuilder.setColumn(i, (int[]) columnValue);
            break;
          case LONG_ARRAY:
            dataTableBuilder.setColumn(i, (long[]) columnValue);
            break;
          case FLOAT_ARRAY:
            dataTableBuilder.setColumn(i, (float[]) columnValue);
            break;
          case DOUBLE_ARRAY:
            dataTableBuilder.setColumn(i, (double[]) columnValue);
            break;
          case STRING_ARRAY:
            dataTableBuilder.setColumn(i, (String[]) columnValue);
            break;

          default:
            throw new IllegalStateException(
                String.format("Unsupported data type: %s for column: %s", storedColumnDataTypes[i],
                    dataSchema.getColumnName(i)));
        }
      }
      dataTableBuilder.finishRow();
    }

    if (nullHandlingEnabled) {
      for (int colId = 0; colId < numColumns; colId++) {
        dataTableBuilder.setNullRowIds(nullBitmaps[colId]);
      }
    }
    return dataTableBuilder.build();
  }

  /**
   * Extract a selection row from {@link DataTable}. (Broker side)
   *
   * @param dataTable data table.
   * @param rowId row id.
   * @return selection row.
   */
  public static Object[] extractRowFromDataTable(DataTable dataTable, int rowId) {
    DataSchema dataSchema = dataTable.getDataSchema();
    ColumnDataType[] storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();
    int numColumns = storedColumnDataTypes.length;

    Object[] row = new Object[numColumns];
    for (int i = 0; i < numColumns; i++) {
      switch (storedColumnDataTypes[i]) {
        // Single-value column
        case INT:
          row[i] = dataTable.getInt(rowId, i);
          break;
        case LONG:
          row[i] = dataTable.getLong(rowId, i);
          break;
        case FLOAT:
          row[i] = dataTable.getFloat(rowId, i);
          break;
        case DOUBLE:
          row[i] = dataTable.getDouble(rowId, i);
          break;
        case BIG_DECIMAL:
          row[i] = dataTable.getBigDecimal(rowId, i);
          break;
        case STRING:
          row[i] = dataTable.getString(rowId, i);
          break;
        case BYTES:
          row[i] = dataTable.getBytes(rowId, i);
          break;
        case UNKNOWN:
          row[i] = null;
          break;

        // Multi-value column
        case INT_ARRAY:
          row[i] = dataTable.getIntArray(rowId, i);
          break;
        case LONG_ARRAY:
          row[i] = dataTable.getLongArray(rowId, i);
          break;
        case FLOAT_ARRAY:
          row[i] = dataTable.getFloatArray(rowId, i);
          break;
        case DOUBLE_ARRAY:
          row[i] = dataTable.getDoubleArray(rowId, i);
          break;
        case STRING_ARRAY:
          row[i] = dataTable.getStringArray(rowId, i);
          break;

        default:
          throw new IllegalStateException(
              String.format("Unsupported data type: %s for column: %s", storedColumnDataTypes[i],
                  dataSchema.getColumnName(i)));
      }
    }

    return row;
  }

  /**
   * Extract a selection row from {@link DataTable} with potential null values. (Broker side)
   *
   * @param dataTable data table.
   * @param rowId row id.
   * @return selection row.
   */
  public static Object[] extractRowFromDataTableWithNullHandling(DataTable dataTable, int rowId,
      RoaringBitmap[] nullBitmaps) {
    Object[] row = extractRowFromDataTable(dataTable, rowId);
    for (int colId = 0; colId < nullBitmaps.length; colId++) {
      if (nullBitmaps[colId] != null && nullBitmaps[colId].contains(rowId)) {
        row[colId] = null;
      }
    }
    return row;
  }

  /**
   * Reduces a collection of {@link DataTable}s to selection rows for selection queries without <code>ORDER BY</code>.
   * (Broker side)
   */
  public static List<Object[]> reduceWithoutOrdering(Collection<DataTable> dataTables, int limit,
      boolean nullHandlingEnabled) {
    List<Object[]> rows = new ArrayList<>(Math.min(limit, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY));
    for (DataTable dataTable : dataTables) {
      int numColumns = dataTable.getDataSchema().size();
      int numRows = dataTable.getNumberOfRows();
      if (nullHandlingEnabled) {
        RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
        for (int coldId = 0; coldId < numColumns; coldId++) {
          nullBitmaps[coldId] = dataTable.getNullRowIds(coldId);
        }
        for (int rowId = 0; rowId < numRows; rowId++) {
          if (rows.size() < limit) {
            rows.add(extractRowFromDataTableWithNullHandling(dataTable, rowId, nullBitmaps));
          } else {
            break;
          }
          Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rowId);
        }
      } else {
        for (int rowId = 0; rowId < numRows; rowId++) {
          if (rows.size() < limit) {
            rows.add(extractRowFromDataTable(dataTable, rowId));
          } else {
            break;
          }
          Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rowId);
        }
      }
    }
    return rows;
  }

  /**
   * Render the selection rows to a {@link ResultTable} object
   * for selection queries without <code>ORDER BY</code>
   * <p>{@link ResultTable} object will be used to set in the broker response.
   * <p>Should be called after method "reduceWithoutOrdering()".
   *
   * @param rows selection rows.
   * @param dataSchema data schema.
   * @param selectionColumns selection columns.
   * @return {@link ResultTable} object results.
   */
  public static ResultTable renderResultTableWithoutOrdering(List<Object[]> rows, DataSchema dataSchema,
      List<String> selectionColumns) {
    int numRows = rows.size();
    List<Object[]> resultRows = new ArrayList<>(numRows);

    DataSchema resultDataSchema = dataSchema;
    Map<String, Integer> columnNameToIndexMap = null;
    if (dataSchema.getColumnNames().length != selectionColumns.size()) {
      // Create updated data schema since one column can be selected multiple times.
      columnNameToIndexMap = new HashMap<>(dataSchema.getColumnNames().length);
      String[] columnNames = dataSchema.getColumnNames();
      ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
      for (int i = 0; i < columnNames.length; i++) {
        columnNameToIndexMap.put(columnNames[i], i);
      }

      ColumnDataType[] newColumnDataTypes = new ColumnDataType[selectionColumns.size()];
      for (int i = 0; i < newColumnDataTypes.length; i++) {
        int index = columnNameToIndexMap.get(selectionColumns.get(i));
        newColumnDataTypes[i] = columnDataTypes[index];
      }

      resultDataSchema = new DataSchema(selectionColumns.toArray(new String[0]), newColumnDataTypes);
    }

    int numColumns = resultDataSchema.getColumnNames().length;
    ColumnDataType[] resultColumnDataTypes = resultDataSchema.getColumnDataTypes();
    for (Object[] row : rows) {
      Object[] resultRow = new Object[numColumns];
      for (int i = 0; i < numColumns; i++) {
        int index = (columnNameToIndexMap != null) ? columnNameToIndexMap.get(selectionColumns.get(i)) : i;
        Object value = row[index];
        if (value != null) {
          resultRow[i] = resultColumnDataTypes[i].convertAndFormat(value);
        }
      }
      resultRows.add(resultRow);
    }

    return new ResultTable(resultDataSchema, resultRows);
  }

  /**
   * Helper method to compute column indices from selection columns and the data schema for selection queries
   * @param selectionColumns selection columns.
   * @param dataSchema data schema.
   * @return column indices
   */
  public static int[] getColumnIndices(List<String> selectionColumns, DataSchema dataSchema) {
    String[] columnNames = dataSchema.getColumnNames();
    Map<String, Integer> columnToIndexMap = getColumnToIndexMap(columnNames);
    int numSelectionColumns = selectionColumns.size();
    int[] columnIndices = new int[numSelectionColumns];
    for (int i = 0; i < numSelectionColumns; i++) {
      columnIndices[i] = columnToIndexMap.get(selectionColumns.get(i));
    }
    return columnIndices;
  }

  public static Map<String, Integer> getColumnToIndexMap(String[] columns) {
    Map<String, Integer> columnToIndexMap = new HashMap<>();
    int numColumns = columns.length;
    for (int i = 0; i < numColumns; i++) {
      columnToIndexMap.put(columns[i], i);
    }
    return columnToIndexMap;
  }

  /**
   * Helper method to add a value to a {@link PriorityQueue}.
   *
   * @param value value to be added.
   * @param queue priority queue.
   * @param maxNumValues maximum number of values in the priority queue.
   * @param <T> type for the value.
   */
  public static <T> void addToPriorityQueue(T value, PriorityQueue<T> queue, int maxNumValues) {
    if (queue.size() < maxNumValues) {
      queue.add(value);
    } else if (queue.comparator().compare(queue.peek(), value) < 0) {
      queue.poll();
      queue.offer(value);
    }
  }

  public static DataSchema getSchemaForProjection(DataSchema dataSchema, int[] columnIndices) {
    int numColumns = columnIndices.length;

    String[] columnNames = dataSchema.getColumnNames();
    ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
    String[] resultColumnNames = new String[numColumns];
    ColumnDataType[] resultColumnDataTypes = new ColumnDataType[numColumns];
    for (int i = 0; i < numColumns; i++) {
      int columnIndex = columnIndices[i];
      resultColumnNames[i] = columnNames[columnIndex];
      resultColumnDataTypes[i] = columnDataTypes[columnIndex];
    }
    return new DataSchema(resultColumnNames, resultColumnDataTypes);
  }
}
