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

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryErrorMessage;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;


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
   * Returns the data schema and column indices of the final selection results based on the query and the data schema of
   * the server response. See {@link #extractExpressions} for the column orders on the server side.
   * NOTE: DO NOT rely on column name lookup across query context and data schema because the string representation of
   *       expression can change, which will cause backward incompatibility.
   */
  public static Pair<DataSchema, int[]> getResultTableDataSchemaAndColumnIndices(QueryContext queryContext,
      DataSchema dataSchema) {
    List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
    int numSelectExpressions = selectExpressions.size();
    ColumnDataType[] columnDataTypesInDataSchema = dataSchema.getColumnDataTypes();
    int numColumnsInDataSchema = columnDataTypesInDataSchema.length;

    // No order-by expression
    // NOTE: Order-by expressions are ignored for queries with LIMIT 0.
    boolean isSelectStar = queryContext.getQueryOptions() != null
        && QueryOptionsUtils.isSelectStarQuery(queryContext.getQueryOptions());
    List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
    if (orderByExpressions == null || queryContext.getLimit() == 0) {
      // For 'SELECT *', use the server response data schema as the final results data schema.
      if ((numSelectExpressions == 1 && selectExpressions.get(0).equals(IDENTIFIER_STAR) || isSelectStar)) {
        int[] columnIndices = new int[numColumnsInDataSchema];
        for (int i = 0; i < numColumnsInDataSchema; i++) {
          columnIndices[i] = i;
        }
        return Pair.of(dataSchema, columnIndices);
      }

      // For select without duplicate columns, the order of the final selection columns is the same as the order of the
      // columns in the data schema.
      if (numSelectExpressions == numColumnsInDataSchema) {
        String[] columnNames = new String[numSelectExpressions];
        int[] columnIndices = new int[numSelectExpressions];
        for (int i = 0; i < numSelectExpressions; i++) {
          columnNames[i] = selectExpressions.get(i).toString();
          columnIndices[i] = i;
        }
        return Pair.of(new DataSchema(columnNames, columnDataTypesInDataSchema), columnIndices);
      }

      // For select with duplicate columns, construct a map from expression to index with the same order as the data
      // schema, then look up the selection expressions.
      Object2IntOpenHashMap<ExpressionContext> expressionIndexMap = new Object2IntOpenHashMap<>(numColumnsInDataSchema);
      for (ExpressionContext selectExpression : selectExpressions) {
        expressionIndexMap.putIfAbsent(selectExpression, expressionIndexMap.size());
      }
      Preconditions.checkState(expressionIndexMap.size() == numColumnsInDataSchema,
          "BUG: Expect same number of deduped columns in SELECT clause and in data schema, got %s before dedup and %s"
              + " after dedup in SELECT clause, %s in data schema", numSelectExpressions, expressionIndexMap.size(),
          numColumnsInDataSchema);
      String[] columnNames = new String[numSelectExpressions];
      ColumnDataType[] columnDataTypes = new ColumnDataType[numSelectExpressions];
      int[] columnIndices = new int[numSelectExpressions];
      for (int i = 0; i < numSelectExpressions; i++) {
        ExpressionContext selectExpression = selectExpressions.get(i);
        int columnIndex = expressionIndexMap.getInt(selectExpression);
        columnNames[i] = selectExpression.toString();
        columnDataTypes[i] = columnDataTypesInDataSchema[columnIndex];
        columnIndices[i] = columnIndex;
      }
      return Pair.of(new DataSchema(columnNames, columnDataTypes), columnIndices);
    }

    // For 'SELECT *' with order-by, exclude transform functions from the returned columns and sort.
    if (numSelectExpressions == 1 && selectExpressions.get(0).equals(IDENTIFIER_STAR) || isSelectStar) {
      String[] columnNamesInDataSchema = dataSchema.getColumnNames();
      List<Integer> columnIndexList = new ArrayList<>(columnNamesInDataSchema.length);
      for (int i = 0; i < columnNamesInDataSchema.length; i++) {
        if (columnNamesInDataSchema[i].indexOf('(') == -1) {
          columnIndexList.add(i);
        }
      }
      columnIndexList.sort(Comparator.comparing(o -> columnNamesInDataSchema[o]));
      int numColumns = columnIndexList.size();
      String[] columnNames = new String[numColumns];
      ColumnDataType[] columnDataTypes = new ColumnDataType[numColumns];
      int[] columnIndices = new int[numColumns];
      for (int i = 0; i < numColumns; i++) {
        int columnIndex = columnIndexList.get(i);
        columnNames[i] = columnNamesInDataSchema[columnIndex];
        columnDataTypes[i] = columnDataTypesInDataSchema[columnIndex];
        columnIndices[i] = columnIndex;
      }
      return Pair.of(new DataSchema(columnNames, columnDataTypes), columnIndices);
    }

    // For other order-by queries, construct a map from expression to index with the same order as the data schema,
    // then look up the selection expressions.
    Object2IntOpenHashMap<ExpressionContext> expressionIndexMap = new Object2IntOpenHashMap<>(numColumnsInDataSchema);
    // NOTE: Order-by expressions are already deduped in QueryContext.
    for (OrderByExpressionContext orderByExpression : orderByExpressions) {
      expressionIndexMap.put(orderByExpression.getExpression(), expressionIndexMap.size());
    }
    for (ExpressionContext selectExpression : selectExpressions) {
      expressionIndexMap.putIfAbsent(selectExpression, expressionIndexMap.size());
    }
    String[] columnNames = new String[numSelectExpressions];
    ColumnDataType[] columnDataTypes = new ColumnDataType[numSelectExpressions];
    int[] columnIndices = new int[numSelectExpressions];
    if (expressionIndexMap.size() == numColumnsInDataSchema) {
      for (int i = 0; i < numSelectExpressions; i++) {
        ExpressionContext selectExpression = selectExpressions.get(i);
        int columnIndex = expressionIndexMap.getInt(selectExpression);
        columnNames[i] = selectExpression.toString();
        columnDataTypes[i] = columnDataTypesInDataSchema[columnIndex];
        columnIndices[i] = columnIndex;
      }
    } else {
      // When all segments are pruned on the server side, the data schema will only contain the columns in the SELECT
      // clause, and data type for all columns are set to STRING. See ResultBlocksUtils for details.
      for (int i = 0; i < numSelectExpressions; i++) {
        columnNames[i] = selectExpressions.get(i).toString();
        columnDataTypes[i] = ColumnDataType.STRING;
        columnIndices[i] = i;
      }
    }
    return Pair.of(new DataSchema(columnNames, columnDataTypes), columnIndices);
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

    List<Object[]> mergedRows = mergeSortedRowLists(sortedRows1, sortedRows2, comparator, maxNumRows);
    mergedBlock.setRows(mergedRows);
  }

  public static List<Object[]> mergeSortedRowLists(List<Object[]> list1, List<Object[]> list2,
                                                   Comparator<? super Object[]> comparator, int maxNumRows) {
    int n1 = list1.size();
    int n2 = list2.size();
    List<Object[]> merged = new ArrayList<>(Math.min(maxNumRows, n1 + n2));
    int i1 = 0;
    int i2 = 0;
    int mergedCount = 0;

    while (i1 < n1 && i2 < n2 && mergedCount < maxNumRows) {
      if (comparator.compare(list1.get(i1), list2.get(i2)) <= 0) {
        merged.add(list1.get(i1++));
      } else {
        merged.add(list2.get(i2++));
      }
      Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(mergedCount++);
    }

    while (i1 < n1 && mergedCount < maxNumRows) {
      merged.add(list1.get(i1++));
      mergedCount++;
    }

    while (i2 < n2 && mergedCount < maxNumRows) {
      merged.add(list2.get(i2++));
      mergedCount++;
    }

    return merged;
  }

  /**
   * Build a {@link DataTable} from a {@link Collection} of selection rows with {@link DataSchema}. (Server side)
   *
   * This method is allowed to modify the given rows. Specifically, it may remove nulls cells from it.
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

    int numRowsAdded = 0;
    for (Object[] row : rows) {
      Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(numRowsAdded);
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
          case MAP:
            dataTableBuilder.setColumn(i, (Map) columnValue);
            break;
          case UNKNOWN:
            dataTableBuilder.setNull(i);
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
            throw new IllegalStateException("Unsupported data type: " + storedColumnDataTypes + " for column: "
                + dataSchema.getColumnName(i));
        }
      }
      dataTableBuilder.finishRow();
      numRowsAdded++;
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
        case MAP:
          row[i] = dataTable.getMap(rowId, i);
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
          throw new IllegalStateException("Unsupported data type: " + storedColumnDataTypes[i] + " for column: "
              + dataSchema.getColumnName(i));
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
      boolean nullHandlingEnabled, boolean isSelectStar, boolean hasSchemaMismatch) {
    List<String> reduceSelectedColumns = null;
    if (isSelectStar && hasSchemaMismatch) {
      reduceSelectedColumns = getReducedColumns(dataTables);
    }
    return reduceResults(dataTables, limit, nullHandlingEnabled, reduceSelectedColumns, null);
  }

  /**
   * Renders the selection rows to a {@link ResultTable} object for selection queries without <code>ORDER BY</code>.
   * (Broker side)
   */
  public static ResultTable renderResultTableWithoutOrdering(List<Object[]> rows, DataSchema dataSchema,
      int[] columnIndices) {
    int numRows = rows.size();
    List<Object[]> resultRows = new ArrayList<>(numRows);
    ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
    int numColumns = columnDataTypes.length;
    for (Object[] row : rows) {
      Object[] resultRow = new Object[numColumns];
      for (int i = 0; i < numColumns; i++) {
        Object value = row[columnIndices[i]];
        if (value != null) {
          resultRow[i] = columnDataTypes[i].convertAndFormat(value);
        }
      }
      resultRows.add(resultRow);
    }
    return new ResultTable(dataSchema, resultRows);
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

  /**
   * Reduces a collection of {@link DataTable}s to selection rows for selection queries
   * with/without <code>ORDER BY</code>.
   * @param dataTables collection of data tables.
   * @param limit maximum number of rows to be returned.
   * @param nullHandlingEnabled whether null handling is enabled.
   * @param reduceSelectedColumns if specified, only the columns in this list be used for reducing the results.
   * @param comparator comparator to be used for sorting the rows.
   * @return list of selection rows.
   */
  public static List<Object[]> reduceResults(Collection<DataTable> dataTables, int limit,
                                              boolean nullHandlingEnabled, List<String> reduceSelectedColumns,
                                              Comparator<? super Object[]> comparator) {
    List<Object[]> reducedRows = new ArrayList<>(Math.min(limit, MAX_ROW_HOLDER_INITIAL_CAPACITY));
    if (dataTables.isEmpty()) {
      return reducedRows;
    }

    for (DataTable dataTable : dataTables) {
      List<Object[]> newRows = new ArrayList<>();
      DataSchema dataSchema = dataTable.getDataSchema();
      int numRows = dataTable.getNumberOfRows();

      if (nullHandlingEnabled) {
        int numColumns = dataSchema.size();
        RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
        for (int colId = 0; colId < numColumns; colId++) {
          nullBitmaps[colId] = dataTable.getNullRowIds(colId);
        }
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object[] fullRow = extractRowFromDataTableWithNullHandling(dataTable, rowId, nullBitmaps);
          Object[] projectedRow = projectRow(fullRow, reduceSelectedColumns, dataSchema);
          newRows.add(projectedRow);
          Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rowId);
        }
      } else {
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object[] fullRow = extractRowFromDataTable(dataTable, rowId);
          Object[] projectedRow = projectRow(fullRow, reduceSelectedColumns, dataSchema);
          newRows.add(projectedRow);
          Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rowId);
        }
      }

      if (comparator == null) {
        int spaceLeft = limit - reducedRows.size();
        int rowsToAdd = Math.min(spaceLeft, newRows.size());
        reducedRows.addAll(newRows.subList(0, rowsToAdd));
        if (reducedRows.size() >= limit) {
          break;
        }
      } else {
        reducedRows = mergeSortedRowLists(reducedRows, newRows, comparator, limit);
      }
    }
    return reducedRows;
  }

  /**
   * Returns a new DataSchema containing only the common columns between two schemas.
   * The order of columns is preserved from the first schema (schema1).
   */
  public static DataSchema getCommonDataSchema(DataSchema schema1, DataSchema schema2) {
    Map<String, ColumnDataType> schema2Columns = new HashMap<>();
    for (int i = 0; i < schema2.size(); i++) {
      schema2Columns.put(schema2.getColumnName(i), schema2.getColumnDataType(i));
    }

    String[] commonColumnNames = new String[schema1.size()];
    ColumnDataType[] commonColumnDataTypes = new ColumnDataType[schema1.size()];
    int index = 0;

    for (int i = 0; i < schema1.size(); i++) {
      String columnName = schema1.getColumnName(i);
      ColumnDataType columnDataType = schema1.getColumnDataType(i);
      if (schema2Columns.containsKey(columnName)) {
        commonColumnNames[index] = columnName;
        commonColumnDataTypes[index] = columnDataType;
        index++;
      }
    }
    // Trim arrays to the actual size
    commonColumnNames = Arrays.copyOf(commonColumnNames, index);
    commonColumnDataTypes = Arrays.copyOf(commonColumnDataTypes, index);
    return new DataSchema(commonColumnNames, commonColumnDataTypes);
  }

  public static void handleSchemeMisMatchOnMerge(SelectionResultsBlock mergedBlock, SelectionResultsBlock blockToMerge,
                                                 int limit, boolean toSort, Logger logger) {
    QueryContext mergedQueryContext = mergedBlock.getQueryContext();
    DataSchema mergedDataSchema = mergedBlock.getDataSchema();
    DataSchema dataSchemaToMerge = blockToMerge.getDataSchema();
    if (mergedQueryContext != null && mergedQueryContext.getQueryOptions() != null
            && QueryOptionsUtils.isSelectStarQuery(mergedQueryContext.getQueryOptions())) {
      Map<String, Integer> mergedBlockIndexMap = mergedDataSchema.getColumnNameToIndexMap();
      Map<String, Integer> blockToMergeIndexMap = dataSchemaToMerge.getColumnNameToIndexMap();
      DataSchema commonDataSchema = getCommonDataSchema(mergedDataSchema, dataSchemaToMerge);

      String[] commonColumns = commonDataSchema.getColumnNames();
      List<Integer> mergedIndices = new ArrayList<>();
      List<Integer> toMergeIndices = new ArrayList<>();
      for (String column : commonColumns) {
        mergedIndices.add(mergedBlockIndexMap.get(column));
        toMergeIndices.add(blockToMergeIndexMap.get(column));
      }

      mergeMatchingColumns(mergedBlock, blockToMerge, limit, mergedIndices, toMergeIndices, toSort);
      // Update the data schema of the merged block to only include common columns
      mergedBlock.setDataSchema(commonDataSchema);
    } else {
      String errorMessage = String.format("Data schemas do not match between merged block: %s and block to merge: %s",
              mergedDataSchema, dataSchemaToMerge);
      logger.debug(errorMessage);
      mergedBlock.addErrorMessage(QueryErrorMessage.safeMsg(QueryErrorCode.MERGE_RESPONSE, errorMessage));
    }
  }

  /**
   * Merges two selection results blocks with matching columns.
   * <p>Note: This method assumes that the blocks have already been sorted if required.
   *
   * @param mergedBlock the block to merge into
   * @param blockToMerge the block to merge from
   * @param selectionSize the maximum number of rows to keep in the merged block
   * @param mergedIndices the indices of the columns in the merged block
   * @param toMergeIndices the indices of the columns in the block to merge
   * @param shouldSort whether to sort the merged rows
   */
  public static void mergeMatchingColumns(SelectionResultsBlock mergedBlock, SelectionResultsBlock blockToMerge,
                                          int selectionSize, List<Integer> mergedIndices,
                                          List<Integer> toMergeIndices, boolean shouldSort) {
    List<Object[]> mergedRows = mergedBlock.getRows();
    List<Object[]> rowsToMerge = blockToMerge.getRows();

    List<Object[]> reprojectedMergedRows = new ArrayList<>(mergedRows.size());
    for (Object[] row : mergedRows) {
      reprojectedMergedRows.add(projectRow(row, mergedIndices));
    }
    // If we have already reached the limit, no need to merge more rows
    if (reprojectedMergedRows.size() >= selectionSize) {
      mergedBlock.setRows(reprojectedMergedRows);
      return;
    }

    // Only project rows to merge if we still need more rows
    List<Object[]> reprojectedRowsToMerge = new ArrayList<>();
    int numRowsMerged = reprojectedMergedRows.size();
    for (Object[] row : rowsToMerge) {
      if (numRowsMerged >= selectionSize) {
        break;
      }
      reprojectedRowsToMerge.add(projectRow(row, toMergeIndices));
      numRowsMerged++;
    }

    if (shouldSort) {
      List<Object[]> sortedMergedRows = mergeSortedRowLists(reprojectedMergedRows, reprojectedRowsToMerge,
              mergedBlock.getComparator(), selectionSize);
      mergedBlock.setRows(sortedMergedRows);
    } else {
      // Merge the two lists of rows
      reprojectedMergedRows.addAll(reprojectedRowsToMerge);
      mergedBlock.setRows(reprojectedMergedRows);
    }
  }

  /**
   * Projects a row to the specified columns using the column indices.
   */
  private static Object[] projectRow(Object[] row, List<Integer> indices) {
    Object[] projected = new Object[indices.size()];
    for (int i = 0; i < indices.size(); i++) {
      int idx = indices.get(i);
      if (idx >= row.length) {
        throw new IllegalStateException(String.format(
                "Invalid index %d for row of length %d. Row: %s", idx, row.length, Arrays.toString(row)));
      }
      projected[i] = row[idx];
    }
    return projected;
  }

  /**
   * Projects the full row to the specified columns using the column name to index map.
   */
  public static Object[] projectRow(Object[] fullRow, List<String> projectionColumns,
                                     DataSchema dataSchema) {
    if (projectionColumns == null) {
      return fullRow;
    }
    Map<String, Integer> columnNameToIndex = dataSchema.getColumnNameToIndexMap();
    Object[] projectedRow = new Object[projectionColumns.size()];
    for (int i = 0; i < projectionColumns.size(); i++) {
      projectedRow[i] = fullRow[columnNameToIndex.get(projectionColumns.get(i))];
    }
    return projectedRow;
  }

  public static List<String> getReducedColumns(Collection<DataTable> dataTables) {
    List<String> commonColumns = null;
    for (DataTable dataTable : dataTables) {
      List<String> columns = new ArrayList<>(List.of(dataTable.getDataSchema().getColumnNames()));
      if (commonColumns == null) {
        commonColumns = columns;
      } else {
        commonColumns.retainAll(columns);
      }
    }
    return commonColumns;
  }
}
