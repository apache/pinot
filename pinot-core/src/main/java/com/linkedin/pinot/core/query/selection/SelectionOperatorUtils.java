/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.selection;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.response.broker.SelectionResults;
import com.linkedin.pinot.common.utils.DataSchema;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.common.datatable.DataTableBuilder;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


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

  private static final DecimalFormat INT_FORMAT =
      new DecimalFormat("##########", DecimalFormatSymbols.getInstance(Locale.US));
  private static final DecimalFormat LONG_FORMAT =
      new DecimalFormat("####################", DecimalFormatSymbols.getInstance(Locale.US));
  private static final DecimalFormat FLOAT_FORMAT =
      new DecimalFormat("#########0.0####", DecimalFormatSymbols.getInstance(Locale.US));
  private static final DecimalFormat DOUBLE_FORMAT =
      new DecimalFormat("###################0.0#########", DecimalFormatSymbols.getInstance(Locale.US));

  /**
   * Expand <code>'SELECT *'</code> to select all columns with {@link IndexSegment}, order all columns alphabatically.
   * (Inner segment)
   *
   * @param selectionColumns unexpanded selection columns (may contain '*').
   * @param indexSegment index segment.
   * @return expanded selection columns.
   */
  @Nonnull
  public static List<String> getSelectionColumns(@Nonnull List<String> selectionColumns,
      @Nonnull IndexSegment indexSegment) {
    if (selectionColumns.size() == 1 && selectionColumns.get(0).equals("*")) {
      List<String> allColumns = Arrays.asList(indexSegment.getColumnNames());
      Collections.sort(allColumns);
      return allColumns;
    } else {
      return selectionColumns;
    }
  }

  /**
   * Extract all related columns for a selection query with {@link IndexSegment}. (Inner segment)
   *
   * @param selection selection query.
   * @param indexSegment index segment.
   * @return all related columns.
   */
  @Nonnull
  public static String[] extractSelectionRelatedColumns(@Nonnull Selection selection,
      @Nonnull IndexSegment indexSegment) {
    Set<String> selectionColumns = new HashSet<>(getSelectionColumns(selection.getSelectionColumns(), indexSegment));
    if (selection.getSelectionSortSequence() != null) {
      for (SelectionSort selectionSort : selection.getSelectionSortSequence()) {
        selectionColumns.add(selectionSort.getColumn());
      }
    }
    return selectionColumns.toArray(new String[selectionColumns.size()]);
  }

  /**
   * Extract the {@link DataSchema} from sort sequence, selection columns and {@link IndexSegment}. (Inner segment)
   * <p>Inside data schema, we just store each column once (de-duplicated).
   *
   * @param sortSequence sort sequence.
   * @param selectionColumns selection columns.
   * @param indexSegment index segment.
   * @return data schema.
   */
  @Nonnull
  public static DataSchema extractDataSchema(@Nullable List<SelectionSort> sortSequence,
      @Nonnull List<String> selectionColumns, @Nonnull IndexSegment indexSegment) {
    List<String> columnList = new ArrayList<>();
    Set<String> columnSet = new HashSet<>();

    if (sortSequence != null) {
      for (SelectionSort selectionSort : sortSequence) {
        String column = selectionSort.getColumn();
        columnList.add(column);
        columnSet.add(column);
      }
    }

    for (String column : selectionColumns) {
      if (!columnSet.contains(column)) {
        columnList.add(column);
        columnSet.add(column);
      }
    }

    int numColumns = columnList.size();
    String[] columns = new String[numColumns];
    DataType[] dataTypes = new DataType[numColumns];
    for (int i = 0; i < numColumns; i++) {
      String column = columnList.get(i);
      columns[i] = column;
      DataSourceMetadata columnMetadata = indexSegment.getDataSource(column).getDataSourceMetadata();
      if (columnMetadata.isSingleValue()) {
        dataTypes[i] = columnMetadata.getDataType();
      } else {
        dataTypes[i] = columnMetadata.getDataType().toMultiValue();
      }
    }

    return new DataSchema(columns, dataTypes);
  }

  /**
   * Expand <code>'SELECT *'</code> to select all columns with {@link DataSchema}, order all columns alphabatically.
   * (Inter segment)
   *
   * @param selectionColumns unexpanded selection columns (may contain '*').
   * @param dataSchema data schema.
   * @return expanded selection columns.
   */
  @Nonnull
  public static List<String> getSelectionColumns(@Nonnull List<String> selectionColumns,
      @Nonnull DataSchema dataSchema) {
    if ((selectionColumns.size() == 1) && selectionColumns.get(0).equals("*")) {
      int numColumns = dataSchema.size();
      List<String> allColumns = new ArrayList<>(numColumns);
      for (int i = 0; i < numColumns; i++) {
        allColumns.add(dataSchema.getColumnName(i));
      }
      Collections.sort(allColumns);
      return allColumns;
    } else {
      return selectionColumns;
    }
  }

  /**
   * Merge two partial results for selection queries without <code>ORDER BY</code>. (Server side)
   *
   * @param mergedRows partial results 1.
   * @param rowsToMerge partial results 2.
   * @param selectionSize size of the selection.
   */
  public static void mergeWithoutOrdering(@Nonnull Collection<Serializable[]> mergedRows,
      @Nonnull Collection<Serializable[]> rowsToMerge, int selectionSize) {
    Iterator<Serializable[]> iterator = rowsToMerge.iterator();
    while (mergedRows.size() < selectionSize && iterator.hasNext()) {
      mergedRows.add(iterator.next());
    }
  }

  /**
   * Merge two partial results for selection queries with <code>ORDER BY</code>. (Server side)
   *
   * @param mergedRows partial results 1.
   * @param rowsToMerge partial results 2.
   * @param maxNumRows maximum number of rows need to be stored.
   */
  public static void mergeWithOrdering(@Nonnull PriorityQueue<Serializable[]> mergedRows,
      @Nonnull Collection<Serializable[]> rowsToMerge, int maxNumRows) {
    for (Serializable[] row : rowsToMerge) {
      addToPriorityQueue(row, mergedRows, maxNumRows);
    }
  }

  /**
   * Build a {@link DataTable} from a {@link Collection} of selection rows with {@link DataSchema}. (Server side)
   * <p>The passed in data schema stored the column data type that can cover all actual data types for that column.
   * <p>The actual data types for each column in rows can be different but must be compatible with each other.
   * <p>Before write each row into the data table, first convert it to match the data types in data schema.
   *
   * @param rows {@link Collection} of selection rows.
   * @param dataSchema data schema.
   * @return data table.
   * @throws Exception
   */
  @Nonnull
  public static DataTable getDataTableFromRows(@Nonnull Collection<Serializable[]> rows, @Nonnull DataSchema dataSchema)
      throws Exception {
    int numColumns = dataSchema.size();

    DataTableBuilder dataTableBuilder = new DataTableBuilder(dataSchema);
    for (Serializable[] row : rows) {
      dataTableBuilder.startRow();
      for (int i = 0; i < numColumns; i++) {
        Serializable columnValue = row[i];
        DataType columnType = dataSchema.getColumnType(i);
        switch (columnType) {
          // Single-value column.
          case INT:
            dataTableBuilder.setColumn(i, ((Number) columnValue).intValue());
            break;
          case LONG:
            dataTableBuilder.setColumn(i, ((Number) columnValue).longValue());
            break;
          case FLOAT:
            dataTableBuilder.setColumn(i, ((Number) columnValue).floatValue());
            break;
          case DOUBLE:
            dataTableBuilder.setColumn(i, ((Number) columnValue).doubleValue());
            break;
          case STRING:
            dataTableBuilder.setColumn(i, ((String) columnValue));
            break;

          // Multi-value column.
          case INT_ARRAY:
            dataTableBuilder.setColumn(i, (int[]) columnValue);
            break;
          case LONG_ARRAY:
            // LONG_ARRAY type covers INT_ARRAY and LONG_ARRAY.
            if (columnValue instanceof int[]) {
              int[] ints = (int[]) columnValue;
              int length = ints.length;
              long[] longs = new long[length];
              for (int j = 0; j < length; j++) {
                longs[j] = ints[j];
              }
              dataTableBuilder.setColumn(i, longs);
            } else {
              dataTableBuilder.setColumn(i, (long[]) columnValue);
            }
            break;
          case FLOAT_ARRAY:
            dataTableBuilder.setColumn(i, (float[]) columnValue);
            break;
          case DOUBLE_ARRAY:
            // DOUBLE_ARRAY type covers INT_ARRAY, LONG_ARRAY, FLOAT_ARRAY and DOUBLE_ARRAY.
            if (columnValue instanceof int[]) {
              int[] ints = (int[]) columnValue;
              int length = ints.length;
              double[] doubles = new double[length];
              for (int j = 0; j < length; j++) {
                doubles[j] = ints[j];
              }
              dataTableBuilder.setColumn(i, doubles);
            } else if (columnValue instanceof long[]) {
              long[] longs = (long[]) columnValue;
              int length = longs.length;
              double[] doubles = new double[length];
              for (int j = 0; j < length; j++) {
                doubles[j] = longs[j];
              }
              dataTableBuilder.setColumn(i, doubles);
            } else if (columnValue instanceof float[]) {
              float[] floats = (float[]) columnValue;
              int length = floats.length;
              double[] doubles = new double[length];
              for (int j = 0; j < length; j++) {
                doubles[j] = floats[j];
              }
              dataTableBuilder.setColumn(i, doubles);
            } else {
              dataTableBuilder.setColumn(i, (double[]) columnValue);
            }
            break;
          case STRING_ARRAY:
            dataTableBuilder.setColumn(i, (String[]) columnValue);
            break;

          default:
            throw new UnsupportedOperationException(
                "Unsupported data type: " + columnType + " for column: " + dataSchema.getColumnName(i));
        }
      }
      dataTableBuilder.finishRow();
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
  @Nonnull
  public static Serializable[] extractRowFromDataTable(@Nonnull DataTable dataTable, int rowId) {
    DataSchema dataSchema = dataTable.getDataSchema();
    int numColumns = dataSchema.size();

    Serializable[] row = new Serializable[numColumns];
    for (int i = 0; i < numColumns; i++) {
      DataType columnType = dataSchema.getColumnType(i);
      switch (columnType) {
        // Single-value column.
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
        case STRING:
          row[i] = dataTable.getString(rowId, i);
          break;

        // Multi-value column.
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
          throw new UnsupportedOperationException(
              "Unsupported data type: " + columnType + " for column: " + dataSchema.getColumnName(i));
      }
    }

    return row;
  }

  /**
   * Reduce a collection of {@link DataTable}s to selection rows for selection queries without <code>ORDER BY</code>.
   * (Broker side)
   *
   * @param selectionResults {@link Map} from {@link ServerInstance} to {@link DataTable}.
   * @param selectionSize size of the selection.
   * @return reduced results.
   */
  @Nonnull
  public static List<Serializable[]> reduceWithoutOrdering(@Nonnull Map<ServerInstance, DataTable> selectionResults,
      int selectionSize) {
    List<Serializable[]> rows = new ArrayList<>(selectionSize);
    for (DataTable dataTable : selectionResults.values()) {
      int numRows = dataTable.getNumberOfRows();
      for (int rowId = 0; rowId < numRows; rowId++) {
        if (rows.size() < selectionSize) {
          rows.add(extractRowFromDataTable(dataTable, rowId));
        } else {
          return rows;
        }
      }
    }
    return rows;
  }

  /**
   * Render the unformatted selection rows to a formatted {@link SelectionResults} object for selection queries without
   * <code>ORDER BY</code>. (Broker side)
   * <p>{@link SelectionResults} object will be used to build the broker response.
   * <p>Should be called after method "reduceWithoutOrdering()".
   *
   * @param rows unformatted selection rows.
   * @param dataSchema data schema.
   * @param selectionColumns selection columns.
   * @return {@link SelectionResults} object results.
   */
  @Nonnull
  public static SelectionResults renderSelectionResultsWithoutOrdering(@Nonnull List<Serializable[]> rows,
      @Nonnull DataSchema dataSchema, @Nonnull List<String> selectionColumns) {
    // TODO: remove the code for backward compatible after server updated to the latest code.
    int numSelectionColumns = selectionColumns.size();
    int[] columnIndices = new int[numSelectionColumns];
    Map<String, Integer> dataSchemaIndices = new HashMap<>(numSelectionColumns);
    for (int i = 0; i < numSelectionColumns; i++) {
      dataSchemaIndices.put(dataSchema.getColumnName(i), i);
    }
    for (int i = 0; i < numSelectionColumns; i++) {
      columnIndices[i] = dataSchemaIndices.get(selectionColumns.get(i));
    }
    int numRows = rows.size();
    for (int i = 0; i < numRows; i++) {
      rows.set(i, getFormattedRowWithoutOrdering(rows.get(i), dataSchema, columnIndices));
    }

    /* TODO: uncomment after server updated to the latest code.
    for (Serializable[] row : rows) {
      formatRowWithoutOrdering(row, dataSchema);
    }*/
    return new SelectionResults(selectionColumns, rows);
  }

  /**
   * Helper method to format a selection row, make all values string or string array type based on data schema passed in
   * for selection queries without <code>ORDER BY</code>. (Broker side)
   * <p>Formatted row is used to build the {@link SelectionResults}.
   *
   * @param row selection row to be formatted.
   * @param dataSchema data schema.
   */
  private static void formatRowWithoutOrdering(@Nonnull Serializable[] row, @Nonnull DataSchema dataSchema) {
    int numColumns = row.length;
    for (int i = 0; i < numColumns; i++) {
      row[i] = getFormattedValue(row[i], dataSchema.getColumnType(i));
    }
  }

  // TODO: remove this method after server updated to the latest code.
  private static Serializable[] getFormattedRowWithoutOrdering(@Nonnull Serializable[] row,
      @Nonnull DataSchema dataSchema, @Nonnull int[] columnIndices) {
    int numColumns = columnIndices.length;
    Serializable[] formattedRow = new Serializable[numColumns];
    for (int i = 0; i < numColumns; i++) {
      int columnIndex = columnIndices[i];
      formattedRow[i] =
          SelectionOperatorUtils.getFormattedValue(row[columnIndex], dataSchema.getColumnType(columnIndex));
    }
    return formattedRow;
  }

  /**
   * Format a {@link Serializable} value into a {@link String} or {@link String} array based on the data type.
   * (Broker side)
   * <p>Actual value type can be different with data type passed in, but they must be type compatible.
   *
   * @param value value to be formatted.
   * @param dataType data type.
   * @return formatted value.
   */
  @Nonnull
  public static Serializable getFormattedValue(@Nonnull Serializable value, @Nonnull DataType dataType) {
    switch (dataType) {
      // Single-value column.
      case INT:
        return INT_FORMAT.format(((Number) value).intValue());
      case LONG:
        return LONG_FORMAT.format(((Number) value).longValue());
      case FLOAT:
        return FLOAT_FORMAT.format(((Number) value).floatValue());
      case DOUBLE:
        return DOUBLE_FORMAT.format(((Number) value).doubleValue());

      // Multi-value column.
      case INT_ARRAY:
        int[] ints = (int[]) value;
        int length = ints.length;
        String[] formattedValue = new String[length];
        for (int i = 0; i < length; i++) {
          formattedValue[i] = INT_FORMAT.format(ints[i]);
        }
        return formattedValue;
      case LONG_ARRAY:
        // LONG_ARRAY type covers INT_ARRAY and LONG_ARRAY.
        if (value instanceof int[]) {
          ints = (int[]) value;
          length = ints.length;
          formattedValue = new String[length];
          for (int i = 0; i < length; i++) {
            formattedValue[i] = LONG_FORMAT.format(ints[i]);
          }
        } else {
          long[] longs = (long[]) value;
          length = longs.length;
          formattedValue = new String[length];
          for (int i = 0; i < length; i++) {
            formattedValue[i] = LONG_FORMAT.format(longs[i]);
          }
        }
        return formattedValue;
      case FLOAT_ARRAY:
        float[] floats = (float[]) value;
        length = floats.length;
        formattedValue = new String[length];
        for (int i = 0; i < length; i++) {
          formattedValue[i] = FLOAT_FORMAT.format(floats[i]);
        }
        return formattedValue;
      case DOUBLE_ARRAY:
        // DOUBLE_ARRAY type covers INT_ARRAY, LONG_ARRAY, FLOAT_ARRAY and DOUBLE_ARRAY.
        if (value instanceof int[]) {
          ints = (int[]) value;
          length = ints.length;
          formattedValue = new String[length];
          for (int i = 0; i < length; i++) {
            formattedValue[i] = DOUBLE_FORMAT.format((double) ints[i]);
          }
          return formattedValue;
        } else if (value instanceof long[]) {
          long[] longs = (long[]) value;
          length = longs.length;
          formattedValue = new String[length];
          for (int i = 0; i < length; i++) {
            formattedValue[i] = DOUBLE_FORMAT.format((double) longs[i]);
          }
          return formattedValue;
        } else if (value instanceof float[]) {
          floats = (float[]) value;
          length = floats.length;
          formattedValue = new String[length];
          for (int i = 0; i < length; i++) {
            formattedValue[i] = DOUBLE_FORMAT.format(floats[i]);
          }
          return formattedValue;
        } else {
          double[] doubles = (double[]) value;
          length = doubles.length;
          formattedValue = new String[length];
          for (int i = 0; i < length; i++) {
            formattedValue[i] = DOUBLE_FORMAT.format(doubles[i]);
          }
          return formattedValue;
        }
      default:
        // For STRING and STRING_ARRAY, no need to format.
        return value;
    }
  }

  /**
   * Helper method to add a value to a {@link PriorityQueue}.
   *
   * @param value value to be added.
   * @param queue priority queue.
   * @param maxNumValues maximum number of values in the priority queue.
   * @param <T> type for the value.
   */
  public static <T> void addToPriorityQueue(@Nonnull T value, @Nonnull PriorityQueue<T> queue, int maxNumValues) {
    if (queue.size() < maxNumValues) {
      queue.add(value);
    } else if (queue.comparator().compare(queue.peek(), value) < 0) {
      queue.poll();
      queue.offer(value);
    }
  }
}
