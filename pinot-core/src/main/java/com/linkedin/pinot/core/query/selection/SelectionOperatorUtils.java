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
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.DataTableBuilder;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
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
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * The <code>SelectionOperatorUtils</code> class provides the utility methods for selection queries without
 * <code>ORDER BY</code>.
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

  private static final Map<DataType, DecimalFormat> DEFAULT_FORMAT_STRING_MAP = new HashMap<>();

  static {
    DEFAULT_FORMAT_STRING_MAP.put(DataType.INT,
        new DecimalFormat("##########", DecimalFormatSymbols.getInstance(Locale.US)));
    DEFAULT_FORMAT_STRING_MAP.put(DataType.LONG,
        new DecimalFormat("####################", DecimalFormatSymbols.getInstance(Locale.US)));
    DEFAULT_FORMAT_STRING_MAP.put(DataType.FLOAT,
        new DecimalFormat("#########0.0####", DecimalFormatSymbols.getInstance(Locale.US)));
    DEFAULT_FORMAT_STRING_MAP.put(DataType.DOUBLE,
        new DecimalFormat("###################0.0#########", DecimalFormatSymbols.getInstance(Locale.US)));
  }

  /**
   * Merge two partial results for selection queries without <code>ORDER BY</code>. (Server side)
   *
   * @param mergedRowEventsSet partial results 1.
   * @param toMergeRowEventsSet partial results 2.
   * @param maxRowSize maximum number of rows in merged result.
   */
  public static void mergeWithoutOrdering(@Nonnull Collection<Serializable[]> mergedRowEventsSet,
      @Nonnull Collection<Serializable[]> toMergeRowEventsSet, int maxRowSize) {
    Iterator<Serializable[]> iterator = toMergeRowEventsSet.iterator();
    while (mergedRowEventsSet.size() < maxRowSize && iterator.hasNext()) {
      mergedRowEventsSet.add(iterator.next());
    }
  }

  /**
   * Reduce a collection of {@link DataTable}s to selection results for selection queries without <code>ORDER BY</code>.
   * (Broker side)
   *
   * @param selectionResults {@link Map} from {@link ServerInstance} to {@link DataTable}.
   * @param maxRowSize maximum number of rows in reduced result.
   * @return reduced results.
   */
  public static Collection<Serializable[]> reduceWithoutOrdering(
      @Nonnull Map<ServerInstance, DataTable> selectionResults, int maxRowSize) {
    Collection<Serializable[]> rowEventsSet = new ArrayList<>(maxRowSize);
    for (DataTable dataTable : selectionResults.values()) {
      int numRows = dataTable.getNumberOfRows();
      for (int rowId = 0; rowId < numRows; rowId++) {
        if (rowEventsSet.size() < maxRowSize) {
          rowEventsSet.add(extractRowFromDataTable(dataTable, rowId));
        } else {
          break;
        }
      }
    }
    return rowEventsSet;
  }

  /**
   * Render the final selection results to a {@link SelectionResults} object for selection queries without
   * <code>ORDER BY</code>. (Broker side)
   * <p>{@link SelectionResults} object will be used in building the BrokerResponse.
   *
   * @param finalResults final selection results.
   * @param selectionColumns selection columns.
   * @param dataSchema data schema.
   * @return {@link SelectionResults} object results.
   */
  public static SelectionResults renderSelectionResultsWithoutOrdering(@Nonnull Collection<Serializable[]> finalResults,
      @Nonnull List<String> selectionColumns, @Nonnull DataSchema dataSchema) {
    selectionColumns = getSelectionColumns(selectionColumns, dataSchema);

    List<Serializable[]> rowEvents = new ArrayList<>();
    for (Serializable[] row : finalResults) {
      rowEvents.add(getFormattedRow(row, selectionColumns, dataSchema));
    }

    return new SelectionResults(selectionColumns, rowEvents);
  }

  /**
   * Expand <code>'SELECT *'</code> to select all columns with {@link IndexSegment}, order all columns alphabatically.
   * (Server side)
   *
   * @param selectionColumns unexpanded selection columns (may contain '*').
   * @param indexSegment index segment.
   * @return expanded selection columns.
   */
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
   * Expand <code>'SELECT *'</code> to select all columns with {@link DataSchema}, order all columns alphabatically.
   * (Broker side)
   *
   * @param selectionColumns unexpanded selection columns (may contain '*').
   * @param dataSchema data schema.
   * @return expanded selection columns.
   */
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
   * Extract all related columns for a selection query with {@link IndexSegment}. (Server side)
   *
   * @param selection selection query.
   * @param indexSegment index segment.
   * @return all related columns.
   */
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
   * Extract the {@link DataSchema} from selection columns and {@link IndexSegment}.
   *
   * @param selectionColumns selection columns.
   * @param indexSegment index segment.
   * @return data schema.
   */
  public static DataSchema extractDataSchema(@Nonnull String[] selectionColumns, @Nonnull IndexSegment indexSegment) {
    return extractDataSchema(null, Arrays.asList(selectionColumns), indexSegment);
  }

  /**
   * Extract the {@link DataSchema} from sort sequence, selection columns and {@link IndexSegment}.
   *
   * @param sortSequence sort sequence.
   * @param selectionColumns selection columns.
   * @param indexSegment index segment.
   * @return data schema.
   */
  public static DataSchema extractDataSchema(@Nullable List<SelectionSort> sortSequence,
      @Nonnull List<String> selectionColumns, @Nonnull IndexSegment indexSegment) {
    List<String> columns = new ArrayList<>();

    if (sortSequence != null) {
      for (SelectionSort selectionSort : sortSequence) {
        columns.add(selectionSort.getColumn());
      }
    }

    String[] selectionColumnArray = selectionColumns.toArray(new String[selectionColumns.size()]);
    Arrays.sort(selectionColumnArray);
    for (String selectionColumn : selectionColumnArray) {
      if (!columns.contains(selectionColumn)) {
        columns.add(selectionColumn);
      }
    }

    int numColumns = columns.size();
    DataType[] dataTypes = new DataType[numColumns];
    for (int i = 0; i < dataTypes.length; ++i) {
      DataSource ds = indexSegment.getDataSource(columns.get(i));
      DataSourceMetadata m = ds.getDataSourceMetadata();
      dataTypes[i] = m.getDataType();
      if (!m.isSingleValue()) {
        dataTypes[i] = DataType.valueOf(dataTypes[i] + "_ARRAY");
      }
    }

    return new DataSchema(columns.toArray(new String[numColumns]), dataTypes);
  }

  /**
   * Format a selection row to make all values {@link String} type.
   *
   * @param row selection row.
   * @param selectionColumns selection columns.
   * @param dataSchema data schema.
   * @return formatted selection row.
   */
  public static Serializable[] getFormattedRow(@Nonnull Serializable[] row, @Nonnull List<String> selectionColumns,
      @Nonnull DataSchema dataSchema) {
    int numColumns = selectionColumns.size();
    Serializable[] formattedRow = new Serializable[numColumns];
    Map<String, Integer> columnToIdxMapping = buildColumnToIdxMappingForDataSchema(dataSchema);

    for (int i = 0; i < numColumns; i++) {
      String column = selectionColumns.get(i);
      if (columnToIdxMapping.containsKey(column)) {
        int idxInDataSchema = columnToIdxMapping.get(selectionColumns.get(i));
        DataType columnType = dataSchema.getColumnType(idxInDataSchema);

        if (columnType.isSingleValue()) {
          // Single-value column.
          if (columnType == DataType.STRING) {
            formattedRow[i] = row[idxInDataSchema];
          } else {
            formattedRow[i] =
                DEFAULT_FORMAT_STRING_MAP.get(dataSchema.getColumnType(idxInDataSchema)).format(row[idxInDataSchema]);
          }
        } else {
          // Multi-value column.
          String[] multiValues;
          int numValues;

          switch (columnType) {
            case INT_ARRAY:
              int[] intValues = (int[]) row[idxInDataSchema];
              numValues = intValues.length;
              multiValues = new String[numValues];
              for (int j = 0; j < numValues; j++) {
                multiValues[j] = DEFAULT_FORMAT_STRING_MAP.get(DataType.INT).format(intValues[j]);
              }
              break;
            case LONG_ARRAY:
              long[] longValues = (long[]) row[idxInDataSchema];
              numValues = longValues.length;
              multiValues = new String[numValues];
              for (int j = 0; j < numValues; j++) {
                multiValues[j] = DEFAULT_FORMAT_STRING_MAP.get(DataType.LONG).format(longValues[j]);
              }
              break;
            case FLOAT_ARRAY:
              float[] floatValues = (float[]) row[idxInDataSchema];
              numValues = floatValues.length;
              multiValues = new String[numValues];
              for (int j = 0; j < numValues; j++) {
                multiValues[j] = DEFAULT_FORMAT_STRING_MAP.get(DataType.FLOAT).format(floatValues[j]);
              }
              break;
            case DOUBLE_ARRAY:
              double[] doubleValues = (double[]) row[idxInDataSchema];
              numValues = doubleValues.length;
              multiValues = new String[numValues];
              for (int j = 0; j < numValues; j++) {
                multiValues[j] = DEFAULT_FORMAT_STRING_MAP.get(DataType.DOUBLE).format(doubleValues[j]);
              }
              break;
            case STRING_ARRAY:
              multiValues = (String[]) row[idxInDataSchema];
              break;
            default:
              throw new RuntimeException(
                  "Unsupported data type " + columnType + " for column " + dataSchema.getColumnName(idxInDataSchema));
          }
          formattedRow[i] = multiValues;
        }
      }
    }
    return formattedRow;
  }

  /**
   * Build a {@link Map} from column name to column index in {@link DataSchema}.
   *
   * @param dataSchema data schema.
   * @return {@link Map} from column name to column index.
   */
  private static Map<String, Integer> buildColumnToIdxMappingForDataSchema(@Nonnull DataSchema dataSchema) {
    Map<String, Integer> columnToIdxMapping = new HashMap<>();
    int numColumns = dataSchema.size();
    for (int i = 0; i < numColumns; i++) {
      columnToIdxMapping.put(dataSchema.getColumnName(i), i);
    }
    return columnToIdxMapping;
  }

  /**
   * Extract a selection row from {@link DataTable}.
   *
   * @param dataTable data table.
   * @param rowId row id.
   * @return selection row.
   */
  public static Serializable[] extractRowFromDataTable(@Nonnull DataTable dataTable, int rowId) {
    DataSchema dataSchema = dataTable.getDataSchema();
    int numRows = dataSchema.size();

    Serializable[] row = new Serializable[numRows];
    for (int i = 0; i < numRows; i++) {
      switch (dataSchema.getColumnType(i)) {
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
          throw new RuntimeException(
              "Unsupported data type " + dataSchema.getColumnType(i) + " for column " + dataSchema.getColumnName(i));
      }
    }
    return row;
  }

  /**
   * Build a {@link DataTable} from a {@link Collection} of selection rows with {@link DataSchema}.
   *
   * @param rowEventsSet {@link Collection} of selection rows.
   * @param dataSchema data schema.
   * @return data table.
   * @throws Exception
   */
  public static DataTable getDataTableFromRowSet(@Nonnull Collection<Serializable[]> rowEventsSet,
      @Nonnull DataSchema dataSchema)
      throws Exception {
    DataTableBuilder dataTableBuilder = new DataTableBuilder(dataSchema);
    dataTableBuilder.open();
    int numRows = dataSchema.size();

    for (Serializable[] row : rowEventsSet) {
      dataTableBuilder.startRow();
      for (int i = 0; i < numRows; i++) {
        switch (dataSchema.getColumnType(i)) {
          // Single-value column.
          case INT:
            dataTableBuilder.setColumn(i, ((Integer) row[i]).intValue());
            break;
          case LONG:
            dataTableBuilder.setColumn(i, ((Long) row[i]).longValue());
            break;
          case FLOAT:
            dataTableBuilder.setColumn(i, ((Float) row[i]).floatValue());
            break;
          case DOUBLE:
            dataTableBuilder.setColumn(i, ((Double) row[i]).doubleValue());
            break;
          case STRING:
            dataTableBuilder.setColumn(i, ((String) row[i]));
            break;

          // Multi-value column.
          case INT_ARRAY:
            dataTableBuilder.setColumn(i, (int[]) row[i]);
            break;
          case LONG_ARRAY:
            dataTableBuilder.setColumn(i, (long[]) row[i]);
            break;
          case FLOAT_ARRAY:
            dataTableBuilder.setColumn(i, (float[]) row[i]);
            break;
          case DOUBLE_ARRAY:
            dataTableBuilder.setColumn(i, (double[]) row[i]);
            break;
          case STRING_ARRAY:
            dataTableBuilder.setColumn(i, (String[]) row[i]);
            break;
          default:
            throw new RuntimeException(
                "Unsupported data type " + dataSchema.getColumnType(i) + " for column " + dataSchema.getColumnName(i));
        }
      }
      dataTableBuilder.finishRow();
    }
    dataTableBuilder.seal();
    return dataTableBuilder.build();
  }

  /**
   * Get a {@link String} representation of a selection row.
   *
   * @param row selection row.
   * @param dataSchema data schema.
   * @return {@link String} representation of the selection row.
   */
  public static String getRowStringFromSerializable(@Nonnull Serializable[] row, @Nonnull DataSchema dataSchema) {
    StringBuilder rowStringBuilder = new StringBuilder();
    int numColumns = dataSchema.size();
    for (int i = 0; i < numColumns; ++i) {
      if (i != 0) {
        rowStringBuilder.append(" : ");
      }
      DataType columnType = dataSchema.getColumnType(i);
      if (columnType.isSingleValue()) {
        // Single-value column.
        rowStringBuilder.append(row[i]);
      } else {
        // Multi-value column.
        rowStringBuilder.append("[ ");
        int arrayLength;
        switch (columnType) {
          case INT_ARRAY:
            int[] intValues = (int[]) row[i];
            arrayLength = intValues.length;
            for (int j = 0; j < arrayLength; j++) {
              rowStringBuilder.append(intValues[j]).append(' ');
            }
            break;
          case LONG_ARRAY:
            long[] longValues = (long[]) row[i];
            arrayLength = longValues.length;
            for (int j = 0; j < arrayLength; j++) {
              rowStringBuilder.append(longValues[j]).append(' ');
            }
            break;
          case FLOAT_ARRAY:
            float[] floatValues = (float[]) row[i];
            arrayLength = floatValues.length;
            for (int j = 0; j < arrayLength; j++) {
              rowStringBuilder.append(floatValues[j]).append(' ');
            }
            break;
          case DOUBLE_ARRAY:
            double[] doubleValues = (double[]) row[i];
            arrayLength = doubleValues.length;
            for (int j = 0; j < arrayLength; j++) {
              rowStringBuilder.append(doubleValues[j]).append(' ');
            }
            break;
          case STRING_ARRAY:
            String[] stringValues = (String[]) row[i];
            arrayLength = stringValues.length;
            for (int j = 0; j < arrayLength; j++) {
              rowStringBuilder.append(stringValues[j]).append(' ');
            }
            break;
          default:
            throw new RuntimeException(
                "Unsupported data type " + dataSchema.getColumnType(i) + " for column " + dataSchema.getColumnName(i));
        }
        rowStringBuilder.append(']');
      }
    }
    return rowStringBuilder.toString();
  }
}
