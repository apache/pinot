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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * SelectionOperatorUtils provides the utility methods for selection query with capability of un-sorting.
 *
 * Expected behavior:
 * - Return selection results with the same order of columns as user passed in.
 *   Eg. SELECT colB, colA, colC FROM table -> [valB, valA, valC]
 * - For 'select *', return columns with alphabetically order.
 *   Eg. SELECT * FROM table -> [valA, valB, valC]
 * - Order by does not change the order of columns in selection results.
 *   Eg. SELECT colB, colA, colC FROM table ORDER BY calC -> [valB, valA, valC]
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
   * Expand 'select *' to select all columns using index segment, order all columns alphabatically.
   *
   * @param selectionColumns unexpanded selection columns (may contain '*').
   * @param indexSegment index segment.
   * @return expanded selection columns.
   */
  public static List<String> getSelectionColumns(List<String> selectionColumns, IndexSegment indexSegment) {
    if (selectionColumns.size() == 1 && selectionColumns.get(0).equals("*")) {
      List<String> allColumns = Arrays.asList(indexSegment.getColumnNames());
      Collections.sort(allColumns);
      return allColumns;
    } else {
      return selectionColumns;
    }
  }


  /**
   * Expand 'select *' to select all columns using data schema, order all columns alphabatically.
   *
   * @param selectionColumns unexpanded selection columns (may contain '*').
   * @param dataSchema data schema.
   * @return expanded selection columns.
   */
  public static List<String> getSelectionColumns(List<String> selectionColumns, DataSchema dataSchema) {
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
   * Extract all related columns for a selection query using index segment.
   *
   * @param selection selection query.
   * @param indexSegment index segment.
   * @return all related columns.
   */
  public static String[] extractSelectionRelatedColumns(Selection selection, IndexSegment indexSegment) {
    Set<String> selectionColumns = new HashSet<>(getSelectionColumns(selection.getSelectionColumns(), indexSegment));
    if (selection.getSelectionSortSequence() != null) {
      for (SelectionSort selectionSort : selection.getSelectionSortSequence()) {
        selectionColumns.add(selectionSort.getColumn());
      }
    }
    return selectionColumns.toArray(new String[selectionColumns.size()]);
  }

  /**
   * Merge two partial results for non-sorting selection query.
   *
   * @param rowEventsSet1 partial results 1.
   * @param rowEventsSet2 partial results 2.
   * @param maxRowSize maximum number of rows in merged result.
   * @return merged result.
   */
  public static Collection<Serializable[]> mergeWithoutOrdering(Collection<Serializable[]> rowEventsSet1,
      Collection<Serializable[]> rowEventsSet2, int maxRowSize) {
    if (rowEventsSet1 == null) {
      return rowEventsSet2;
    }
    if (rowEventsSet2 == null) {
      return rowEventsSet1;
    }
    Iterator<Serializable[]> iterator = rowEventsSet2.iterator();
    while (rowEventsSet1.size() < maxRowSize && iterator.hasNext()) {
      rowEventsSet1.add(iterator.next());
    }
    return rowEventsSet1;
  }

  /**
   * Reduce a collection of data tables to selection results for non-sorting selection query.
   *
   * @param selectionResults map from server instance to data table.
   * @param maxRowSize maximum number of rows in reduced result.
   * @return reduced result.
   */
  public static Collection<Serializable[]> reduceWithoutOrdering(Map<ServerInstance, DataTable> selectionResults,
      int maxRowSize) {
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
   * Render the final selection results to a JSONObject for non-sorting selection query.
   *
   * @param finalResults final selection results.
   * @param selectionColumns selection columns.
   * @param dataSchema data schema.
   * @return final JSONObject result.
   * @throws Exception
   */
  public static JSONObject renderWithoutOrdering(Collection<Serializable[]> finalResults, List<String> selectionColumns,
      DataSchema dataSchema)
      throws Exception {
    selectionColumns = getSelectionColumns(selectionColumns, dataSchema);

    List<JSONArray> rowEventsJsonList = new ArrayList<>();
    for (Serializable[] row : finalResults) {
      rowEventsJsonList.add(getJSonArrayFromRow(row, selectionColumns, dataSchema));
    }

    JSONObject resultJsonObject = new JSONObject();
    resultJsonObject.put("results", new JSONArray(rowEventsJsonList));
    resultJsonObject.put("columns", new JSONArray(selectionColumns));
    return resultJsonObject;
  }

  /**
   * Render the final selection results to a SelectionResults object for non-sorting selection query.
   * SelectionResults object will be used in building the BrokerResponse.
   *
   * @param finalResults final selection results.
   * @param selectionColumns selection columns.
   * @param dataSchema data schema.
   * @return SelectionResults object result.
   * @throws Exception
   */
  public static SelectionResults renderSelectionResultsWithoutOrdering(Collection<Serializable[]> finalResults,
      List<String> selectionColumns, DataSchema dataSchema)
      throws Exception {
    selectionColumns = getSelectionColumns(selectionColumns, dataSchema);

    List<Serializable[]> rowEvents = new ArrayList<>();
    for (Serializable[] row : finalResults) {
      rowEvents.add(getFormattedRow(row, selectionColumns, dataSchema));
    }

    return new SelectionResults(selectionColumns, rowEvents);
  }

  /**
   * Extract the data schema from selection columns list and index segment.
   *
   * @param selectionColumns selection columns.
   * @param indexSegment index segment.
   * @return extracted data schema.
   */
  public static DataSchema extractDataSchema(String[] selectionColumns, IndexSegment indexSegment) {
    return extractDataSchema(null, Arrays.asList(selectionColumns), indexSegment);
  }

  /**
   * Extract the data schema from sort columns list, selection columns list and index segment.
   *
   * @param sortSequence sort columns.
   * @param selectionColumns selection columns.
   * @param indexSegment index segment.
   * @return extracted data schema.
   */
  public static DataSchema extractDataSchema(List<SelectionSort> sortSequence, List<String> selectionColumns,
      IndexSegment indexSegment) {
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
   * Render a selection row to a JSONArray result.
   *
   * @param row selection row.
   * @param selectionColumns selection columns.
   * @param dataSchema data schema.
   * @return JSONArray result.
   * @throws JSONException
   */
  public static JSONArray getJSonArrayFromRow(Serializable[] row, List<String> selectionColumns, DataSchema dataSchema)
      throws JSONException {
    JSONArray jsonArray = new JSONArray();
    Map<String, Integer> columnToIdxMapping = buildColumnToIdxMappingForDataSchema(dataSchema);

    for (String column : selectionColumns) {
      // If column not part of schema, we put null value. This shouldn't be hit as segment level response
      // will be null if selecting columns not existed in the segment.
      if (!columnToIdxMapping.containsKey(column)) {
        jsonArray.put((String) null);
      } else {
        int idxInDataSchema = columnToIdxMapping.get(column);
        DataType columnType = dataSchema.getColumnType(idxInDataSchema);

        if (columnType.isSingleValue()) {
          // Single-value column.

          if (columnType == DataType.STRING || columnType == DataType.BOOLEAN) {
            jsonArray.put(row[idxInDataSchema]);
          } else {
            jsonArray.put(DEFAULT_FORMAT_STRING_MAP.get(dataSchema.getColumnType(idxInDataSchema))
                .format(row[idxInDataSchema]));
          }
        } else {
          // Multi-value column.

          JSONArray stringJsonArray = new JSONArray();
          switch (columnType) {
            case STRING_ARRAY:
              String[] stringValues = (String[]) row[idxInDataSchema];
              for (String s : stringValues) {
                stringJsonArray.put(s);
              }
              break;
            case INT_ARRAY:
              int[] intValues = (int[]) row[idxInDataSchema];
              for (int s : intValues) {
                stringJsonArray.put(DEFAULT_FORMAT_STRING_MAP.get(DataType.INT).format(s));
              }
              break;
            case FLOAT_ARRAY:
              float[] floatValues = (float[]) row[idxInDataSchema];
              for (float s : floatValues) {
                stringJsonArray.put(DEFAULT_FORMAT_STRING_MAP.get(DataType.FLOAT).format(s));
              }
              break;
            case LONG_ARRAY:
              long[] longValues = (long[]) row[idxInDataSchema];
              for (long s : longValues) {
                stringJsonArray.put(DEFAULT_FORMAT_STRING_MAP.get(DataType.LONG).format(s));
              }
              break;
            case DOUBLE_ARRAY:
              double[] doubleValues = (double[]) row[idxInDataSchema];
              for (double s : doubleValues) {
                stringJsonArray.put(DEFAULT_FORMAT_STRING_MAP.get(DataType.DOUBLE).format(s));
              }
              break;
            default:
              throw new RuntimeException(
                  "Unsupported data type " + columnType + " for column " + dataSchema.getColumnName(idxInDataSchema));
          }
          jsonArray.put(stringJsonArray);
        }
      }
    }
    return jsonArray;
  }

  /**
   * Format a selection row to make all values inside String type.
   *
   * @param row selection row.
   * @param selectionColumns selection columns.
   * @param dataSchema data schema.
   * @return formatted selection row.
   * @throws JSONException
   */
  public static Serializable[] getFormattedRow(Serializable[] row, List<String> selectionColumns, DataSchema dataSchema)
      throws JSONException {
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

          if (columnType == DataType.STRING || columnType == DataType.BOOLEAN) {
            formattedRow[i] = row[idxInDataSchema];
          } else {
            formattedRow[i] = DEFAULT_FORMAT_STRING_MAP.get(dataSchema.getColumnType(idxInDataSchema))
                .format(row[idxInDataSchema]);
          }
        } else {
          // Multi-value column.

          String[] multiValues;
          int numValues;

          switch (columnType) {
            case STRING_ARRAY:
              multiValues = (String[]) row[idxInDataSchema];
              break;
            case INT_ARRAY:
              int[] intValues = (int[]) row[idxInDataSchema];
              numValues = intValues.length;
              multiValues = new String[numValues];
              for (int j = 0; j < numValues; j++) {
                multiValues[j] = DEFAULT_FORMAT_STRING_MAP.get(DataType.INT).format(intValues[j]);
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
            case LONG_ARRAY:
              long[] longValues = (long[]) row[idxInDataSchema];
              numValues = longValues.length;
              multiValues = new String[numValues];
              for (int j = 0; j < numValues; j++) {
                multiValues[j] = DEFAULT_FORMAT_STRING_MAP.get(DataType.LONG).format(longValues[j]);
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
   * Build a map from column name to column index in data schema using data schema.
   *
   * @param dataSchema data schema.
   * @return map from column name to column index in data schema.
   */
  private static Map<String, Integer> buildColumnToIdxMappingForDataSchema(DataSchema dataSchema) {
    Map<String, Integer> columnToIdxMapping = new HashMap<>();
    int numColumns = dataSchema.size();
    for (int i = 0; i < numColumns; i++) {
      columnToIdxMapping.put(dataSchema.getColumnName(i), i);
    }
    return columnToIdxMapping;
  }

  /**
   * Extract a selection row from data table.
   *
   * @param dataTable data table.
   * @param rowId row id.
   * @return selection row.
   */
  public static Serializable[] extractRowFromDataTable(DataTable dataTable, int rowId) {
    DataSchema dataSchema = dataTable.getDataSchema();
    int numRows = dataSchema.size();

    Serializable[] row = new Serializable[numRows];
    for (int i = 0; i < numRows; i++) {
      switch (dataSchema.getColumnType(i)) {
        // Single-value column.
        case BOOLEAN:
          if (dataTable.getBoolean(rowId, i)) {
            row[i] = "true";
          } else {
            row[i] = "false";
          }
          break;
        case BYTE:
          row[i] = dataTable.getByte(rowId, i);
          break;
        case CHAR:
          row[i] = dataTable.getChar(rowId, i);
          break;
        case SHORT:
          row[i] = dataTable.getShort(rowId, i);
          break;
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
        case OBJECT:
          row[i] = dataTable.getObject(rowId, i);
          break;

        // Multi-value column.
        case BYTE_ARRAY:
          row[i] = dataTable.getByteArray(rowId, i);
          break;
        case CHAR_ARRAY:
          row[i] = dataTable.getCharArray(rowId, i);
          break;
        case SHORT_ARRAY:
          row[i] = dataTable.getShortArray(rowId, i);
          break;
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
   * Build a data table from a collection of selection rows using data schema.
   *
   * @param rowEventsSet collection of selection rows.
   * @param dataSchema data schema.
   * @return data table.
   * @throws Exception
   */
  public static DataTable getDataTableFromRowSet(Collection<Serializable[]> rowEventsSet, DataSchema dataSchema)
      throws Exception {
    DataTableBuilder dataTableBuilder = new DataTableBuilder(dataSchema);
    dataTableBuilder.open();
    int numRows = dataSchema.size();

    for (Serializable[] row : rowEventsSet) {
      dataTableBuilder.startRow();
      for (int i = 0; i < numRows; i++) {
        switch (dataSchema.getColumnType(i)) {
          // Single-value column.
          case BOOLEAN:
            dataTableBuilder.setColumn(i, Boolean.parseBoolean((String) row[i]));
            break;
          case BYTE:
            dataTableBuilder.setColumn(i, ((Byte) row[i]).byteValue());
            break;
          case CHAR:
            dataTableBuilder.setColumn(i, ((Character) row[i]).charValue());
            break;
          case SHORT:
            dataTableBuilder.setColumn(i, ((Short) row[i]).shortValue());
            break;
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
          case OBJECT:
            dataTableBuilder.setColumn(i, row[i]);
            break;

          // Multi-value column.
          case BYTE_ARRAY:
            dataTableBuilder.setColumn(i, (byte[]) row[i]);
            break;
          case CHAR_ARRAY:
            dataTableBuilder.setColumn(i, (char[]) row[i]);
            break;
          case SHORT_ARRAY:
            dataTableBuilder.setColumn(i, (short[]) row[i]);
            break;
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
   * Get a String representation of a selection row.
   *
   * @param row selection row.
   * @param dataSchema data schema.
   * @return String representation.
   */
  public static String getRowStringFromSerializable(Serializable[] row, DataSchema dataSchema) {
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
          case BYTE_ARRAY:
            byte[] byteValues = (byte[]) row[i];
            arrayLength = byteValues.length;
            for (int j = 0; j < arrayLength; j++) {
              rowStringBuilder.append(byteValues[j]).append(' ');
            }
            break;
          case CHAR_ARRAY:
            char[] charValues = (char[]) row[i];
            arrayLength = charValues.length;
            for (int j = 0; j < arrayLength; j++) {
              rowStringBuilder.append(charValues[j]).append(' ');
            }
            break;
          case SHORT_ARRAY:
            short[] shortValues = (short[]) row[i];
            arrayLength = shortValues.length;
            for (int j = 0; j < arrayLength; j++) {
              rowStringBuilder.append(shortValues[j]).append(' ');
            }
            break;
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
