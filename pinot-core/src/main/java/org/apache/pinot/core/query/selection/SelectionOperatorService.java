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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.response.broker.SelectionResults;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;


/**
 * The <code>SelectionOperatorService</code> class provides the services for selection queries with
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
 *   <li>
 *     Order by does not change the order of columns in selection results.
 *     <ul>
 *       <li>Eg. SELECT colB, colA, colC FROM table ORDER BY calC -> [valB, valA, valC]</li>
 *     </ul>
 *   </li>
 * </ul>
 */
public class SelectionOperatorService {
  private final List<String> _selectionColumns;
  private final DataSchema _dataSchema;
  private final int _offset;
  private final int _numRowsToKeep;
  private final PriorityQueue<Object[]> _rows;

  /**
   * Constructor for <code>SelectionOperatorService</code> with {@link DataSchema}. (Inter segment)
   *
   * @param selection selection query.
   * @param dataSchema data schema.
   */
  public SelectionOperatorService(Selection selection, DataSchema dataSchema) {
    _selectionColumns = SelectionOperatorUtils.getSelectionColumns(selection.getSelectionColumns(), dataSchema);
    _dataSchema = dataSchema;
    // Select rows from offset to offset + size.
    _offset = selection.getOffset();
    _numRowsToKeep = _offset + selection.getSize();
    _rows = new PriorityQueue<>(Math.min(_numRowsToKeep, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY),
        getTypeCompatibleComparator(selection.getSelectionSortSequence()));
  }

  /**
   * Helper method to get the type-compatible {@link Comparator} for selection rows. (Inter segment)
   * <p>Type-compatible comparator allows compatible types to compare with each other.
   *
   * @return flexible {@link Comparator} for selection rows.
   */
  private Comparator<Object[]> getTypeCompatibleComparator(List<SelectionSort> sortSequence) {
    // Compare all single-value columns
    int numOrderByExpressions = sortSequence.size();
    List<Integer> valueIndexList = new ArrayList<>(numOrderByExpressions);
    for (int i = 0; i < numOrderByExpressions; i++) {
      if (!_dataSchema.getColumnDataType(i).isArray()) {
        valueIndexList.add(i);
      }
    }

    int numValuesToCompare = valueIndexList.size();
    int[] valueIndices = new int[numValuesToCompare];
    boolean[] isNumber = new boolean[numValuesToCompare];
    // Use multiplier -1 or 1 to control ascending/descending order
    int[] multipliers = new int[numValuesToCompare];
    for (int i = 0; i < numValuesToCompare; i++) {
      int valueIndex = valueIndexList.get(i);
      valueIndices[i] = valueIndex;
      isNumber[i] = _dataSchema.getColumnDataType(valueIndex).isNumber();
      multipliers[i] = sortSequence.get(valueIndex).isIsAsc() ? -1 : 1;
    }

    return (o1, o2) -> {
      for (int i = 0; i < numValuesToCompare; i++) {
        int index = valueIndices[i];
        Object v1 = o1[index];
        Object v2 = o2[index];
        int result;
        if (isNumber[i]) {
          result = Double.compare(((Number) v1).doubleValue(), ((Number) v2).doubleValue());
        } else {
          //noinspection unchecked
          result = ((Comparable) v1).compareTo(v2);
        }
        if (result != 0) {
          return result * multipliers[i];
        }
      }
      return 0;
    };
  }

  /**
   * Get the selection results.
   *
   * @return selection results.
   */
  public PriorityQueue<Object[]> getRows() {
    return _rows;
  }

  /**
   * Reduces a collection of {@link DataTable}s to selection rows for selection queries with <code>ORDER BY</code>.
   * (Broker side)
   */
  public void reduceWithOrdering(Collection<DataTable> dataTables) {
    for (DataTable dataTable : dataTables) {
      int numRows = dataTable.getNumberOfRows();
      for (int rowId = 0; rowId < numRows; rowId++) {
        Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
        SelectionOperatorUtils.addToPriorityQueue(row, _rows, _numRowsToKeep);
      }
    }
  }

  /**
   * Render the selection rows to a {@link SelectionResults} object for selection queries with
   * <code>ORDER BY</code>. (Broker side)
   * <p>{@link SelectionResults} object will be used to build the broker response.
   * <p>Should be called after method "reduceWithOrdering()".
   *
   * @return {@link SelectionResults} object results.
   */
  public SelectionResults renderSelectionResultsWithOrdering(boolean preserveType) {
    LinkedList<Serializable[]> rowsInSelectionResults = new LinkedList<>();
    int[] columnIndices = SelectionOperatorUtils.getColumnIndices(_selectionColumns, _dataSchema);
    int numColumns = columnIndices.length;
    DataSchema.ColumnDataType[] columnDataTypes = _dataSchema.getColumnDataTypes();

    if (preserveType) {
      while (_rows.size() > _offset) {
        Object[] row = _rows.poll();
        assert row != null;
        Serializable[] extractedRow = new Serializable[numColumns];
        for (int i = 0; i < numColumns; i++) {
          int columnIndex = columnIndices[i];
          extractedRow[i] = SelectionOperatorUtils.convertValueToType(row[columnIndex], columnDataTypes[columnIndex]);
        }
        rowsInSelectionResults.addFirst(extractedRow);
      }
    } else {
      while (_rows.size() > _offset) {
        Object[] row = _rows.poll();
        assert row != null;
        Serializable[] extractedRow = new Serializable[numColumns];
        for (int i = 0; i < numColumns; i++) {
          int columnIndex = columnIndices[i];
          extractedRow[i] = SelectionOperatorUtils.getFormattedValue(row[columnIndex], columnDataTypes[columnIndex]);
        }
        rowsInSelectionResults.addFirst(extractedRow);
      }
    }

    return new SelectionResults(_selectionColumns, rowsInSelectionResults);
  }

  /**
   * Render the selection rows to a {@link ResultTable} object for selection queries with
   * <code>ORDER BY</code>. (Broker side)
   * <p>{@link ResultTable} object will be used to build the broker response.
   * <p>Should be called after method "reduceWithOrdering()".
   *
   * @return {@link SelectionResults} object results.
   */
  public ResultTable renderResultTableWithOrdering() {
    LinkedList<Object[]> rowsInSelectionResults = new LinkedList<>();
    int[] columnIndices = SelectionOperatorUtils.getColumnIndices(_selectionColumns, _dataSchema);
    int numColumns = columnIndices.length;
    DataSchema.ColumnDataType[] columnDataTypes = _dataSchema.getColumnDataTypes();

    while (_rows.size() > _offset) {
      Object[] row = _rows.poll();
      assert row != null;
      Object[] extractedRow = new Object[numColumns];
      for (int i = 0; i < numColumns; i++) {
        int columnIndex = columnIndices[i];
        extractedRow[i] = SelectionOperatorUtils.convertValueToType(row[columnIndex], columnDataTypes[columnIndex]);
      }
      rowsInSelectionResults.addFirst(extractedRow);
    }

    // Construct the result data schema
    String[] columnNames = _dataSchema.getColumnNames();
    String[] resultColumnNames = new String[numColumns];
    DataSchema.ColumnDataType[] resultColumnDataTypes = new DataSchema.ColumnDataType[numColumns];
    for (int i = 0; i < numColumns; i++) {
      int columnIndex = columnIndices[i];
      resultColumnNames[i] = columnNames[columnIndex];
      resultColumnDataTypes[i] = columnDataTypes[columnIndex];
    }
    DataSchema resultDataSchema = new DataSchema(resultColumnNames, resultColumnDataTypes);
    return new ResultTable(resultDataSchema, rowsInSelectionResults);
  }
}
