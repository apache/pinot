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
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.response.ServerInstance;
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
  private final List<SelectionSort> _sortSequence;
  private final DataSchema _dataSchema;
  private final int _offset;
  private final int _numRowsToKeep;
  private final PriorityQueue<Serializable[]> _rows;

  /**
   * Constructor for <code>SelectionOperatorService</code> with {@link DataSchema}. (Inter segment)
   *
   * @param selection selection query.
   * @param dataSchema data schema.
   */
  public SelectionOperatorService(Selection selection, DataSchema dataSchema) {
    _selectionColumns = SelectionOperatorUtils.getSelectionColumns(selection.getSelectionColumns(), dataSchema);
    _sortSequence = selection.getSelectionSortSequence();
    _dataSchema = dataSchema;
    // Select rows from offset to offset + size.
    _offset = selection.getOffset();
    _numRowsToKeep = _offset + selection.getSize();
    _rows = new PriorityQueue<>(Math.min(_numRowsToKeep, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY),
        getTypeCompatibleComparator());
  }

  /**
   * Helper method to get the type-compatible {@link Comparator} for selection rows. (Inter segment)
   * <p>Type-compatible comparator allows compatible types to compare with each other.
   *
   * @return flexible {@link Comparator} for selection rows.
   */
  private Comparator<Serializable[]> getTypeCompatibleComparator() {
    // Compare all single-value columns
    int numOrderByExpressions = _sortSequence.size();
    List<Integer> valueIndexList = new ArrayList<>(numOrderByExpressions);
    for (int i = 0; i < numOrderByExpressions; i++) {
      if (!_dataSchema.getColumnDataType(i).isArray()) {
        valueIndexList.add(i);
      }
    }

    int numValuesToCompare = valueIndexList.size();
    int[] valueIndices = new int[numValuesToCompare];
    Comparator[] valueComparators = new Comparator[numValuesToCompare];
    for (int i = 0; i < numValuesToCompare; i++) {
      int valueIndex = valueIndexList.get(i);
      valueIndices[i] = valueIndex;
      if (_dataSchema.getColumnDataType(i).isNumber()) {
        valueComparators[i] = Comparator.comparingDouble(Number::doubleValue);
      } else {
        valueComparators[i] = Comparator.naturalOrder();
      }
      if (_sortSequence.get(valueIndex).isIsAsc()) {
        valueComparators[i] = valueComparators[i].reversed();
      }
    }

    return new SelectionOperatorUtils.RowComparator(valueIndices, valueComparators);
  }

  /**
   * Get the selection results.
   *
   * @return selection results.
   */
  public PriorityQueue<Serializable[]> getRows() {
    return _rows;
  }

  /**
   * Reduce a collection of {@link DataTable}s to selection rows for selection queries with <code>ORDER BY</code>.
   * (Broker side)
   *
   * @param selectionResults {@link Map} from {@link ServerInstance} to {@link DataTable}.
   */
  public void reduceWithOrdering(Map<ServerInstance, DataTable> selectionResults) {
    for (DataTable dataTable : selectionResults.values()) {
      int numRows = dataTable.getNumberOfRows();
      for (int rowId = 0; rowId < numRows; rowId++) {
        Serializable[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
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
    DataSchema.ColumnDataType[] columnDataTypes;
    if (preserveType) {
      columnDataTypes = null;
    } else {
      int numColumns = _selectionColumns.size();
      columnDataTypes = new DataSchema.ColumnDataType[numColumns];
      for (int i = 0; i < numColumns; i++) {
        columnDataTypes[i] = _dataSchema.getColumnDataType(columnIndices[i]);
      }
    }
    while (_rows.size() > _offset) {
      rowsInSelectionResults
          .addFirst(SelectionOperatorUtils.extractColumns(_rows.poll(), columnIndices, columnDataTypes));
    }

    return new SelectionResults(_selectionColumns, rowsInSelectionResults);
  }
}
