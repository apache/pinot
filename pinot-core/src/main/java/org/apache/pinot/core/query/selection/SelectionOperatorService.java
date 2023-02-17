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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.roaringbitmap.RoaringBitmap;


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
@SuppressWarnings("rawtypes")
public class SelectionOperatorService {
  private final List<String> _selectionColumns;
  private final DataSchema _dataSchema;
  private final int _limit;
  private final int _numRowsToKeep;
  private final Queue<Object[]> _rowsToKeep;
  private final Comparator<Object[]> _comparator;

  private static class DataTableAndIndex {

    private final DataTable _dataTable;
    private final RoaringBitmap[] _nullBitmaps;
    private int _index;
    private Pair<Integer, Object[]> _currentMaterializedRow;

    private DataTableAndIndex(DataTable dataTable, boolean nullHandlingEnabled) {
      _dataTable = dataTable;
      if (nullHandlingEnabled) {
        _nullBitmaps = new RoaringBitmap[dataTable.getDataSchema().size()];
        for (int colId = 0; colId < _nullBitmaps.length; colId++) {
          _nullBitmaps[colId] = dataTable.getNullRowIds(colId);
        }
      } else {
        _nullBitmaps = null;
      }
      _index = 0;
    }

    public Object[] getRow() {
      // Because we'll be getting the same index row multiple times;
      if (_currentMaterializedRow != null && _currentMaterializedRow.getLeft() == _index) {
        return _currentMaterializedRow.getRight();
      }

      Object[] row = SelectionOperatorUtils.extractRowFromDataTable(_dataTable, _index);
      if (_nullBitmaps != null) {
        for (int colId = 0; colId < _nullBitmaps.length; colId++) {
          if (_nullBitmaps[colId] != null && _nullBitmaps[colId].contains(_index)) {
            row[colId] = null;
          }
        }
      }
      _currentMaterializedRow = Pair.of(_index, row);
      return row;
    }

    public void incrementIndex() {
      _index++;
    }

    public boolean hasNext() {
      return _index <= _dataTable.getNumberOfRows();
    }
  }

  /**
   * Constructor for <code>SelectionOperatorService</code> with {@link DataSchema}. (Inter segment)
   *
   * @param queryContext Selection order-by query
   * @param dataSchema data schema.
   */
  public SelectionOperatorService(QueryContext queryContext, DataSchema dataSchema) {
    _selectionColumns = SelectionOperatorUtils.getSelectionColumns(queryContext, dataSchema);
    _dataSchema = dataSchema;
    // Select rows from offset to offset + limit.
    _limit = queryContext.getLimit();
    _numRowsToKeep = queryContext.getOffset() + queryContext.getLimit();
    assert queryContext.getOrderByExpressions() != null;
    _rowsToKeep = new ArrayDeque<>(_limit);
    _comparator = SelectionOperatorUtils.getTypeCompatibleComparator(queryContext.getOrderByExpressions(), _dataSchema,
        queryContext.isNullHandlingEnabled());
  }

  /**
   * Get the selection results.
   *
   * @return selection results.
   */
  public Queue<Object[]> getRows() {
    return _rowsToKeep;
  }

  /**
   * Reduces a collection of {@link DataTable}s to selection rows for selection queries with <code>ORDER BY</code>.
   * (Broker side)
   */
  public void reduceWithOrdering(Collection<DataTable> dataTables, boolean nullHandlingEnabled) {
    List<DataTableAndIndex> dataTableToCurrentIndex = new LinkedList<>();
    for (DataTable table : dataTables) {
      dataTableToCurrentIndex.add(new DataTableAndIndex(table, nullHandlingEnabled));
    }

    Queue<Integer> dataTablesToRemove = new ArrayDeque<>();
    for (int i = 0; i < _numRowsToKeep; i++) {
      Object[] rowToKeep = null;
      int dataTableToIncrement = -1;
      for (int j = 0; j < dataTableToCurrentIndex.size(); j++) {
        DataTableAndIndex dataTableAndIndex = dataTableToCurrentIndex.get(j);
        if (dataTableAndIndex.hasNext()) {
          Object[] rowHere = dataTableAndIndex.getRow();
          if (rowToKeep == null || _comparator.compare(rowToKeep, rowHere) >= 0) {
            rowToKeep = rowHere;
            dataTableToIncrement = j;
          }
        } else {
          dataTablesToRemove.add(j);
        }
      }
      if (dataTableToIncrement != -1) {
        if (_rowsToKeep.size() >= _limit) {
          _rowsToKeep.poll();
        }
        dataTableToCurrentIndex.get(dataTableToIncrement).incrementIndex();
        _rowsToKeep.add(rowToKeep);
      } else {
        // No more rows in dataTables
        break;
      }

      while (!dataTablesToRemove.isEmpty()) {
        // Removing by index is faster with LinkedList
        dataTableToCurrentIndex.remove(dataTablesToRemove.poll().intValue());
      }
    }
  }

  /**
   * Render the selection rows to a {@link ResultTable} object for selection queries with <code>ORDER BY</code>.
   * (Broker side)
   * <p>{@link ResultTable} object will be used to build the broker response.
   * <p>Should be called after method "reduceWithOrdering()".
   */
  public ResultTable renderResultTableWithOrdering() {
    int[] columnIndices = SelectionOperatorUtils.getColumnIndices(_selectionColumns, _dataSchema);
    int numColumns = columnIndices.length;
    DataSchema resultDataSchema = SelectionOperatorUtils.getSchemaForProjection(_dataSchema, columnIndices);

    // Extract the result rows
    LinkedList<Object[]> rowsInSelectionResults = new LinkedList<>();
    while (!_rowsToKeep.isEmpty()) {
      Object[] row = _rowsToKeep.poll();
      assert row != null;
      Object[] extractedRow = new Object[numColumns];
      for (int i = 0; i < numColumns; i++) {
        Object value = row[columnIndices[i]];
        if (value != null) {
          extractedRow[i] = resultDataSchema.getColumnDataType(i).convertAndFormat(value);
        }
      }
      rowsInSelectionResults.addFirst(extractedRow);
    }

    return new ResultTable(resultDataSchema, rowsInSelectionResults);
  }
}
