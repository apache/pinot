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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DataTable;
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
  private final QueryContext _queryContext;
  private final List<String> _selectionColumns;
  private final DataSchema _dataSchema;
  private final int _offset;
  private final int _numRowsToKeep;
  private final PriorityQueue<Object[]> _rows;

  /**
   * Constructor for <code>SelectionOperatorService</code> with {@link DataSchema}. (Inter segment)
   *
   * @param queryContext Selection order-by query
   * @param dataSchema data schema.
   */
  public SelectionOperatorService(QueryContext queryContext, DataSchema dataSchema) {
    _queryContext = queryContext;
    _selectionColumns = SelectionOperatorUtils.getSelectionColumns(queryContext, dataSchema);
    _dataSchema = dataSchema;
    // Select rows from offset to offset + limit.
    _offset = queryContext.getOffset();
    _numRowsToKeep = _offset + queryContext.getLimit();
    assert queryContext.getOrderByExpressions() != null;
    _rows = new PriorityQueue<>(Math.min(_numRowsToKeep, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY),
        getTypeCompatibleComparator(queryContext.getOrderByExpressions()));
  }

  /**
   * Helper method to get the type-compatible {@link Comparator} for selection rows. (Inter segment)
   * <p>Type-compatible comparator allows compatible types to compare with each other.
   *
   * @return flexible {@link Comparator} for selection rows.
   */
  private Comparator<Object[]> getTypeCompatibleComparator(List<OrderByExpressionContext> orderByExpressions) {
    ColumnDataType[] columnDataTypes = _dataSchema.getColumnDataTypes();

    // Compare all single-value columns
    int numOrderByExpressions = orderByExpressions.size();
    List<Integer> valueIndexList = new ArrayList<>(numOrderByExpressions);
    for (int i = 0; i < numOrderByExpressions; i++) {
      if (!columnDataTypes[i].isArray()) {
        valueIndexList.add(i);
      }
    }

    int numValuesToCompare = valueIndexList.size();
    int[] valueIndices = new int[numValuesToCompare];
    boolean[] useDoubleComparison = new boolean[numValuesToCompare];
    // Use multiplier -1 or 1 to control ascending/descending order
    int[] multipliers = new int[numValuesToCompare];
    for (int i = 0; i < numValuesToCompare; i++) {
      int valueIndex = valueIndexList.get(i);
      valueIndices[i] = valueIndex;
      if (columnDataTypes[valueIndex].isNumber()) {
        useDoubleComparison[i] = true;
      }
      multipliers[i] = orderByExpressions.get(valueIndex).isAsc() ? -1 : 1;
    }

    if (_queryContext.isNullHandlingEnabled()) {
      return (o1, o2) -> {
        for (int i = 0; i < numValuesToCompare; i++) {
          int index = valueIndices[i];
          Object v1 = o1[index];
          Object v2 = o2[index];
          if (v1 == null) {
            // The default null ordering is: 'NULLS LAST'.
            return v2 == null ? 0 : -multipliers[i];
          } else if (v2 == null) {
            return multipliers[i];
          }
          int result;
          if (useDoubleComparison[i]) {
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
    } else {
      return (o1, o2) -> {
        for (int i = 0; i < numValuesToCompare; i++) {
          int index = valueIndices[i];
          Object v1 = o1[index];
          Object v2 = o2[index];
          int result;
          if (useDoubleComparison[i]) {
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
  public void reduceWithOrdering(Collection<DataTable> dataTables, boolean nullHandlingEnabled) {
    for (DataTable dataTable : dataTables) {
      int numRows = dataTable.getNumberOfRows();
      if (nullHandlingEnabled) {
        RoaringBitmap[] nullBitmaps = new RoaringBitmap[dataTable.getDataSchema().size()];
        for (int colId = 0; colId < nullBitmaps.length; colId++) {
          nullBitmaps[colId] = dataTable.getNullRowIds(colId);
        }
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
          for (int colId = 0; colId < nullBitmaps.length; colId++) {
            if (nullBitmaps[colId] != null && nullBitmaps[colId].contains(rowId)) {
              row[colId] = null;
            }
          }
          SelectionOperatorUtils.addToPriorityQueue(row, _rows, _numRowsToKeep);
        }
      } else {
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
          SelectionOperatorUtils.addToPriorityQueue(row, _rows, _numRowsToKeep);
        }
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

    // Construct the result data schema
    String[] columnNames = _dataSchema.getColumnNames();
    ColumnDataType[] columnDataTypes = _dataSchema.getColumnDataTypes();
    String[] resultColumnNames = new String[numColumns];
    ColumnDataType[] resultColumnDataTypes = new ColumnDataType[numColumns];
    for (int i = 0; i < numColumns; i++) {
      int columnIndex = columnIndices[i];
      resultColumnNames[i] = columnNames[columnIndex];
      resultColumnDataTypes[i] = columnDataTypes[columnIndex];
    }
    DataSchema resultDataSchema = new DataSchema(resultColumnNames, resultColumnDataTypes);

    // Extract the result rows
    LinkedList<Object[]> rowsInSelectionResults = new LinkedList<>();
    while (_rows.size() > _offset) {
      Object[] row = _rows.poll();
      assert row != null;
      Object[] extractedRow = new Object[numColumns];
      for (int i = 0; i < numColumns; i++) {
        Object value = row[columnIndices[i]];
        if (value != null) {
          extractedRow[i] = resultColumnDataTypes[i].convertAndFormat(value);
        }
      }
      rowsInSelectionResults.addFirst(extractedRow);
    }

    return new ResultTable(resultDataSchema, rowsInSelectionResults);
  }
}
