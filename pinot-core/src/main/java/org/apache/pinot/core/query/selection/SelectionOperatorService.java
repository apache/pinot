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

import java.util.Collection;
import java.util.LinkedList;
import java.util.PriorityQueue;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.utils.OrderByComparatorFactory;
import org.apache.pinot.spi.trace.Tracing;
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
public class SelectionOperatorService {
  private final QueryContext _queryContext;
  private final DataSchema _dataSchema;
  private final int _offset;
  private final int _numRowsToKeep;
  private final PriorityQueue<Object[]> _rows;

  /**
   * Constructor for <code>SelectionOperatorService</code> with {@link DataSchema}. (Broker side)
   *
   * @param queryContext Selection order-by query
   * @param dataSchema data schema.
   */
  public SelectionOperatorService(QueryContext queryContext, DataSchema dataSchema) {
    _queryContext = queryContext;
    _dataSchema = dataSchema;
    // Select rows from offset to offset + limit.
    _offset = queryContext.getOffset();
    _numRowsToKeep = _offset + queryContext.getLimit();
    assert queryContext.getOrderByExpressions() != null;
    _rows = new PriorityQueue<>(Math.min(_numRowsToKeep, SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY),
        OrderByComparatorFactory.getComparator(queryContext.getOrderByExpressions(),
            _queryContext.isNullHandlingEnabled()).reversed());
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
   * TODO: Do merge sort after releasing 0.13.0 when server side results are sorted
   *       Can also consider adding a data table metadata to indicate whether the server side results are sorted
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
          Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rowId);
        }
      } else {
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
          SelectionOperatorUtils.addToPriorityQueue(row, _rows, _numRowsToKeep);
          Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(rowId);
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
    int[] columnIndices = SelectionOperatorUtils.getResultTableColumnIndices(_queryContext, _dataSchema);
    int numColumns = columnIndices.length;
    DataSchema resultDataSchema = SelectionOperatorUtils.getSchemaForProjection(_dataSchema, columnIndices);

    // Extract the result rows
    LinkedList<Object[]> rowsInSelectionResults = new LinkedList<>();
    while (_rows.size() > _offset) {
      Object[] row = _rows.poll();
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
